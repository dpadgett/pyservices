#!/usr/bin/python -u
# -*- coding: utf-8 -*-
"""
Network / BGP ASN / Reverse-DNS PTR / Parallel UDP Traceroute PoP +
C SHA-1 CIDR Inversion coordinator for player session disambiguation.
Compatible with Python 2.7 inside demos-web-pyservices-1.
"""
import datetime
import hashlib
import json
import os
import subprocess

VPN_KEYWORDS = (
  'M247', 'MULLVAD', 'CHOOPA', 'VULTR', 'DATACAMP', 'CDN77',
  'DIGITALOCEAN', 'AMAZON', 'HETZNER', 'OVH', 'LEASEWEB',
  'TEFINCOM', 'NORD', 'PRIVAX', 'TZULO', 'HOSTROYALE', 'PERFORMIVE',
  'SHARKTECH', 'QUADRANET', 'COLOCROSSING', 'CLOUDFLARE', 'LINODE'
)


def ip_to_subnet24(ip_str):
  if not ip_str:
    return None
  parts = ip_str.strip().split('.')
  if len(parts) != 4:
    return None
  return '%s.%s.%s.0/24' % (parts[0], parts[1], parts[2])


def is_vpn_asn(asn_name, acc_count_on_asn=0):
  up = (asn_name or '').upper()
  for kw in VPN_KEYWORDS:
    if kw in up:
      return True
  # If >= 5 distinct JKA accounts share one ASN, treat it as non-exclusive (large national ISP or VPN)
  return False


def run_py3_network_probe(ips, trace_subnets=None):
  if not ips and not trace_subnets:
    return {}
  payload = json.dumps({
    'ips': list(ips),
    'trace_subnets': list(trace_subnets or []),
  })
  proc = subprocess.Popen(
    ['/usr/bin/python3', '/home/pyservices/network_probe_py3.py'],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
  )
  out, err = proc.communicate(payload)
  if proc.returncode != 0:
    return {}
  try:
    return json.loads(out).get('subnets', {})
  except Exception:
    return {}


def run_single_ip_trace(ip_str):
  proc = subprocess.Popen(
    ['/usr/bin/python3', '/home/pyservices/network_probe_py3.py', '--single-trace', str(ip_str)],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
  )
  out, err = proc.communicate()
  if proc.returncode != 0:
    raise RuntimeError('Traceroute probe failed: %s' % err)
  res = json.loads(out)
  return res


def run_c_cidr_sha1_scan(target_hashes, cidrs):
  """
  Run compiled /home/pyservices/lib/sha1_cidr_scan across candidate BGP CIDRs.
  Returns {int(ip_hash): (ip_str, cidr_str)}.
  """
  bin_path = '/home/pyservices/lib/sha1_cidr_scan'
  if not os.path.isfile(bin_path) or not target_hashes or not cidrs:
    return {}

  # Only scan prefixes >= /11 (max 2M IPs per CIDR) to keep total scan < 1 second
  safe_cidrs = []
  total_ips = 0
  for c in sorted(set(cidrs)):
    if '/' not in c:
      continue
    try:
      plen = int(c.split('/')[1])
    except ValueError:
      continue
    if 12 <= plen <= 32:
      cnt = 1 << (32 - plen)
      if total_ips + cnt <= 48000000:
        safe_cidrs.append(str(c))
        total_ips += cnt

  if not safe_cidrs:
    return {}

  stdin_data = '\n'.join([str(int(h)) for h in target_hashes]) + '\n'
  proc = subprocess.Popen(
    [bin_path] + safe_cidrs,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
  )
  out, _ = proc.communicate(stdin_data)
  found = {}
  for line in out.splitlines():
    parts = line.strip().split()
    if len(parts) >= 3:
      try:
        h = int(parts[0])
        found[h] = (parts[1], parts[2])
      except ValueError:
        pass
  return found


def sync_and_load_network_maps(db, servers_extracted, all_sessions, is_usable_ip_fn, priority_trace_ip_hashes=None):
  """
  1. Upserts all cleartext IPs from enhanced.db (linked + unlinked) into db.ipHashCache.
  2. Enriches /24 subnets (BGP ASN, Country, PTR) into db.ipSubnetCache.
  3. Runs fast C SHA-1 inversion across all observed non-VPN BGP CIDRs for unresolved session IP hashes.
  4. Runs parallel UDP traceroutes on priority /24 subnets (shared-GUID / split / conflict candidates).

  Returns:
    ip_hash_to_net: dict {int(ip_hash): net_info_dict}
  """
  # 1. Load existing caches
  hash_cache = {int(d['_id']): d for d in db.ipHashCache.find()}
  subnet_cache = {d['_id']: d for d in db.ipSubnetCache.find()}

  # Harvest cleartext IPs from all 6 enhanced.db files
  new_hash_docs = []
  for srv_data in servers_extracted:
    c_map = srv_data.get('cleartext_ips') or {}
    for h_str, ip_str in c_map.iteritems():
      try:
        h = int(h_str)
      except ValueError:
        continue
      if not is_usable_ip_fn(h):
        continue
      existing = hash_cache.get(h)
      if existing is None or existing.get('source') != 'enhanced_db':
        doc = {'_id': h, 'ip': ip_str, 'source': 'enhanced_db'}
        hash_cache[h] = doc
        new_hash_docs.append(doc)

  if new_hash_docs:
    bulk = db.ipHashCache.initialize_unordered_bulk_op()
    for d in new_hash_docs:
      bulk.find({'_id': d['_id']}).upsert().replace_one(d)
    try:
      bulk.execute()
    except Exception:
      pass

  # 2. Enrich any uncached /24s for linked enhanced.db IPs + MongoDB archive session IPs
  linked_ip_hashes = set()
  for srv_data in servers_extracted:
    for s_obj in srv_data.get('sessions', []):
      if s_obj.get('sha1_ip') is not None:
        linked_ip_hashes.add(int(s_obj['sha1_ip']))
  mongo_ip_hashes = set([int(s['_id'].get('ip', 0)) for s in all_sessions if is_usable_ip_fn(int(s['_id'].get('ip', 0)))])
  relevant_hashes = linked_ip_hashes | mongo_ip_hashes

  uncached_ips = []
  seen_s24 = set()
  for h in relevant_hashes:
    d = hash_cache.get(h)
    if not d:
      continue
    ip_str = d.get('ip')
    s24 = ip_to_subnet24(ip_str)
    if s24 and s24 not in subnet_cache and s24 not in seen_s24:
      seen_s24.add(s24)
      uncached_ips.append(ip_str)

  if uncached_ips:
    # Batch in chunks of 250 /24s
    for i in range(0, len(uncached_ips), 250):
      enriched = run_py3_network_probe(uncached_ips[i:i + 250], trace_subnets=[])
      if enriched:
        bulk_s = db.ipSubnetCache.initialize_unordered_bulk_op()
        now = datetime.datetime.utcnow()
        for s24, sdoc in enriched.iteritems():
          sdoc['_id'] = s24
          sdoc['updated_at'] = now
          subnet_cache[s24] = sdoc
          bulk_s.find({'_id': s24}).upsert().replace_one(sdoc)
        try:
          bulk_s.execute()
        except Exception:
          pass

  # 3. Collect all non-VPN BGP CIDRs from linked enhanced.db sessions and invert remaining MongoDB IP hashes!

  candidate_cidrs = set()
  for h in linked_ip_hashes:
    hd = hash_cache.get(h)
    if not hd:
      continue
    s24 = ip_to_subnet24(hd.get('ip'))
    sdoc = subnet_cache.get(s24)
    if sdoc and sdoc.get('bgp_prefix') and not is_vpn_asn(sdoc.get('asn_name', '')):
      candidate_cidrs.add(sdoc['bgp_prefix'])

  unresolved_hashes = set()
  for s in all_sessions:
    iph = int(s['_id'].get('ip', 0))
    if is_usable_ip_fn(iph) and iph not in hash_cache:
      unresolved_hashes.add(iph)

  cidr_sig = hashlib.sha1(','.join(sorted(candidate_cidrs))).hexdigest()
  scan_state = db.profileLinkConfig.find_one({'_id': 'cidr_scan_state'}) or {}
  prev_unres_cnt = scan_state.get('unresolved_count', -1)

  if unresolved_hashes and candidate_cidrs and (
    scan_state.get('cidr_sig') != cidr_sig or abs(len(unresolved_hashes) - prev_unres_cnt) > 10
  ):
    inverted = run_c_cidr_sha1_scan(unresolved_hashes, candidate_cidrs)
    if inverted:
      bulk_h = db.ipHashCache.initialize_unordered_bulk_op()
      new_inv_ips = []
      for h, (ip_str, cidr_str) in inverted.iteritems():
        doc = {'_id': int(h), 'ip': ip_str, 'source': 'bgp_cidr_sha1', 'cidr': cidr_str}
        hash_cache[int(h)] = doc
        bulk_h.find({'_id': int(h)}).upsert().replace_one(doc)
        s24 = ip_to_subnet24(ip_str)
        if s24 and s24 not in subnet_cache:
          new_inv_ips.append(ip_str)
      try:
        bulk_h.execute()
      except Exception:
        pass
      if new_inv_ips:
        enriched2 = run_py3_network_probe(new_inv_ips[:300], trace_subnets=[])
        if enriched2:
          bulk_s2 = db.ipSubnetCache.initialize_unordered_bulk_op()
          now = datetime.datetime.utcnow()
          for s24, sdoc in enriched2.iteritems():
            sdoc['_id'] = s24
            sdoc['updated_at'] = now
            subnet_cache[s24] = sdoc
            bulk_s2.find({'_id': s24}).upsert().replace_one(sdoc)
          try:
            bulk_s2.execute()
          except Exception:
            pass
    db.profileLinkConfig.replace_one(
      {'_id': 'cidr_scan_state'},
      {
        '_id': 'cidr_scan_state',
        'cidr_sig': cidr_sig,
        'unresolved_count': len([h for h in unresolved_hashes if h not in hash_cache]),
        'updated_at': datetime.datetime.utcnow(),
      },
      upsert=True
    )

  # 4. Run parallel UDP traceroutes on priority subnets (shared-GUID / dynamic-IP collision candidates)
  if priority_trace_ip_hashes:
    need_trace_ips = []
    need_trace_s24 = set()
    for h in priority_trace_ip_hashes:
      hd = hash_cache.get(int(h))
      if not hd:
        continue
      ip_str = hd.get('ip')
      s24 = ip_to_subnet24(ip_str)
      sdoc = subnet_cache.get(s24)
      if s24 and (sdoc is None or not sdoc.get('traced')) and s24 not in need_trace_s24:
        need_trace_s24.add(s24)
        need_trace_ips.append(ip_str)
        if len(need_trace_s24) >= 90:
          break
    if need_trace_ips:
      tr_enriched = run_py3_network_probe(need_trace_ips, trace_subnets=need_trace_s24)
      if tr_enriched:
        bulk_tr = db.ipSubnetCache.initialize_unordered_bulk_op()
        now = datetime.datetime.utcnow()
        for s24, sdoc in tr_enriched.iteritems():
          sdoc['_id'] = s24
          sdoc['updated_at'] = now
          subnet_cache[s24] = sdoc
          bulk_tr.find({'_id': s24}).upsert().replace_one(sdoc)
        try:
          bulk_tr.execute()
        except Exception:
          pass

  # Build combined ip_hash -> net_info dictionary
  ip_hash_to_net = {}
  for h, hd in hash_cache.iteritems():
    ip_str = hd.get('ip')
    s24 = ip_to_subnet24(ip_str)
    sdoc = subnet_cache.get(s24) or {}
    asn_name = sdoc.get('asn_name') or ''
    ip_hash_to_net[int(h)] = {
      'ip': ip_str,
      'subnet24': s24,
      'ip_source': hd.get('source', 'enhanced_db'),
      'asn': sdoc.get('asn'),
      'asn_name': asn_name,
      'bgp_prefix': sdoc.get('bgp_prefix') or '',
      'country': sdoc.get('country') or '',
      'alloc_date': sdoc.get('alloc_date') or '',
      'ptr': sdoc.get('ptr') or '',
      'ptr_domain': sdoc.get('ptr_domain') or '',
      'pop_sig': sdoc.get('pop_sig') or '',
      'far_hops': sdoc.get('far_hops') or [],
      'is_vpn': is_vpn_asn(asn_name),
    }
  return ip_hash_to_net


def lookup_cached_net_for_sessions(db, session_docs):
  """Fast read-only lookup from db.ipHashCache + db.ipSubnetCache for RPC hydration."""
  ip_hashes = list({int(s['_id'].get('ip', 0)) for s in session_docs if int(s['_id'].get('ip', 0)) != 0})
  if not ip_hashes:
    return {}
  hdocs = {int(d['_id']): d for d in db.ipHashCache.find({'_id': {'$in': ip_hashes}})}
  s24s = list({ip_to_subnet24(d.get('ip')) for d in hdocs.values() if d.get('ip')})
  sdocs = {d['_id']: d for d in db.ipSubnetCache.find({'_id': {'$in': s24s}})} if s24s else {}
  out = {}
  for h, hd in hdocs.iteritems():
    ip_str = hd.get('ip')
    s24 = ip_to_subnet24(ip_str)
    sd = sdocs.get(s24) or {}
    asn_name = sd.get('asn_name') or ''
    out[h] = {
      'ip': ip_str,
      'subnet24': s24,
      'ip_source': hd.get('source', 'enhanced_db'),
      'asn': sd.get('asn'),
      'asn_name': asn_name,
      'bgp_prefix': sd.get('bgp_prefix') or '',
      'country': sd.get('country') or '',
      'ptr': sd.get('ptr') or '',
      'ptr_domain': sd.get('ptr_domain') or '',
      'pop_sig': sd.get('pop_sig') or '',
      'is_vpn': is_vpn_asn(asn_name),
    }
  return out
