#!/usr/bin/python -u
# -*- coding: utf-8 -*-
"""
Historical IP backfill for db.sessionGames, db.sessions, db.ipHashCache, and db.ipSubnetCache.
Recovers cleartext IPs for matches affected by the base_enhanced g_client.c:2559 bug
(uint32_t ip = getIpFromString(...) overwriting ip with qtrue=1 before sha3_Update)
using server_internal.log extracts + enhanced.db tables, and re-keys constant-IP sessions
to their true peppered SHA3-512 IP hashes while preserving every session's playerid.

Usage:
  python backfill_missing_ips.py             # dry-run (no MongoDB writes)
  python backfill_missing_ips.py --apply     # execute backfill under mutation_lock()
"""
import calendar
from collections import Counter, defaultdict
import datetime
import json
import os
import subprocess
import sys
import tempfile
import time

import bson
import pymongo
from bson.objectid import ObjectId
from pymongo import MongoClient

import network_disambig_lib
import player_lib
import session_alias_lib

PY3_BIN = '/usr/bin/python3'
RESOLVER_PY3 = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resolve_missing_ips_py3.py')


def _make_session_son(ip_val, guid_val, newmod_id=None):
  items = [('ip', int(ip_val)), ('guid', int(guid_val))]
  if newmod_id is not None:
    items.append(('newmod_id', newmod_id))
  return bson.SON(items)


def _to_session_son(sid_dict):
  return _make_session_son(
    sid_dict['ip'],
    sid_dict.get('guid', 0),
    sid_dict.get('newmod_id')
  )


def _run_py3_resolver(req_obj):
  fd_in, in_path = tempfile.mkstemp(prefix='ip_backfill_in_', suffix='.json')
  fd_out, out_path = tempfile.mkstemp(prefix='ip_backfill_out_', suffix='.json')
  os.close(fd_in)
  os.close(fd_out)
  try:
    with open(in_path, 'w') as f:
      json.dump(req_obj, f)
    proc = subprocess.Popen(
      [PY3_BIN, RESOLVER_PY3, in_path, out_path],
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE
    )
    out, err = proc.communicate()
    if proc.returncode != 0:
      raise RuntimeError('resolve_missing_ips_py3.py failed (%d): %s\n%s' % (proc.returncode, out, err))
    with open(out_path, 'r') as f:
      return json.load(f)
  finally:
    for p in (in_path, out_path):
      try:
        os.unlink(p)
      except OSError:
        pass


def _extract_sg_summary_for_resolver(idx, sg, match_doc):
  sid = sg['_id']['session']
  ts = 0
  if sg.get('time'):
    ts = int(calendar.timegm(sg['time'].timetuple()))
  elif match_doc and match_doc.get('t'):
    ts = int(calendar.timegm(match_doc['t'].timetuple()))

  mapname = ''
  if match_doc and match_doc.get('n'):
    mapname = match_doc['n']

  cids = []
  names = []
  team_ms = Counter()
  for g in sg.get('games', []):
    cid = g.get('clientid')
    if cid is not None and int(cid) not in cids:
      cids.append(int(cid))
    for nm in g.get('names', []):
      nval = nm.get('name')
      if nval and nval not in names:
        names.append(nval)
    for tm in g.get('teams', []):
      tname = tm.get('team')
      dur = max(0, int(tm.get('end', 0)) - int(tm.get('start', 0)))
      if tname in ('RED', 'BLUE') and dur > 0:
        team_ms[tname] += dur

  primary_team = None
  primary_team_ms = 0
  if team_ms:
    primary_team, primary_team_ms = team_ms.most_common(1)[0]

  return {
    'idx': idx,
    'match': sg['_id']['match'],
    'const_ip': int(sid['ip']),
    'guid': int(sid.get('guid', 0)),
    'newmod_id': sid.get('newmod_id'),
    'ts': ts,
    'mapname': mapname,
    'is_match': bool(sg.get('is_match')),
    'clientids': cids,
    'names': names,
    'primary_team': primary_team,
    'team_ms': int(primary_team_ms),
  }


def _merge_games_into_doc(existing_doc, added_games, match_is_rated):
  games = list(existing_doc.get('games', [])) + list(added_games)
  games.sort(key=lambda g: (int(g.get('start', 0)), int(g.get('clientid', 0))))
  total_time = 0
  inc_match = 1 if existing_doc.get('is_match') else 0
  for g in games:
    for tm in g.get('teams', []):
      total_time += max(0, int(tm.get('end', 0)) - int(tm.get('start', 0)))
      if match_is_rated and tm.get('team') in ('RED', 'BLUE'):
        inc_match = 1
  existing_doc['games'] = games
  existing_doc['dur'] = total_time
  existing_doc['is_match'] = (inc_match == 1)
  return existing_doc


def _summarize_session_from_games(sid, sgames, playerid):
  if not sgames:
    return None
  sgames_sorted = sorted(
    sgames,
    key=lambda sg: sg.get('time') or datetime.datetime(1970, 1, 1)
  )
  first_game = sgames_sorted[0].get('time')
  last_game = sgames_sorted[-1].get('time')
  num_games = len(sgames_sorted)
  num_matches = sum([1 for sg in sgames_sorted if sg.get('is_match')])
  total_time = sum([int(sg.get('dur', 0)) for sg in sgames_sorted])
  last_name = u'^7Padawan'
  for sg in reversed(sgames_sorted):
    glist = sg.get('games') or []
    if glist:
      nlist = glist[-1].get('names') or []
      if nlist and nlist[-1].get('name'):
        last_name = nlist[-1]['name']
        break
  doc = {
    '_id': sid,
    'first_game': first_game,
    'last_game': last_game,
    'num_games': num_games,
    'num_matches': num_matches,
    'time': total_time,
    'last_name': last_name,
  }
  if playerid is not None:
    doc['playerid'] = playerid
  return doc


def run_backfill(apply_changes=False):
  t0 = time.time()
  db = MongoClient('mongodb').demos

  # 1. Discover target server pepper constants
  init_info = _run_py3_resolver({'items': []})
  target_constants = {int(k): v for k, v in init_info['target_constants'].iteritems()}
  print 'Target server pepper constants:', target_constants

  # 2. Fetch all db.sessions whose _id.ip is in target_constants
  old_sessions = list(db.sessions.find({'_id.ip': {'$in': list(target_constants.keys())}}))
  print 'Constant-IP db.sessions found: %d' % len(old_sessions)

  old_sid_by_key = {}
  old_playerid_by_key = {}
  for s in old_sessions:
    sk = player_lib.canonical_session_key(s['_id'])
    old_sid_by_key[sk] = _to_session_son(s['_id'])
    old_playerid_by_key[sk] = s.get('playerid')

  # 3. Fetch all db.sessionGames matching _id.session.ip directly (BSON-order agnostic)
  all_sg = list(db.sessionGames.find({'_id.session.ip': {'$in': list(target_constants.keys())}}))
  for sg in all_sg:
    sk = player_lib.canonical_session_key(sg['_id']['session'])
    if sk not in old_sid_by_key:
      old_sid_by_key[sk] = _to_session_son(sg['_id']['session'])
  print 'Constant-IP db.sessionGames loaded: %d (%.2fs)' % (len(all_sg), time.time() - t0)

  # 4. Fetch minmatches metadata (mapname 'n', time 't', is_match 'ma')
  match_ids = sorted(set([sg['_id']['match'] for sg in all_sg]))
  matches_by_id = {}
  for i in range(0, len(match_ids), 1000):
    chunk = match_ids[i:i + 1000]
    for m in db.minmatches.find({'_id': {'$in': chunk}}, {'_id': 1, 'n': 1, 't': 1, 'ma': 1}):
      matches_by_id[m['_id']] = m
  print 'Loaded %d distinct minmatches (%.2fs)' % (len(matches_by_id), time.time() - t0)

  # 4b. Load already-resolved real-IP teammates in rated matches + historical (guid, ts, ip) observations
  rated_mids = sorted(set([sg['_id']['match'] for sg in all_sg if sg.get('is_match')]))
  ip_cache_map = {int(d['_id']): d['ip'] for d in db.ipHashCache.find({}, {'_id': 1, 'ip': 1}) if d.get('ip')}
  match_known_players = defaultdict(list)
  for i in range(0, len(rated_mids), 500):
    chunk = rated_mids[i:i + 500]
    for ksg in db.sessionGames.find({'_id.match': {'$in': chunk}, 'is_match': True}):
      iph = int(ksg['_id']['session'].get('ip', 0))
      if iph in target_constants or iph not in ip_cache_map:
        continue
      kip = ip_cache_map[iph]
      tms = Counter()
      for g in ksg.get('games', []):
        for tm in g.get('teams', []):
          if tm.get('team') in ('RED', 'BLUE'):
            tms[tm['team']] += max(0, int(tm.get('end', 0)) - int(tm.get('start', 0)))
      pteam = tms.most_common(1)[0][0] if tms else None
      match_known_players[ksg['_id']['match']].append({
        'ip': kip,
        'cuid': ksg['_id']['session'].get('newmod_id'),
        'guid': int(ksg['_id']['session'].get('guid', 0)),
        'team': pteam,
      })

  # Also seed (guid, ts, ip) timeline from already-resolved real-IP sessions of the same guids
  target_guids = sorted(set([
    int(sg['_id']['session'].get('guid', 0))
    for sg in all_sg
    if int(sg['_id']['session'].get('guid', 0)) not in (0, 1150862)
  ]))
  extra_guid_obs = []
  if target_guids:
    real_sids_for_guids = [
      _to_session_son(s['_id'])
      for s in db.sessions.find({'_id.guid': {'$in': target_guids}}, {'_id': 1})
      if int(s['_id'].get('ip', 0)) in ip_cache_map and int(s['_id'].get('ip', 0)) not in target_constants
    ]
    for i in range(0, len(real_sids_for_guids), 300):
      chunk = real_sids_for_guids[i:i + 300]
      for ksg in db.sessionGames.find({'_id.session': {'$in': chunk}}, {'_id': 1, 'time': 1}):
        if not ksg.get('time'):
          continue
        iph = int(ksg['_id']['session']['ip'])
        kip = ip_cache_map.get(iph)
        if kip:
          extra_guid_obs.append({
            'guid': int(ksg['_id']['session']['guid']),
            'ts': int(calendar.timegm(ksg['time'].timetuple())),
            'ip': kip,
          })
  print 'Loaded %d rated-match teammate contexts & %d real-IP guid observations (%.2fs)' % (
    len(match_known_players), len(extra_guid_obs), time.time() - t0
  )

  # 5. Invoke Python 3 resolver
  items = [
    _extract_sg_summary_for_resolver(idx, sg, matches_by_id.get(sg['_id']['match']))
    for idx, sg in enumerate(all_sg)
  ]
  res = _run_py3_resolver({
    'items': items,
    'match_known_players': dict(match_known_players),
    'extra_guid_obs': extra_guid_obs,
  })
  resolved_map = res['resolved']
  ip_hash_entries = res['ip_hash_entries']
  print 'Python 3 resolver finished (%.2fs): all_ips_count=%d, ip_hash_entries=%d' % (
    time.time() - t0, res.get('all_ips_count', 0), len(ip_hash_entries)
  )

  # 6. Compute detailed statistics
  stats_total = defaultdict(lambda: Counter())
  stats_resolved = defaultdict(lambda: Counter())
  tier_counts_match = Counter()
  tier_counts_pub = Counter()
  resolved_unique_ips = defaultdict(set)

  new_sid_by_key = {}
  new_skey_contributors = defaultdict(Counter)  # new_sk -> Counter(old_playerid)
  old_skey_to_new_skeys = defaultdict(set)
  old_skey_unresolved_count = Counter()

  for idx, sg in enumerate(all_sg):
    cip = int(sg['_id']['session']['ip'])
    grp = target_constants.get(cip, 'unknown')
    is_m = bool(sg.get('is_match'))
    kind = 'match' if is_m else 'pub'
    stats_total[grp][kind] += 1
    old_sk = player_lib.canonical_session_key(sg['_id']['session'])
    old_pid = old_playerid_by_key.get(old_sk)

    rinfo = resolved_map.get(str(idx))
    if not rinfo:
      old_skey_unresolved_count[old_sk] += 1
      continue

    stats_resolved[grp][kind] += 1
    if is_m:
      tier_counts_match[rinfo['tier']] += 1
    else:
      tier_counts_pub[rinfo['tier']] += 1
    resolved_unique_ips[grp].add(rinfo['ip'])

    new_sid = _make_session_son(
      rinfo['pepper_hash'],
      sg['_id']['session'].get('guid', 0),
      sg['_id']['session'].get('newmod_id')
    )
    new_sk = player_lib.canonical_session_key(new_sid)
    new_sid_by_key[new_sk] = new_sid
    old_skey_to_new_skeys[old_sk].add(new_sk)
    if old_pid is not None:
      new_skey_contributors[new_sk][old_pid] += 1

  print '\n=== COVERAGE BY SERVER GROUP ==='
  for grp in ('pug', 'bomb', 'ent'):
    tm = stats_total[grp]['match']
    rm = stats_resolved[grp]['match']
    tp = stats_total[grp]['pub']
    rp = stats_resolved[grp]['pub']
    pct_m = (100.0 * rm / tm) if tm else 100.0
    pct_p = (100.0 * rp / tp) if tp else 100.0
    print '  %-6s | Rated Matches: %6d / %6d (%6.2f%%) | Pub/Spec: %6d / %6d (%6.2f%%) | Unique Cleartext IPs: %d' % (
      grp, rm, tm, pct_m, rp, tp, pct_p, len(resolved_unique_ips[grp])
    )

  print '\n=== RESOLUTION TIERS (RATED MATCHES) ==='
  for tier, cnt in tier_counts_match.most_common():
    print '  %-30s : %6d' % (tier, cnt)

  print '\n=== RESOLUTION TIERS (PUB / SPEC) ==='
  for tier, cnt in tier_counts_pub.most_common():
    print '  %-30s : %6d' % (tier, cnt)

  fully_resolved_old = sum([1 for sk in old_sid_by_key if old_skey_unresolved_count[sk] == 0])
  partially_resolved_old = sum([
    1 for sk in old_sid_by_key
    if old_skey_unresolved_count[sk] > 0 and len(old_skey_to_new_skeys[sk]) > 0
  ])
  untouched_old = sum([
    1 for sk in old_sid_by_key
    if len(old_skey_to_new_skeys[sk]) == 0
  ])

  # Check if any new_sid already exists in db.sessions
  existing_new_sessions = {}
  new_sid_list = list(new_sid_by_key.values())
  for i in range(0, len(new_sid_list), 300):
    chunk = new_sid_list[i:i + 300]
    for s in db.sessions.find({'_id': {'$in': chunk}}):
      existing_new_sessions[player_lib.canonical_session_key(s['_id'])] = s

  multi_pid_conflicts = [
    (nsk, ctr) for nsk, ctr in new_skey_contributors.iteritems() if len(ctr) > 1
  ]
  existing_pid_conflicts = []
  for nsk, es in existing_new_sessions.iteritems():
    epid = es.get('playerid')
    if epid is not None and nsk in new_skey_contributors:
      for opid in new_skey_contributors[nsk]:
        if opid != epid:
          existing_pid_conflicts.append((nsk, epid, opid))

  print '\n=== SESSION RE-KEY SUMMARY ==='
  print '  Old constant-IP sessions total     : %d' % len(old_sid_by_key)
  print '    Fully resolved (will be removed) : %d' % fully_resolved_old
  print '    Partially resolved (pub remains) : %d' % partially_resolved_old
  print '    Unresolved (pub/spec only)       : %d' % untouched_old
  print '  Distinct real-IP target sessions   : %d (already in db.sessions: %d)' % (
    len(new_sid_by_key), len(existing_new_sessions)
  )
  print '  Contributor playerid conflicts     : %d (existing-session conflicts: %d)' % (
    len(multi_pid_conflicts), len(existing_pid_conflicts)
  )
  for nsk, ctr in multi_pid_conflicts[:10]:
    print '    Multi-PID on %s -> %s' % (nsk, dict(ctr))

  if not apply_changes:
    print '\n[DRY-RUN COMPLETE] Pass --apply to execute MongoDB backfill.'
    return

  print '\n=== APPLYING BACKFILL UNDER MUTATION LOCK ==='
  with player_lib.mutation_lock():
    # A. Upsert ipHashCache
    existing_hashes = set(db.ipHashCache.distinct('_id'))
    hash_bulk = db.ipHashCache.initialize_unordered_bulk_op()
    hash_ops = 0
    for h_str, ip_str in ip_hash_entries.iteritems():
      h_int = int(h_str)
      if h_int not in existing_hashes:
        hash_bulk.find({'_id': h_int}).upsert().update_one({
          '$set': {'ip': ip_str, 'source': 'ip_backfill_20261117'}
        })
        hash_ops += 1
        if hash_ops % 2000 == 0:
          hash_bulk.execute()
          hash_bulk = db.ipHashCache.initialize_unordered_bulk_op()
    if hash_ops % 2000 != 0:
      hash_bulk.execute()
    print '  [A] Inserted %d new cleartext IP hash mappings into db.ipHashCache' % hash_ops

    # B. Probe uncached /24 subnets for resolved active session IPs
    active_ips = set()
    for grp_ips in resolved_unique_ips.values():
      active_ips.update(grp_ips)
    existing_s24 = set(db.ipSubnetCache.distinct('_id'))
    probe_ips = []
    seen_s24 = set()
    for ip_str in sorted(active_ips):
      s24 = network_disambig_lib.ip_to_subnet24(ip_str)
      if s24 and s24 not in existing_s24 and s24 not in seen_s24:
        seen_s24.add(s24)
        probe_ips.append(ip_str)
    if probe_ips:
      print '  [B] Probing BGP/ASN/PTR for %d newly discovered /24 subnets...' % len(probe_ips)
      now_dt = datetime.datetime.utcnow()
      for i in range(0, len(probe_ips), 250):
        chunk_ips = probe_ips[i:i + 250]
        sub_res = network_disambig_lib.run_py3_network_probe(chunk_ips)
        if sub_res:
          sbulk = db.ipSubnetCache.initialize_unordered_bulk_op()
          sops = 0
          for s24, sinfo in sub_res.iteritems():
            doc = dict(sinfo)
            doc['_id'] = s24
            doc['subnet24'] = s24
            doc['updated_at'] = now_dt
            sbulk.find({'_id': s24}).upsert().replace_one(doc)
            sops += 1
          if sops:
            sbulk.execute()
      print '  [B] Subnet cache updated.'

    # C. Re-key db.sessionGames (write new docs first, then remove old docs)
    # Group resolved sg docs by (new_sk, match_id)
    grouped_targets = {}
    old_ids_to_delete = []
    touched_old_skeys = set()
    touched_new_skeys = set()

    for idx, sg in enumerate(all_sg):
      rinfo = resolved_map.get(str(idx))
      if not rinfo:
        continue
      old_sk = player_lib.canonical_session_key(sg['_id']['session'])
      touched_old_skeys.add(old_sk)
      old_ids_to_delete.append(sg['_id'])

      pep_hash = int(rinfo['pepper_hash'])
      new_sid = _make_session_son(
        pep_hash,
        sg['_id']['session'].get('guid', 0),
        sg['_id']['session'].get('newmod_id')
      )
      new_sk = player_lib.canonical_session_key(new_sid)
      touched_new_skeys.add(new_sk)

      match_id = sg['_id']['match']
      match_doc = matches_by_id.get(match_id)
      match_is_rated = bool(match_doc and match_doc.get('ma'))

      updated_games = []
      for g in sg.get('games', []):
        ng = dict(g)
        ng['ip'] = pep_hash
        updated_games.append(ng)

      tkey = (new_sk, match_id)
      if tkey not in grouped_targets:
        new_doc = dict(sg)
        new_doc['_id'] = bson.SON([('session', new_sid), ('match', match_id)])
        new_doc['games'] = updated_games
        grouped_targets[tkey] = (new_doc, match_is_rated)
      else:
        prev_doc, prev_rated = grouped_targets[tkey]
        _merge_games_into_doc(prev_doc, updated_games, prev_rated or match_is_rated)

    # Check if any target _id already exists in db.sessionGames
    target_doc_ids = [doc['_id'] for (doc, _mr) in grouped_targets.values()]
    for i in range(0, len(target_doc_ids), 300):
      chunk_ids = target_doc_ids[i:i + 300]
      for ex_sg in db.sessionGames.find({'_id': {'$in': chunk_ids}}):
        esk = player_lib.canonical_session_key(ex_sg['_id']['session'])
        tkey = (esk, ex_sg['_id']['match'])
        if tkey in grouped_targets:
          new_doc, mr = grouped_targets[tkey]
          merged = _merge_games_into_doc(ex_sg, new_doc['games'], mr)
          grouped_targets[tkey] = (merged, mr)

    # Upsert all new sessionGames in chunks of 1000
    target_items = list(grouped_targets.values())
    for i in range(0, len(target_items), 1000):
      chunk = target_items[i:i + 1000]
      sg_bulk = db.sessionGames.initialize_unordered_bulk_op()
      for doc, _mr in chunk:
        doc_body = {k: v for k, v in doc.iteritems() if k != '_id'}
        sg_bulk.find({'_id': doc['_id']}).upsert().replace_one(doc_body)
      sg_bulk.execute()
    print '  [C1] Upserted %d re-keyed db.sessionGames documents.' % len(target_items)

    # Delete old constant-IP sessionGames in chunks of 1000
    for i in range(0, len(old_ids_to_delete), 1000):
      chunk = old_ids_to_delete[i:i + 1000]
      del_bulk = db.sessionGames.initialize_unordered_bulk_op()
      for oid in chunk:
        del_bulk.find({'_id': oid}).remove_one()
      del_bulk.execute()
    print '  [C2] Removed %d old constant-IP db.sessionGames documents.' % len(old_ids_to_delete)

    # D. Recompute db.sessions for all touched old & new session keys
    all_touched_skeys = sorted(touched_old_skeys | touched_new_skeys)
    touched_playerids = set()
    surviving_sessions_for_aliases = []
    deleted_old_skeys = set()

    sess_bulk = db.sessions.initialize_unordered_bulk_op()
    sess_ops = 0
    alias_del_keys = []

    for i in range(0, len(all_touched_skeys), 250):
      chunk_skeys = all_touched_skeys[i:i + 250]
      chunk_sids = [
        new_sid_by_key[sk] if sk in new_sid_by_key else old_sid_by_key[sk]
        for sk in chunk_skeys
      ]
      sg_by_sk = defaultdict(list)
      for sg in db.sessionGames.find({'_id.session': {'$in': chunk_sids}}):
        sg_by_sk[player_lib.canonical_session_key(sg['_id']['session'])].append(sg)

      for sk, sid in zip(chunk_skeys, chunk_sids):
        sg_list = sg_by_sk.get(sk, [])
        if not sg_list:
          sess_bulk.find({'_id': sid}).remove_one()
          sess_ops += 1
          deleted_old_skeys.add(sk)
          alias_del_keys.append(sk)
          old_pid = old_playerid_by_key.get(sk)
          if old_pid is not None:
            touched_playerids.add(old_pid)
          continue

        # Determine preserved playerid
        pid = None
        if sk in existing_new_sessions and existing_new_sessions[sk].get('playerid') is not None:
          pid = existing_new_sessions[sk]['playerid']
        elif sk in old_playerid_by_key and old_playerid_by_key[sk] is not None:
          pid = old_playerid_by_key[sk]
        elif sk in new_skey_contributors and len(new_skey_contributors[sk]) > 0:
          pid = new_skey_contributors[sk].most_common(1)[0][0]

        if pid is not None:
          touched_playerids.add(pid)
        for cpid in new_skey_contributors.get(sk, {}):
          touched_playerids.add(cpid)

        sdoc = _summarize_session_from_games(sid, sg_list, pid)
        sdoc_body = {k: v for k, v in sdoc.iteritems() if k != '_id'}
        sess_bulk.find({'_id': sid}).upsert().replace_one(sdoc_body)
        sess_ops += 1
        surviving_sessions_for_aliases.append(sdoc)

        if sess_ops >= 500:
          sess_bulk.execute()
          sess_bulk = db.sessions.initialize_unordered_bulk_op()
          sess_ops = 0

    if sess_ops > 0:
      sess_bulk.execute()

    if alias_del_keys:
      db.sessionAliasSummary.delete_many({'_id': {'$in': alias_del_keys}})

    print '  [D] Rebuilt %d surviving/new db.sessions and removed %d empty constant-IP sessions.' % (
      len(surviving_sessions_for_aliases), len(deleted_old_skeys)
    )

    # E. Migrate any pinned_sessions keys in db.profileLinkConfig
    cfg = player_lib._ensure_config_doc(db)
    pinned = dict(cfg.get('pinned_sessions') or {})
    pins_changed = False
    for old_sk, new_sks in old_skey_to_new_skeys.iteritems():
      if old_sk in pinned:
        pin_entry = pinned[old_sk]
        for nsk in new_sks:
          if nsk not in pinned:
            new_entry = dict(pin_entry)
            new_entry['migrated_from_constant_ip'] = True
            pinned[nsk] = new_entry
            pins_changed = True
        if old_sk in deleted_old_skeys:
          pinned.pop(old_sk, None)
          pins_changed = True
    if pins_changed:
      db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'pinned_sessions': pinned}})
      print '  [E] Updated migrated session keys in db.profileLinkConfig.pinned_sessions.'

    # F. Recompute sessionAliasSummary for all surviving/new sessions
    session_alias_lib._compute_for_sessions(db, surviving_sessions_for_aliases)
    print '  [F] Recomputed db.sessionAliasSummary for %d sessions.' % len(surviving_sessions_for_aliases)

    # G. Recompute db.sessionPlayers summaries for all touched playerids
    for pid in touched_playerids:
      player_lib.recompute_player_summary(db, pid)
    print '  [G] Recomputed db.sessionPlayers summaries for %d profiles.' % len(touched_playerids)

  print '\n[BACKFILL COMPLETE in %.2fs]' % (time.time() - t0)


if __name__ == '__main__':
  apply_flag = ('--apply' in sys.argv[1:])
  run_backfill(apply_changes=apply_flag)
