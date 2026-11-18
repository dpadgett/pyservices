#!/usr/bin/python -u
# -*- coding: utf-8 -*-
"""
Core identity resolution, auto-merge/un-merge engine, and proposal manager
for demos.jactf.com using synced enhanced.db files across the server fleet.
Exposes full per-session attribution criteria, server enhanced.db provenance,
and competitive match aliases. Compatible with Python 2.7.
"""
import datetime
import hashlib
import json
import os
import re
import subprocess
import time
import traceback
import unicodedata

import bson.json_util
import pymongo
from bson.objectid import ObjectId
from pymongo import MongoClient

import network_disambig_lib
import player_lib
import session_alias_lib
import user_lib

GENERIC_NAMES = session_alias_lib.UNHELPFUL_EXACT


def strip_colors(s):
  return session_alias_lib.strip_colors(s)


def clean_alias_key(s):
  c = strip_colors(s).lower().strip()
  if len(c) < 3 or not session_alias_lib.is_helpful_alias(s):
    return u''
  return c


def run_local_extractor(status_only=False):
  cmd = ['/usr/bin/python3', '/home/pyservices/extract_enhanced_local.py']
  if status_only:
    cmd.append('--status-only')
  env = dict(os.environ)
  if os.path.exists('/home/pyservices/lib/libsqlite3.so.0'):
    cur_ld = env.get('LD_LIBRARY_PATH', '')
    env['LD_LIBRARY_PATH'] = ('/home/pyservices/lib:' + cur_ld) if cur_ld else '/home/pyservices/lib'
    env['_ENHANCED_SQLITE_REEXEC'] = '1'
  proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
  out, err = proc.communicate()
  if proc.returncode != 0:
    raise RuntimeError('extract_enhanced_local.py failed (%d): %s' % (proc.returncode, err))
  return json.loads(out)


def canonicalize_account(raw_name, aliases, ignored):
  if not raw_name:
    return None
  cur = raw_name.strip().lower()
  seen = set()
  while cur in aliases and aliases[cur] != cur and cur not in seen:
    seen.add(cur)
    cur = aliases[cur].strip().lower()
  if not cur or cur in ignored:
    return None
  return cur


def _pair_key(a, b):
  sa, sb = str(a), str(b)
  return (sa + '|' + sb) if sa <= sb else (sb + '|' + sa)


def ensure_indexes(db):
  try:
    db.sessionPlayers.create_index([('enhanced_account', 1)], sparse=True)
    db.profileLinkProposals.create_index([('status', 1), ('tier_auto', 1), ('created_at', -1)])
    db.profileLinkAudit.create_index([('timestamp', -1)])
    db.profileSharedIdentifiers.create_index([('type', 1)])
  except Exception:
    pass


def _top_dict_keys(d, limit=3):
  return [k for k, _ in sorted(d.items(), key=lambda x: x[1], reverse=True)[:limit]]


def _format_session_row(s, alias_map, attr_info=None, indirect_reason=None, vote_breakdown=None, ip_net_map=None):
  skey = player_lib.canonical_session_key(s['_id'])
  adoc = alias_map.get(skey) or {}
  last_name = s.get('last_name') or u'^7Unnamed'
  primary_name = adoc.get('primary_name') or last_name
  top_names = adoc.get('top_names') or []
  iph = int(s['_id'].get('ip', 0))
  net_info = ip_net_map.get(iph) if ip_net_map else None

  row = {
    'session_id': s['_id'],
    'skey': skey,
    'primary_name': primary_name,
    'last_name': last_name,
    'top_names': top_names[:4],
    'num_matches': int(s.get('num_matches', 0)),
    'num_games': int(s.get('num_games', 0)),
    'time_ms': int(s.get('time', 0)),
    'first_game': s.get('first_game'),
    'last_game': s.get('last_game'),
    'attributed': bool(attr_info is not None),
    'account': attr_info['account'] if attr_info else None,
    'primary_method': attr_info['primary_method'] if attr_info else ('ambiguous' if vote_breakdown else 'indirect_profile'),
    'servers': attr_info['servers'] if attr_info else [],
    'raw_accounts': attr_info['raw_accounts'] if attr_info else [],
    'server_accounts': attr_info.get('server_accounts', []) if attr_info else [],
    'signals': attr_info['signals'] if attr_info else [],
    'indirect_reason': indirect_reason,
    'net': net_info,
  }
  if vote_breakdown:
    row['vote_breakdown'] = vote_breakdown
  return row


def build_identity_plan(db, extracted=None):
  ensure_indexes(db)
  cfg = player_lib._ensure_config_doc(db)
  ignored = set([x.strip().lower() for x in cfg.get('ignored_accounts', []) if x])
  aliases = {}
  for k, v in (cfg.get('account_aliases') or {}).iteritems():
    if k and v:
      aliases[k.strip().lower()] = v.strip().lower()

  dnm_pairs = set()
  for pair in cfg.get('do_not_merge_pairs', []):
    if isinstance(pair, list) and len(pair) == 2:
      dnm_pairs.add(_pair_key(pair[0], pair[1]))

  pinned_sessions = cfg.get('pinned_sessions') or {}

  if extracted is None:
    extracted = run_local_extractor(status_only=False)

  sync_statuses = extracted.get('sync_statuses', [])
  servers = extracted.get('servers', [])

  # 1. Aggregate enhanced.db signals across all servers WITH full server provenance
  cuid_prov = {}           # cuid -> {cacc: {'servers': set, 'raw_accounts': set, 'nicks': {raw: dur}}}
  cuid_ip_prov = {}        # (cuid, iph) -> {cacc: {'servers': set, 'raw_accounts': set, 'ip_kinds': set, 'nicks': {raw: dur}}}
  ip_prov = {}             # iph -> {cacc: {'servers': set, 'raw_accounts': set, 'ip_kinds': set, 'nicks': {raw: dur}}}
  autolink_prov = {}       # gval -> {cacc: {'servers': set, 'raw_accounts': set, 'props': set}}
  acc_nick_prov = {}       # cacc -> {clean_key: {'servers': set, 'sample': raw_str}}
  buggy_const_ips = set()
  pug_by_match = {}        # mhash -> {(cacc, team): {'account', 'raw_accounts': set, 'srv_accs': set, 'team', 'duration', 'name', 'map', 'servers': set}}
  direct_by_match = {}     # mhash -> {(cacc, cid, kind, clean_pname): {'account', 'raw_accounts': set, 'srv_accs': set, 'client_id', 'kind', 'player_name', 'servers': set}}
  all_known_accounts = set()
  srv_acc_ids = {}         # (srv_name, raw_acc) -> account_id

  def _fmt_srv_accs(pairs_set):
    return [
      {'server': srv, 'account': racc, 'account_id': srv_acc_ids.get((srv, racc))}
      for (srv, racc) in sorted(list(pairs_set))
    ]

  for srv_data in servers:
    srv_name = srv_data.get('server', '?')
    for cip in srv_data.get('buggy_pepper_constants', []):
      buggy_const_ips.add(int(cip))

    for acc_obj in srv_data.get('accounts', []):
      raw_acc = (acc_obj.get('name') or '').strip().lower()
      if raw_acc:
        srv_acc_ids[(srv_name, raw_acc)] = acc_obj.get('account_id')
      cacc = canonicalize_account(raw_acc, aliases, ignored)
      if not cacc:
        continue
      all_known_accounts.add(cacc)
      np = acc_nick_prov.setdefault(cacc, {}).setdefault(cacc, {'servers': set(), 'sample': cacc})
      np['servers'].add(srv_name)
      if raw_acc != cacc and len(raw_acc) >= 3:
        np2 = acc_nick_prov.setdefault(cacc, {}).setdefault(raw_acc, {'servers': set(), 'sample': raw_acc})
        np2['servers'].add(srv_name)

      details = acc_obj.get('autolink_details')
      if details:
        for det in details:
          gval = int(det.get('guid', 0))
          if gval != 0:
            ap = autolink_prov.setdefault(gval, {}).setdefault(cacc, {'servers': set(), 'raw_accounts': set(), 'srv_accs': set(), 'props': set()})
            ap['servers'].add(srv_name)
            ap['raw_accounts'].add(raw_acc)
            ap['srv_accs'].add((srv_name, raw_acc))
            ap['props'].add(det.get('prop', 'autolink_guid'))
      else:
        for aguid in acc_obj.get('autolink_guids', []):
          gval = int(aguid)
          if gval != 0:
            ap = autolink_prov.setdefault(gval, {}).setdefault(cacc, {'servers': set(), 'raw_accounts': set(), 'srv_accs': set(), 'props': set()})
            ap['servers'].add(srv_name)
            ap['raw_accounts'].add(raw_acc)
            ap['srv_accs'].add((srv_name, raw_acc))
            ap['props'].add('autolink_guid')

    for sess_obj in srv_data.get('sessions', []):
      raw_acc = (sess_obj.get('account_name') or '').strip().lower()
      cacc = canonicalize_account(raw_acc, aliases, ignored)
      if not cacc:
        continue
      sess_nicks = sess_obj.get('nicknames', [])
      for nobj in sess_nicks:
        nraw = nobj.get('name') or u''
        nk = clean_alias_key(nraw)
        if nk:
          np = acc_nick_prov.setdefault(cacc, {}).setdefault(nk, {'servers': set(), 'sample': nraw})
          np['servers'].add(srv_name)

      cuid = sess_obj.get('cuid_hash2')
      sha1_ip = sess_obj.get('sha1_ip')
      pep_ips = sess_obj.get('pepper_ips') or []
      ip_entries = []  # [(iph, kind_str)]
      if sha1_ip is not None and not user_lib.ipIsBlacklisted(int(sha1_ip)) and int(sha1_ip) not in buggy_const_ips:
        ip_entries.append((int(sha1_ip), 'sha1'))
      for pip in pep_ips:
        pip = int(pip)
        if not user_lib.ipIsBlacklisted(pip) and pip not in buggy_const_ips:
          ip_entries.append((pip, 'pepper'))

      if cuid:
        cp = cuid_prov.setdefault(cuid, {}).setdefault(cacc, {'servers': set(), 'raw_accounts': set(), 'srv_accs': set(), 'nicks': {}})
        cp['servers'].add(srv_name)
        cp['raw_accounts'].add(raw_acc)
        cp['srv_accs'].add((srv_name, raw_acc))
        for nobj in sess_nicks[:4]:
          nm = nobj.get('name')
          if nm and session_alias_lib.is_helpful_alias(nm):
            cp['nicks'][nm] = cp['nicks'].get(nm, 0) + int(nobj.get('duration', 0))

        for iph, ikind in ip_entries:
          cip_rec = cuid_ip_prov.setdefault((cuid, iph), {}).setdefault(
            cacc, {'servers': set(), 'raw_accounts': set(), 'srv_accs': set(), 'ip_kinds': set(), 'nicks': {}}
          )
          cip_rec['servers'].add(srv_name)
          cip_rec['raw_accounts'].add(raw_acc)
          cip_rec['srv_accs'].add((srv_name, raw_acc))
          cip_rec['ip_kinds'].add(ikind)
          for nobj in sess_nicks[:3]:
            nm = nobj.get('name')
            if nm and session_alias_lib.is_helpful_alias(nm):
              cip_rec['nicks'][nm] = cip_rec['nicks'].get(nm, 0) + int(nobj.get('duration', 0))

      for iph, ikind in ip_entries:
        ip_rec = ip_prov.setdefault(iph, {}).setdefault(
          cacc, {'servers': set(), 'raw_accounts': set(), 'srv_accs': set(), 'ip_kinds': set(), 'nicks': {}}
        )
        ip_rec['servers'].add(srv_name)
        ip_rec['raw_accounts'].add(raw_acc)
        ip_rec['srv_accs'].add((srv_name, raw_acc))
        ip_rec['ip_kinds'].add(ikind)
        for nobj in sess_nicks[:3]:
          nm = nobj.get('name')
          if nm and session_alias_lib.is_helpful_alias(nm):
            ip_rec['nicks'][nm] = ip_rec['nicks'].get(nm, 0) + int(nobj.get('duration', 0))

    for pobj in srv_data.get('pugs', []):
      mhash = pobj.get('match_hash')
      if not mhash:
        continue
      mbucket = pug_by_match.setdefault(mhash, {})
      map_name = pobj.get('map', '')
      for pl in pobj.get('players', []):
        raw_acc = (pl.get('account_name') or '').strip().lower()
        cacc = canonicalize_account(raw_acc, aliases, ignored)
        team = pl.get('team')
        if not cacc or team not in ('RED', 'BLUE'):
          continue
        k = (cacc, team)
        if k not in mbucket:
          mbucket[k] = {
            'account': cacc,
            'raw_accounts': set([raw_acc]),
            'srv_accs': set([(srv_name, raw_acc)]),
            'team': team,
            'duration': int(pl.get('duration', 0)),
            'name': pl.get('name', ''),
            'map': map_name,
            'servers': set([srv_name]),
          }
        else:
          mbucket[k]['servers'].add(srv_name)
          mbucket[k]['raw_accounts'].add(raw_acc)
          mbucket[k]['srv_accs'].add((srv_name, raw_acc))

    for dobj in srv_data.get('direct_client_matches', []):
      raw_acc = (dobj.get('account_name') or '').strip().lower()
      cacc = canonicalize_account(raw_acc, aliases, ignored)
      mhash = dobj.get('match_hash')
      pname = dobj.get('player_name') or ''
      clean_pname = strip_colors(pname).lower()
      if not cacc or not mhash or not clean_pname:
        continue
      mbucket = direct_by_match.setdefault(mhash, {})
      cid = int(dobj['client_id'])
      kind = dobj.get('kind', 'fastcap')
      k = (cacc, cid, kind, clean_pname)
      if k not in mbucket:
        mbucket[k] = {
          'account': cacc,
          'raw_accounts': set([raw_acc]),
          'srv_accs': set([(srv_name, raw_acc)]),
          'client_id': cid,
          'kind': kind,
          'player_name': pname,
          'clean_pname': clean_pname,
          'servers': set([srv_name]),
        }
      else:
        mbucket[k]['servers'].add(srv_name)
        mbucket[k]['raw_accounts'].add(raw_acc)
        mbucket[k]['srv_accs'].add((srv_name, raw_acc))

  # 2. Load all MongoDB sessions, sessionPlayers, and sessionAliasSummary
  all_players = {p['_id']: p for p in db.sessionPlayers.find()}
  all_sessions = list(db.sessions.find())
  alias_map = session_alias_lib.ensure_session_aliases(db, all_sessions)

  known_real_ip_hashes = set()
  for srv_data in servers:
    for h_str, ip_s in (srv_data.get('cleartext_ips') or {}).iteritems():
      if ip_s and not ip_s.startswith(('0.', '127.', '10.', '192.168.')):
        try:
          known_real_ip_hashes.add(int(h_str))
        except ValueError:
          pass
  for hdoc in db.ipHashCache.find({}, {'_id': 1, 'ip': 1}):
    ip_s = hdoc.get('ip') or ''
    if ip_s and not ip_s.startswith(('0.', '127.', '10.', '192.168.')):
      known_real_ip_hashes.add(int(hdoc['_id']))

  mongo_ip_freq = {}
  mongo_ip_nmids = {}
  for s in all_sessions:
    iph = int(s['_id'].get('ip', 0))
    if iph != 0:
      mongo_ip_freq[iph] = mongo_ip_freq.get(iph, 0) + 1
      nm = s['_id'].get('newmod_id')
      if nm:
        mongo_ip_nmids.setdefault(iph, set()).add(nm)

  def is_usable_ip(iph):
    if iph == 0 or iph == -1 or iph in buggy_const_ips or user_lib.ipIsBlacklisted(iph):
      return False
    if iph in known_real_ip_hashes:
      return True
    # Catch any historical rotated pepper constant (spans many distinct NewMod CUIDs)
    if len(mongo_ip_nmids.get(iph, set())) >= 6 or mongo_ip_freq.get(iph, 0) >= 80:
      return False
    return True

  manual_shared_guids = set([
    int(d['value']) for d in db.profileSharedIdentifiers.find({'type': 'guid', 'manual_split': True})
    if d.get('value') is not None
  ])
  manual_shared_nmids = set([
    d['value'] for d in db.profileSharedIdentifiers.find({'type': 'newmod_id', 'manual_split': True})
    if d.get('value')
  ])

  session_by_key = {}
  tier1_signals = {}  # skey -> signal_dict

  for s in all_sessions:
    skey = player_lib.canonical_session_key(s['_id'])
    session_by_key[skey] = s
    if skey in pinned_sessions and pinned_sessions[skey].get('locked'):
      continue
    nmid = s['_id'].get('newmod_id')
    iph = int(s['_id'].get('ip', 0))
    if nmid and nmid not in manual_shared_nmids and nmid in cuid_prov:
      acc_map = cuid_prov[nmid]
      if len(acc_map) == 1:
        acc, pinfo = next(iter(acc_map.items()))
        tier1_signals[skey] = {
          'account': acc,
          'method': 'cuid_hash2',
          'tier': 1,
          'servers': sorted(list(pinfo['servers'])),
          'raw_accounts': sorted(list(pinfo['raw_accounts'])),
          'server_accounts': _fmt_srv_accs(pinfo['srv_accs']),
          'cuid': nmid,
          'enhanced_nicks': _top_dict_keys(pinfo['nicks'], 3),
        }
      elif is_usable_ip(iph) and (nmid, iph) in cuid_ip_prov and len(cuid_ip_prov[(nmid, iph)]) == 1:
        acc, pinfo = next(iter(cuid_ip_prov[(nmid, iph)].items()))
        tier1_signals[skey] = {
          'account': acc,
          'method': 'cuid_plus_ip',
          'tier': 1,
          'servers': sorted(list(pinfo['servers'])),
          'raw_accounts': sorted(list(pinfo['raw_accounts'])),
          'server_accounts': _fmt_srv_accs(pinfo['srv_accs']),
          'cuid': nmid,
          'ip': iph,
          'ip_kinds': sorted(list(pinfo['ip_kinds'])),
          'shared_cuid_accounts': sorted(list(acc_map.keys())),
          'enhanced_nicks': _top_dict_keys(pinfo['nicks'], 3),
        }

  # 3. Tier 2: Exact 64-bit match_id Join (pugs + fastcaps + topaims)
  all_match_hashes = sorted(set(pug_by_match.keys()) | set(direct_by_match.keys()))
  # skey -> {acc: {'count': N, 'servers': set, 'raw_accounts': set, 'ways': set, 'samples': [...]}}
  tier2_votes = {}

  batch_size = 600
  for i in range(0, len(all_match_hashes), batch_size):
    chunk = all_match_hashes[i:i + batch_size]
    sgs_by_match = {}
    for sg in db.sessionGames.find({'_id.match': {'$in': chunk}}):
      sgs_by_match.setdefault(sg['_id']['match'], []).append(sg)

    for mhash in chunk:
      sgs = sgs_by_match.get(mhash, [])
      if not sgs:
        continue

      # 3a. Direct (match_id, client_id, matching player_name) joins from fastcaps & topaims
      for dentry in direct_by_match.get(mhash, {}).values():
        cid = dentry['client_id']
        cpname = dentry['clean_pname']
        matching_sgs = []
        for sg in sgs:
          for g in sg.get('games', []):
            if int(g.get('clientid', -1)) == cid:
              g_names = set([strip_colors(nm.get('name', '')).lower() for nm in g.get('names', [])])
              if cpname in g_names:
                matching_sgs.append(sg)
                break
        if len(matching_sgs) == 1:
          skey = player_lib.canonical_session_key(matching_sgs[0]['_id']['session'])
          if not pinned_sessions.get(skey, {}).get('locked'):
            bucket = tier2_votes.setdefault(skey, {}).setdefault(
              dentry['account'],
              {'count': 0, 'servers': set(), 'raw_accounts': set(), 'srv_accs': set(), 'ways': set(), 'samples': []}
            )
            bucket['count'] += 1
            bucket['servers'].update(dentry['servers'])
            bucket['raw_accounts'].update(dentry['raw_accounts'])
            bucket['srv_accs'].update(dentry['srv_accs'])
            bucket['ways'].add('%s(slot+name)' % dentry['kind'])
            if len(bucket['samples']) < 5:
              bucket['samples'].append({
                'match': mhash,
                'kind': dentry['kind'],
                'servers': sorted(list(dentry['servers'])),
                'name': dentry['player_name'],
              })

      # 3b. PUG player rows in this match
      pug_players = list(pug_by_match.get(mhash, {}).values())
      if not pug_players:
        continue

      match_sgs = [sg for sg in sgs if sg.get('is_match')]
      for team_name in ('RED', 'BLUE'):
        team_pugs = [pp for pp in pug_players if pp['team'] == team_name]
        if not team_pugs:
          continue

        team_sgs = []
        for sg in match_sgs:
          t_dur = 0
          names_clean = set()
          for g in sg.get('games', []):
            for t in g.get('teams', []):
              if t.get('team') == team_name:
                t_dur += max(0, int(t.get('end', 0)) - int(t.get('start', 0)))
            for nm in g.get('names', []):
              names_clean.add(strip_colors(nm.get('name', '')).lower())
          if t_dur > 0:
            skey = player_lib.canonical_session_key(sg['_id']['session'])
            team_sgs.append({
              'sg': sg,
              'skey': skey,
              'dur': t_dur,
              'names': names_clean,
              't1_acc': tier1_signals[skey]['account'] if skey in tier1_signals else None,
            })

        used_sg_idxs = set()
        used_pp_idxs = set()
        for pp_idx, pp in enumerate(team_pugs):
          t1_matches = [
            idx for idx, c in enumerate(team_sgs)
            if idx not in used_sg_idxs and c['t1_acc'] == pp['account']
          ]
          if len(t1_matches) == 1:
            sidx = t1_matches[0]
            used_sg_idxs.add(sidx)
            used_pp_idxs.add(pp_idx)
            skey = team_sgs[sidx]['skey']
            bucket = tier2_votes.setdefault(skey, {}).setdefault(
              pp['account'],
              {'count': 0, 'servers': set(), 'raw_accounts': set(), 'srv_accs': set(), 'ways': set(), 'samples': []}
            )
            bucket['count'] += 1
            bucket['servers'].update(pp['servers'])
            bucket['raw_accounts'].update(pp['raw_accounts'])
            bucket['srv_accs'].update(pp['srv_accs'])
            bucket['ways'].add('pug(cuid_lock)')
            if len(bucket['samples']) < 5:
              bucket['samples'].append({
                'match': mhash,
                'kind': 'pug',
                'servers': sorted(list(pp['servers'])),
                'map': pp.get('map', ''),
                'name': pp.get('name', ''),
              })

        for pp_idx, pp in enumerate(team_pugs):
          if pp_idx in used_pp_idxs:
            continue
          rem_sgs = [(idx, c) for idx, c in enumerate(team_sgs) if idx not in used_sg_idxs and c['t1_acc'] is None]
          if not rem_sgs:
            continue
          pp_clean = strip_colors(pp['name']).lower()
          chosen_idx = None
          way_label = None
          name_matches = [(idx, c) for idx, c in rem_sgs if pp_clean and pp_clean in c['names']]
          if len(name_matches) == 1:
            chosen_idx = name_matches[0][0]
            way_label = 'pug(team+name)'
          elif len(name_matches) > 1:
            by_dur = sorted(name_matches, key=lambda x: abs(x[1]['dur'] - pp['duration']))
            if abs(by_dur[0][1]['dur'] - pp['duration']) + 15000 < abs(by_dur[1][1]['dur'] - pp['duration']):
              chosen_idx = by_dur[0][0]
              way_label = 'pug(team+name+dur)'
          elif len(rem_sgs) == 1 and len([p_i for p_i in range(len(team_pugs)) if p_i not in used_pp_idxs]) == 1:
            if abs(rem_sgs[0][1]['dur'] - pp['duration']) <= 120000:
              chosen_idx = rem_sgs[0][0]
              way_label = 'pug(4th_teammate)'

          if chosen_idx is not None:
            used_sg_idxs.add(chosen_idx)
            used_pp_idxs.add(pp_idx)
            skey = team_sgs[chosen_idx]['skey']
            if not pinned_sessions.get(skey, {}).get('locked'):
              bucket = tier2_votes.setdefault(skey, {}).setdefault(
                pp['account'],
                {'count': 0, 'servers': set(), 'raw_accounts': set(), 'srv_accs': set(), 'ways': set(), 'samples': []}
              )
              bucket['count'] += 1
              bucket['servers'].update(pp['servers'])
              bucket['raw_accounts'].update(pp['raw_accounts'])
              bucket['srv_accs'].update(pp['srv_accs'])
              bucket['ways'].add(way_label)
              if len(bucket['samples']) < 5:
                bucket['samples'].append({
                  'match': mhash,
                  'kind': 'pug',
                  'servers': sorted(list(pp['servers'])),
                  'map': pp.get('map', ''),
                  'name': pp.get('name', ''),
                })

  # Combine Tier 1 + Tier 2 into unified session_attr:
  # skey -> {'account': acc, 'primary_method': ..., 'servers': [...], 'raw_accounts': [...], 'signals': [...]}
  session_attr = {}
  ambiguous_sessions = {}  # skey -> {'votes': {acc: {...}}}

  for skey, sig1 in tier1_signals.iteritems():
    session_attr[skey] = {
      'account': sig1['account'],
      'primary_method': sig1['method'],
      'servers': list(sig1['servers']),
      'raw_accounts': list(sig1['raw_accounts']),
      'server_accounts': list(sig1.get('server_accounts', [])),
      'signals': [sig1],
    }

  for skey, votes in tier2_votes.iteritems():
    if skey in session_attr:
      t1_acc = session_attr[skey]['account']
      other_votes = sum([v['count'] for a, v in votes.iteritems() if a != t1_acc])
      own_info = votes.get(t1_acc)
      own_votes = own_info['count'] if own_info else 0
      if other_votes >= 2 and other_votes > own_votes:
        vbreak = {}
        for a, v in votes.iteritems():
          vbreak[a] = {
            'count': v['count'],
            'servers': sorted(list(v['servers'])),
            'raw_accounts': sorted(list(v['raw_accounts'])),
            'server_accounts': _fmt_srv_accs(v['srv_accs']),
            'samples': v['samples'],
          }
        vbreak.setdefault(t1_acc, {
          'count': own_votes,
          'servers': session_attr[skey]['servers'],
          'raw_accounts': session_attr[skey]['raw_accounts'],
          'server_accounts': session_attr[skey]['server_accounts'],
          'cuid_signal': session_attr[skey]['signals'][0],
          'samples': [],
        })
        ambiguous_sessions[skey] = vbreak
        del session_attr[skey]
      elif own_info:
        # Attach corroborating Tier 2 PUG/fastcap evidence alongside Tier 1!
        sig2 = {
          'account': t1_acc,
          'method': 'match_id',
          'tier': 2,
          'matches_count': own_info['count'],
          'servers': sorted(list(own_info['servers'])),
          'raw_accounts': sorted(list(own_info['raw_accounts'])),
          'server_accounts': _fmt_srv_accs(own_info['srv_accs']),
          'ways': sorted(list(own_info['ways'])),
          'samples': own_info['samples'],
        }
        session_attr[skey]['signals'].append(sig2)
        session_attr[skey]['servers'] = sorted(list(set(session_attr[skey]['servers']) | own_info['servers']))
        session_attr[skey]['raw_accounts'] = sorted(list(set(session_attr[skey]['raw_accounts']) | own_info['raw_accounts']))
        existing_sa = {(x['server'], x['account']) for x in session_attr[skey]['server_accounts']}
        session_attr[skey]['server_accounts'] = _fmt_srv_accs(existing_sa | own_info['srv_accs'])
      continue

    total_v = sum([v['count'] for v in votes.values()])
    ranked = sorted(votes.items(), key=lambda x: x[1]['count'], reverse=True)
    top_acc, top_info = ranked[0]
    if len(ranked) == 1 or (top_info['count'] >= 3 and float(top_info['count']) / float(total_v) >= 0.90):
      sig2 = {
        'account': top_acc,
        'method': 'match_id',
        'tier': 2,
        'matches_count': top_info['count'],
        'servers': sorted(list(top_info['servers'])),
        'raw_accounts': sorted(list(top_info['raw_accounts'])),
        'server_accounts': _fmt_srv_accs(top_info['srv_accs']),
        'ways': sorted(list(top_info['ways'])),
        'samples': top_info['samples'],
      }
      session_attr[skey] = {
        'account': top_acc,
        'primary_method': 'match_id',
        'servers': sig2['servers'],
        'raw_accounts': sig2['raw_accounts'],
        'server_accounts': sig2['server_accounts'],
        'signals': [sig2],
      }
    else:
      ambiguous_sessions[skey] = {
        a: {
          'count': v['count'],
          'servers': sorted(list(v['servers'])),
          'raw_accounts': sorted(list(v['raw_accounts'])),
          'server_accounts': _fmt_srv_accs(v['srv_accs']),
          'samples': v['samples'],
        }
        for a, v in votes.iteritems()
      }

  # Identify shared GUIDs across >1 canonical account from Tier 1+2 before applying Tier 3
  guid_observed_accs = {}
  for skey, ainfo in session_attr.iteritems():
    gval = int(session_by_key[skey]['_id'].get('guid', 0))
    if gval != 0:
      guid_observed_accs.setdefault(gval, set()).add(ainfo['account'])
  for gval, amap in autolink_prov.iteritems():
    guid_observed_accs.setdefault(gval, set()).update(amap.keys())

  shared_guids = set([g for g, aset in guid_observed_accs.iteritems() if len(aset) > 1]) | manual_shared_guids
  shared_nmids = set([c for c, amap in cuid_prov.iteritems() if len(amap) > 1]) | manual_shared_nmids

  # Tier 3: Admin autolink GUIDs
  for s in all_sessions:
    skey = player_lib.canonical_session_key(s['_id'])
    if pinned_sessions.get(skey, {}).get('locked') or skey in ambiguous_sessions:
      continue
    gval = int(s['_id'].get('guid', 0))
    if gval != 0 and gval not in shared_guids and gval in autolink_prov:
      amap = autolink_prov[gval]
      if len(amap) == 1:
        acc, pinfo = next(iter(amap.items()))
        sig3 = {
          'account': acc,
          'method': 'autolink_guid',
          'tier': 3,
          'guid': gval,
          'props': sorted(list(pinfo['props'])),
          'servers': sorted(list(pinfo['servers'])),
          'raw_accounts': sorted(list(pinfo['raw_accounts'])),
          'server_accounts': _fmt_srv_accs(pinfo['srv_accs']),
        }
        if skey not in session_attr:
          session_attr[skey] = {
            'account': acc,
            'primary_method': 'autolink_guid',
            'servers': sig3['servers'],
            'raw_accounts': sig3['raw_accounts'],
            'server_accounts': sig3['server_accounts'],
            'signals': [sig3],
          }
        elif session_attr[skey]['account'] == acc:
          session_attr[skey]['signals'].append(sig3)
          session_attr[skey]['servers'] = sorted(list(set(session_attr[skey]['servers']) | pinfo['servers']))
          existing_sa = {(x['server'], x['account']) for x in session_attr[skey]['server_accounts']}
          session_attr[skey]['server_accounts'] = _fmt_srv_accs(existing_sa | pinfo['srv_accs'])

  # Compute account active time ranges from Tier 1-3 sessions for Tier 4 temporal corroboration
  acc_time_ranges = {}  # acc -> list of (first_game, last_game, servers_list)
  for skey, ainfo in session_attr.iteritems():
    s = session_by_key[skey]
    fg, lg = s.get('first_game'), s.get('last_game')
    if fg and lg:
      acc_time_ranges.setdefault(ainfo['account'], []).append((fg, lg, ainfo['servers']))

  # 4. Tier 4: Exclusive + Corroborated IP Hash (for remaining non-NewMod sessions)
  uncorroborated_ip_candidates = []
  window_delta = datetime.timedelta(days=14)

  for s in all_sessions:
    skey = player_lib.canonical_session_key(s['_id'])
    if skey in session_attr or skey in ambiguous_sessions or pinned_sessions.get(skey, {}).get('locked'):
      continue
    if s['_id'].get('newmod_id'):
      continue
    iph = int(s['_id'].get('ip', 0))
    if not is_usable_ip(iph):
      continue
    amap = ip_prov.get(iph)
    if not amap or len(amap) != 1:
      continue
    acc, pinfo = next(iter(amap.items()))

    # Check alias corroboration across ALL helpful aliases of this session (plus last_name)
    adoc = alias_map.get(skey) or {}
    candidate_raw_names = [tn['name'] for tn in adoc.get('top_names', []) if tn.get('helpful')]
    if s.get('last_name'):
      candidate_raw_names.append(s['last_name'])

    matched_nick = None
    matched_nick_servers = []
    nick_dict = acc_nick_prov.get(acc, {})
    for rname in candidate_raw_names:
      nk = clean_alias_key(rname)
      if nk and nk in nick_dict:
        matched_nick = strip_colors(rname)
        matched_nick_servers = sorted(list(nick_dict[nk]['servers']))
        break

    best_days_apart = None
    anchor_servers = []
    fg, lg = s.get('first_game'), s.get('last_game')
    if fg and lg and acc in acc_time_ranges:
      for (afg, alg, asrvs) in acc_time_ranges[acc]:
        if fg <= (alg + window_delta) and lg >= (afg - window_delta):
          if fg > alg:
            gap_sec = (fg - alg).total_seconds()
          elif afg > lg:
            gap_sec = (afg - lg).total_seconds()
          else:
            gap_sec = 0.0
          gap_days = round(gap_sec / 86400.0, 1)
          if best_days_apart is None or gap_days < best_days_apart:
            best_days_apart = gap_days
            anchor_servers = asrvs

    if matched_nick is not None or best_days_apart is not None:
      signals = []
      srvs_union = set(pinfo['servers'])
      srv_accs_fmt = _fmt_srv_accs(pinfo['srv_accs'])
      if matched_nick is not None:
        srvs_union.update(matched_nick_servers)
        signals.append({
          'account': acc,
          'method': 'ip+nick',
          'tier': 4,
          'ip': iph,
          'ip_kinds': sorted(list(pinfo['ip_kinds'])),
          'servers': sorted(list(pinfo['servers'])),
          'raw_accounts': sorted(list(pinfo['raw_accounts'])),
          'server_accounts': srv_accs_fmt,
          'matched_nick': matched_nick,
          'nick_servers': matched_nick_servers,
          'enhanced_nicks': _top_dict_keys(pinfo['nicks'], 3),
        })
      if best_days_apart is not None:
        signals.append({
          'account': acc,
          'method': 'ip+time_14d',
          'tier': 4,
          'ip': iph,
          'ip_kinds': sorted(list(pinfo['ip_kinds'])),
          'servers': sorted(list(pinfo['servers'])),
          'raw_accounts': sorted(list(pinfo['raw_accounts'])),
          'server_accounts': srv_accs_fmt,
          'days_apart': best_days_apart,
          'anchor_servers': anchor_servers,
          'enhanced_nicks': _top_dict_keys(pinfo['nicks'], 3),
        })
      session_attr[skey] = {
        'account': acc,
        'primary_method': signals[0]['method'],
        'servers': sorted(list(pinfo['servers'])),
        'raw_accounts': sorted(list(pinfo['raw_accounts'])),
        'server_accounts': srv_accs_fmt,
        'signals': signals,
      }
    elif int(s.get('num_matches', 0)) > 0:
      uncorroborated_ip_candidates.append({
        'skey': skey,
        'session': s,
        'account': acc,
        'ip': iph,
        'ip_kinds': sorted(list(pinfo['ip_kinds'])),
        'servers': sorted(list(pinfo['servers'])),
        'raw_accounts': sorted(list(pinfo['raw_accounts'])),
        'server_accounts': _fmt_srv_accs(pinfo['srv_accs']),
        'enhanced_nicks': _top_dict_keys(pinfo['nicks'], 3),
      })

  # 4b. Network / BGP ASN / PTR / Parallel UDP Traceroute Disambiguation Layer
  # Priority traceroute subnets: shared GUIDs, shared CUIDs, ambiguous sessions, and multi-account profiles
  prelim_pid_accs = {}
  for skey, ainfo in session_attr.iteritems():
    pid = session_by_key[skey].get('playerid')
    if pid is not None:
      prelim_pid_accs.setdefault(pid, set()).add(ainfo['account'])

  priority_trace_hashes = set()
  for s in all_sessions:
    iph = int(s['_id'].get('ip', 0))
    if not is_usable_ip(iph):
      continue
    gval = int(s['_id'].get('guid', 0))
    nmid = s['_id'].get('newmod_id')
    pid = s.get('playerid')
    if (
      (gval != 0 and gval in shared_guids) or
      (nmid and nmid in shared_nmids) or
      (pid in prelim_pid_accs and len(prelim_pid_accs[pid]) >= 2)
    ):
      priority_trace_hashes.add(iph)

  # Also include Tier 1-2 session IPs of accounts involved in shared GUIDs so their reference PoPs are traced!
  shared_guid_accs = set()
  for gval in shared_guids:
    shared_guid_accs.update(guid_observed_accs.get(gval, set()))
  for skey, ainfo in session_attr.iteritems():
    if ainfo['account'] in shared_guid_accs and ainfo['primary_method'] in ('cuid_hash2', 'cuid_plus_ip', 'match_id'):
      iph = int(session_by_key[skey]['_id'].get('ip', 0))
      if is_usable_ip(iph):
        priority_trace_hashes.add(iph)

  ip_net_map = network_disambig_lib.sync_and_load_network_maps(
    db, servers, all_sessions, is_usable_ip, priority_trace_ip_hashes=priority_trace_hashes
  )

  # Build each canonical account's strong residential ASN & Traceroute PoP footprint
  acc_asns = {}   # acc -> {asn: {'servers': set, 'raw_accounts': set, 'srv_accs': set, 'asn_name': str, 'country': str}}
  asn_to_accs = {}
  acc_pops = {}   # acc -> set(pop_sig)

  # Seed from linked enhanced.db sessions directly
  for srv_data in servers:
    srv_name = srv_data.get('server', '?')
    for s_obj in srv_data.get('sessions', []):
      raw_acc = (s_obj.get('account_name') or '').strip().lower()
      cacc = canonicalize_account(raw_acc, aliases, ignored)
      sh = s_obj.get('sha1_ip')
      if not cacc or sh is None:
        continue
      net = ip_net_map.get(int(sh))
      if net and net.get('asn') and not net.get('is_vpn'):
        asn = int(net['asn'])
        rec = acc_asns.setdefault(cacc, {}).setdefault(
          asn, {'servers': set(), 'raw_accounts': set(), 'srv_accs': set(), 'asn_name': net.get('asn_name', ''), 'country': net.get('country', '')}
        )
        rec['servers'].add(srv_name)
        rec['raw_accounts'].add(raw_acc)
        rec['srv_accs'].add((srv_name, raw_acc))
        asn_to_accs.setdefault(asn, set()).add(cacc)
        if net.get('pop_sig'):
          acc_pops.setdefault(cacc, set()).add(net['pop_sig'])

  # Also include Tier 1-3 strong archive sessions
  for skey, ainfo in session_attr.iteritems():
    if ainfo['primary_method'] not in ('cuid_hash2', 'cuid_plus_ip', 'match_id', 'autolink_guid'):
      continue
    cacc = ainfo['account']
    iph = int(session_by_key[skey]['_id'].get('ip', 0))
    net = ip_net_map.get(iph)
    if net and net.get('asn') and not net.get('is_vpn'):
      asn = int(net['asn'])
      rec = acc_asns.setdefault(cacc, {}).setdefault(
        asn, {'servers': set(), 'raw_accounts': set(), 'srv_accs': set(), 'asn_name': net.get('asn_name', ''), 'country': net.get('country', '')}
      )
      rec['servers'].update(ainfo['servers'])
      rec['raw_accounts'].update(ainfo['raw_accounts'])
      for sa in ainfo.get('server_accounts', []):
        rec['srv_accs'].add((sa['server'], sa['account']))
      asn_to_accs.setdefault(asn, set()).add(cacc)
      if net.get('pop_sig'):
        acc_pops.setdefault(cacc, set()).add(net['pop_sig'])

  # Rule 4c: Shared-GUID + Dynamic-IP Network/Traceroute Disambiguation
  for s in all_sessions:
    skey = player_lib.canonical_session_key(s['_id'])
    if skey in session_attr or skey in ambiguous_sessions or pinned_sessions.get(skey, {}).get('locked'):
      continue
    gval = int(s['_id'].get('guid', 0))
    iph = int(s['_id'].get('ip', 0))
    net = ip_net_map.get(iph)
    if gval == 0 or gval not in guid_observed_accs or not net or not net.get('asn') or net.get('is_vpn'):
      continue
    asn = int(net['asn'])
    if len(asn_to_accs.get(asn, set())) > 3:
      continue
    cand_accs = guid_observed_accs[gval]
    asn_matches = [a for a in cand_accs if asn in acc_asns.get(a, {})]
    chosen_acc = None
    via_mode = None
    if len(asn_matches) == 1:
      chosen_acc = asn_matches[0]
      via_mode = 'guid+net_trace'
    elif len(asn_matches) > 1 and net.get('pop_sig'):
      pop_matches = [a for a in asn_matches if net['pop_sig'] in acc_pops.get(a, set())]
      if len(pop_matches) == 1:
        chosen_acc = pop_matches[0]
        via_mode = 'guid+traceroute_pop'

    if chosen_acc is not None:
      arec = acc_asns[chosen_acc][asn]
      sig_net = {
        'account': chosen_acc,
        'method': via_mode,
        'tier': 4,
        'guid': gval,
        'asn': asn,
        'asn_name': net.get('asn_name', ''),
        'country': net.get('country', ''),
        'ptr': net.get('ptr', ''),
        'pop_sig': net.get('pop_sig', ''),
        'ip_source': net.get('ip_source', ''),
        'disambiguated_against': sorted([a for a in cand_accs if a != chosen_acc]),
        'servers': sorted(list(arec['servers'])),
        'raw_accounts': sorted(list(arec['raw_accounts'])),
        'server_accounts': _fmt_srv_accs(arec['srv_accs']),
      }
      session_attr[skey] = {
        'account': chosen_acc,
        'primary_method': via_mode,
        'servers': sig_net['servers'],
        'raw_accounts': sig_net['raw_accounts'],
        'server_accounts': sig_net['server_accounts'],
        'signals': [sig_net],
      }
      if s.get('playerid') is not None:
        prelim_pid_accs.setdefault(s['playerid'], set()).add(chosen_acc)

  # Rule 4d: Same-Profile Dynamic-IP Network/Traceroute Confirmation
  # Confirms unattributed dynamic-IP sessions on profiles that already have attributed sessions (e.g. fay/amelia, gelad)
  for s in all_sessions:
    skey = player_lib.canonical_session_key(s['_id'])
    if skey in session_attr or skey in ambiguous_sessions or pinned_sessions.get(skey, {}).get('locked'):
      continue
    pid = s.get('playerid')
    if pid is None or pid not in prelim_pid_accs:
      continue
    iph = int(s['_id'].get('ip', 0))
    net = ip_net_map.get(iph)
    if not net or not net.get('asn') or net.get('is_vpn'):
      continue
    asn = int(net['asn'])
    if len(asn_to_accs.get(asn, set())) > 3:
      continue
    pid_cands = prelim_pid_accs[pid]
    asn_matches = [a for a in pid_cands if asn in acc_asns.get(a, {})]
    chosen_acc = None
    via_mode = None
    if len(asn_matches) == 1:
      chosen_acc = asn_matches[0]
      via_mode = 'dyn_ip+net_trace'
    elif len(asn_matches) > 1 and net.get('pop_sig'):
      pop_matches = [a for a in asn_matches if net['pop_sig'] in acc_pops.get(a, set())]
      if len(pop_matches) == 1:
        chosen_acc = pop_matches[0]
        via_mode = 'dyn_ip+traceroute_pop'

    if chosen_acc is not None:
      arec = acc_asns[chosen_acc][asn]
      sig_net = {
        'account': chosen_acc,
        'method': via_mode,
        'tier': 4,
        'asn': asn,
        'asn_name': net.get('asn_name', ''),
        'country': net.get('country', ''),
        'ptr': net.get('ptr', ''),
        'pop_sig': net.get('pop_sig', ''),
        'ip_source': net.get('ip_source', ''),
        'servers': sorted(list(arec['servers'])),
        'raw_accounts': sorted(list(arec['raw_accounts'])),
        'server_accounts': _fmt_srv_accs(arec['srv_accs']),
      }
      session_attr[skey] = {
        'account': chosen_acc,
        'primary_method': via_mode,
        'servers': sig_net['servers'],
        'raw_accounts': sig_net['raw_accounts'],
        'server_accounts': sig_net['server_accounts'],
        'signals': [sig_net],
      }

  # Remove any uncorroborated_ip_candidates that were just resolved by 4c/4d
  uncorroborated_ip_candidates = [c for c in uncorroborated_ip_candidates if c['skey'] not in session_attr]

  # 5. Update db.profileSharedIdentifiers
  db.profileSharedIdentifiers.delete_many({'manual_split': {'$ne': True}})
  shared_docs = []
  for c in sorted(shared_nmids - manual_shared_nmids):
    shared_docs.append({
      '_id': 'newmod_id:%s' % c,
      'type': 'newmod_id',
      'value': c,
      'accounts': sorted(list(cuid_prov.get(c, {}).keys())),
      'manual_split': False,
    })
  for g in sorted(shared_guids - manual_shared_guids):
    shared_docs.append({
      '_id': 'guid:%d' % g,
      'type': 'guid',
      'value': int(g),
      'accounts': sorted(list(guid_observed_accs.get(g, []))),
      'manual_split': False,
    })
  if shared_docs:
    try:
      db.profileSharedIdentifiers.insert_many(shared_docs, ordered=False)
    except Exception:
      pass

  # 6. Group sessions by current playerid and determine home playerid per canonical account
  pid_sessions = {}
  for s in all_sessions:
    pid = s.get('playerid')
    if pid is not None:
      pid_sessions.setdefault(pid, []).append(s)

  pid_acc_breakdown = {}
  for pid, slist in pid_sessions.iteritems():
    bmap = {}
    for s in slist:
      skey = player_lib.canonical_session_key(s['_id'])
      if skey in session_attr:
        ainfo = session_attr[skey]
        acc = ainfo['account']
        method = ainfo['primary_method']
        entry = bmap.setdefault(acc, {
          'sessions': [],
          'matches': 0,
          'games': 0,
          'strong': False,
          'methods': set(),
          'servers': set(),
        })
        entry['sessions'].append(s)
        entry['matches'] += int(s.get('num_matches', 0))
        entry['games'] += int(s.get('num_games', 0))
        for sig in ainfo['signals']:
          entry['methods'].add(sig['method'])
        entry['servers'].update(ainfo['servers'])
        if method in ('cuid_hash2', 'cuid_plus_ip', 'match_id', 'autolink_guid'):
          entry['strong'] = True
    if bmap:
      pid_acc_breakdown[pid] = bmap

  HANDICAP_WORDS = ('laptop', 'touchpad', 'trackpad', 'shittymouse', 'moonping', 'handicap')
  HANDICAP_SUFFIXES = ('_laptop', '_touchpad', '_shittymouse', '_moonping')
  LEET_MAP = {ord(u'0'): u'o', ord(u'1'): u'i', ord(u'3'): u'e', ord(u'4'): u'a', ord(u'5'): u's', ord(u'7'): u't'}

  def _extract_label_matched_accounts(raw_label, bmap_keys=()):
    if not raw_label:
      return set(), False
    clean_s = strip_colors(raw_label)
    if not isinstance(clean_s, unicode):
      clean_s = clean_s.decode('utf-8', 'replace')
    ascii_s = unicodedata.normalize('NFKD', clean_s).encode('ascii', 'ignore').decode('ascii')
    lower_s = ascii_s.lower().strip()
    if not lower_s:
      return set(), False

    has_hc = any([hw in lower_s for hw in HANDICAP_WORDS])
    raw_toks = set([t for t in re.split(r'[^a-z0-9_]+', lower_s) if t])
    camel_toks = set([w.lower() for w in re.findall(r'[A-Z]?[a-z]+|[A-Z]+(?=[A-Z]|$)|[0-9]+', ascii_s) if w])
    leet_s = unicode(lower_s).translate(LEET_MAP)
    leet_toks = set([t for t in re.split(r'[^a-z0-9_]+', str(leet_s)) if t])
    all_toks = raw_toks | camel_toks | leet_toks

    matched = set()
    for full_cand in (lower_s, str(leet_s)):
      mc = canonicalize_account(full_cand, aliases, ignored)
      if mc and mc in all_known_accounts:
        if not has_hc or ('_' in mc or any([hw in mc for hw in HANDICAP_WORDS])):
          matched.add(mc)

    for tok in all_toks:
      if tok in HANDICAP_WORDS:
        continue
      if has_hc:
        for sfx in HANDICAP_SUFFIXES:
          hc_cand = canonicalize_account(tok + sfx, aliases, ignored)
          if hc_cand and hc_cand in all_known_accounts:
            matched.add(hc_cand)
        continue
      tc = canonicalize_account(tok, aliases, ignored)
      if tc and tc in all_known_accounts and not any([hw in tc for hw in HANDICAP_WORDS]):
        matched.add(tc)

    if not has_hc:
      compact_s = re.sub(r'[^a-z0-9]+', '', lower_s)
      for bacc in bmap_keys:
        if len(bacc) >= 4 and not any([hw in bacc for hw in HANDICAP_WORDS]):
          bacc_compact = re.sub(r'[^a-z0-9]+', '', bacc)
          if len(bacc_compact) >= 4 and bacc_compact in compact_s:
            matched.add(bacc)

    return matched, has_hc

  pid_owner_acc = {}
  for pid, bmap in pid_acc_breakdown.iteritems():
    pdoc = all_players.get(pid, {})
    total_sessions = len(pid_sessions.get(pid, []))
    total_matches = int(pdoc.get('num_matches', 0))
    total_games = int(pdoc.get('num_games', 0))

    existing_ea = canonicalize_account(pdoc.get('enhanced_account'), aliases, ignored)
    if existing_ea:
      pid_owner_acc[pid] = existing_ea
      continue

    verified_matched_accs, v_has_hc = _extract_label_matched_accounts(pdoc.get('name', ''), bmap.keys())
    matched_in_bmap = [a for a in bmap.keys() if a in verified_matched_accs]
    if len(matched_in_bmap) == 1:
      pid_owner_acc[pid] = matched_in_bmap[0]
      continue

    # Single-account historical veteran check: if only ONE enhanced.db account appears on this profile
    # and the profile's own competitive alias (#1 by matches or best_name) matches that account, own it
    # rather than splitting post-2023 sessions out of a pre-2023 veteran profile!
    if len(bmap) == 1 and not v_has_hc:
      only_acc = next(iter(bmap.keys()))
      if not any([hw in only_acc for hw in HANDICAP_WORDS]):
        prof_al = session_alias_lib.summarize_profile_aliases(pid_sessions.get(pid, []), alias_map, pdoc)
        alias_cands = set()
        m_best, hc_best = _extract_label_matched_accounts(prof_al.get('best_name', ''), bmap.keys())
        if not hc_best:
          alias_cands.update(m_best)
        top_n = prof_al.get('top_names') or []
        if top_n:
          m_top0, hc_top0 = _extract_label_matched_accounts(top_n[0].get('name', ''), bmap.keys())
          if not hc_top0:
            alias_cands.update(m_top0)
        if only_acc in alias_cands:
          pid_owner_acc[pid] = only_acc
          continue

    qualified = []
    for a, info in bmap.iteritems():
      s_cnt = len(info['sessions'])
      m_cnt = info['matches']
      g_cnt = info['games']
      if verified_matched_accs and a not in verified_matched_accs:
        if not (info['strong'] and m_cnt >= max(5, int(0.50 * max(1, total_matches)))):
          continue
      if v_has_hc and not any([hw in a for hw in HANDICAP_WORDS]):
        if not (info['strong'] and m_cnt >= max(5, int(0.50 * max(1, total_matches)))):
          continue
      if s_cnt == total_sessions:
        qualified.append((a, info))
      elif total_matches >= 5:
        if m_cnt >= max(1, int(0.20 * total_matches)) or (m_cnt >= 1 and s_cnt >= int(0.50 * total_sessions)):
          qualified.append((a, info))
      else:
        if g_cnt >= max(1, int(0.25 * max(1, total_games))) or s_cnt >= max(1, int(0.50 * max(1, total_sessions))):
          qualified.append((a, info))

    if qualified:
      qualified.sort(key=lambda x: (x[1]['matches'], x[1]['games']), reverse=True)
      pid_owner_acc[pid] = qualified[0][0]
    elif len(verified_matched_accs) == 1:
      pid_owner_acc[pid] = next(iter(verified_matched_accs))

  acc_home_pid = {}
  all_candidate_accs = set([a for bmap in pid_acc_breakdown.values() for a in bmap.keys()])
  for pid, pdoc in all_players.iteritems():
    ea = canonicalize_account(pdoc.get('enhanced_account'), aliases, ignored)
    if ea:
      all_candidate_accs.add(ea)
      pid_owner_acc.setdefault(pid, ea)

  for acc in sorted(all_candidate_accs):
    owned_pids = [pid for pid, oacc in pid_owner_acc.iteritems() if oacc == acc]
    if not owned_pids:
      continue

    def home_sort_key(pid):
      pdoc = all_players.get(pid, {})
      has_ea = 1 if canonicalize_account(pdoc.get('enhanced_account'), aliases, ignored) == acc else 0
      has_vname = 1 if pdoc.get('name') else 0
      return (-has_ea, -has_vname, pid)

    owned_pids.sort(key=home_sort_key)
    acc_home_pid[acc] = owned_pids[0]

  def profile_best_label(pid):
    pdoc = all_players.get(pid, {})
    slist = pid_sessions.get(pid, [])
    prof_al = session_alias_lib.summarize_profile_aliases(slist, alias_map, pdoc)
    return strip_colors(prof_al['best_name']) or str(pid)[:8]

  # 7. Generate Proposals WITH Full Per-Session Criteria, Server Provenance & Real Aliases
  proposals = []

  def make_prop_id(prefix, key_str):
    if isinstance(key_str, unicode):
      key_bytes = key_str.encode('utf-8')
    else:
      key_bytes = str(key_str)
    return prefix + '_' + hashlib.sha1(key_bytes).hexdigest()[:16]

  # 7a. Ambiguous single-session conflicts
  for skey, vmap in ambiguous_sessions.iteritems():
    s = session_by_key[skey]
    pid = s.get('playerid')
    srow = _format_session_row(s, alias_map, attr_info=None, vote_breakdown=vmap, ip_net_map=ip_net_map)
    srvs = sorted(list(set([srv for v in vmap.values() for srv in v.get('servers', [])])))
    proposals.append({
      '_id': make_prop_id('conflict_sess', skey),
      'type': 'conflict',
      'subtype': 'ambiguous_session',
      'tier_auto': False,
      'accounts': sorted(vmap.keys()),
      'playerids': [pid] if pid else [],
      'sessions': [s['_id']],
      'summary': 'Single session "%s" used by multiple accounts (%s)' % (
        strip_colors(srow['primary_name']),
        ', '.join(['@%s (%d)' % (k, v['count']) for k, v in sorted(vmap.items())])
      ),
      'evidence': {
        'servers': srvs,
        'methods': ['ambiguous_match_votes'],
        'vote_counts': {k: v['count'] for k, v in vmap.items()},
        'session_rows': [srow],
      },
    })

  # 7b. Session splits (when pid has sessions belonging to non-owner account B)
  for pid, bmap in pid_acc_breakdown.iteritems():
    owner_acc = pid_owner_acc.get(pid)
    pdoc = all_players.get(pid, {})
    from_prof_aliases = session_alias_lib.summarize_profile_aliases(pid_sessions.get(pid, []), alias_map, pdoc)
    for acc, info in bmap.iteritems():
      if acc == owner_acc:
        continue
      target_pid = acc_home_pid.get(acc)
      s_docs = sorted(
        info['sessions'],
        key=lambda x: (int(x.get('num_matches', 0)), int(x.get('num_games', 0))),
        reverse=True
      )
      s_ids = [s['_id'] for s in s_docs]
      s_rows = [
        _format_session_row(
          s,
          alias_map,
          attr_info=session_attr.get(player_lib.canonical_session_key(s['_id'])),
          ip_net_map=ip_net_map
        )
        for s in s_docs
      ]

      is_auto = bool(info['strong'] or not pdoc.get('name'))
      from_label = strip_colors(from_prof_aliases['best_name']) or str(pid)[:8]
      to_label = ('into "%s"' % profile_best_label(target_pid)) if target_pid else 'into new profile'
      prop_key = '%s->%s:%s' % (str(pid), acc, ','.join(sorted([player_lib.canonical_session_key(x) for x in s_ids])))
      split_raw_accs = sorted(list({ra for r in s_rows for ra in r.get('raw_accounts', [])}))
      proposals.append({
        '_id': make_prop_id('split', prop_key),
        'type': 'split',
        'tier_auto': is_auto,
        'accounts': [x for x in [owner_acc, acc] if x],
        'owner_account': owner_acc,
        'from_playerid': pid,
        'target_playerid': target_pid,
        'target_account': acc,
        'playerids': [p for p in [pid, target_pid] if p is not None],
        'sessions': s_ids,
        'summary': 'Split %d session(s) (%d matches) matching enhanced.db account "%s" out of "%s" (%s)' % (
          len(s_ids), info['matches'], ', '.join(split_raw_accs or [acc]), from_label, to_label
        ),
        'evidence': {
          'methods': sorted(list(info['methods'])),
          'servers': sorted(list(info['servers'])),
          'raw_accounts': split_raw_accs,
          'moved_matches': info['matches'],
          'moved_games': info['games'],
          'from_profile': {
            'playerid': str(pid),
            'best_name': from_prof_aliases['best_name'],
            'last_name': pdoc.get('last_name', ''),
            'verified': bool('name' in pdoc),
            'top_names': from_prof_aliases['top_names'][:4],
            'num_matches': int(pdoc.get('num_matches', 0)),
            'num_games': int(pdoc.get('num_games', 0)),
          },
          'session_rows': s_rows,
        },
      })

  # 7c. Profile merges per canonical account (with per-profile AND per-session breakdown)
  for acc, home_pid in sorted(acc_home_pid.iteritems()):
    owned_pids = sorted([pid for pid, oacc in pid_owner_acc.iteritems() if oacc == acc])
    if len(owned_pids) <= 1:
      continue

    mergeable_pids = [home_pid]
    for opid in owned_pids:
      if opid == home_pid:
        continue
      if _pair_key(home_pid, opid) in dnm_pairs or _pair_key(acc, str(opid)) in dnm_pairs:
        continue
      mergeable_pids.append(opid)

    if len(mergeable_pids) <= 1:
      continue

    verified_named = []
    distinct_vnames = set()
    for mpid in mergeable_pids:
      mp = all_players.get(mpid, {})
      if mp.get('name'):
        verified_named.append(mpid)
        distinct_vnames.add(strip_colors(mp['name']).lower())

    has_verified_conflict = (len(verified_named) >= 2 and len(distinct_vnames) >= 2)
    is_auto = (not has_verified_conflict)

    prop_key = '%s:%s' % (acc, ','.join([str(x) for x in sorted(mergeable_pids)]))
    p_summaries = []
    total_m = 0
    methods_union = set()
    servers_union = set()
    raw_accs_union = set()

    # Order profiles with home_pid first, then by num_matches DESC
    ordered_mpids = [home_pid] + sorted(
      [x for x in mergeable_pids if x != home_pid],
      key=lambda x: int(all_players.get(x, {}).get('num_matches', 0)),
      reverse=True
    )

    for mpid in ordered_mpids:
      mp = all_players.get(mpid, {})
      slist = pid_sessions.get(mpid, [])
      m_cnt = int(mp.get('num_matches', 0))
      total_m += m_cnt
      prof_aliases = session_alias_lib.summarize_profile_aliases(slist, alias_map, mp)

      # Identify identifiers of attributed sessions on this profile so we can explain indirect sessions!
      attr_guids = set()
      attr_nmids = set()
      attr_ips = set()
      prof_methods = set()
      prof_servers = set()
      prof_raw_accs = set()
      attr_sess_cnt = 0
      attr_match_cnt = 0

      for s in slist:
        sk = player_lib.canonical_session_key(s['_id'])
        ainfo = session_attr.get(sk)
        if ainfo and ainfo['account'] == acc:
          attr_sess_cnt += 1
          attr_match_cnt += int(s.get('num_matches', 0))
          for sig in ainfo['signals']:
            prof_methods.add(sig['method'])
            methods_union.add(sig['method'])
          prof_servers.update(ainfo['servers'])
          servers_union.update(ainfo['servers'])
          prof_raw_accs.update(ainfo.get('raw_accounts', []))
          raw_accs_union.update(ainfo.get('raw_accounts', []))
          g = int(s['_id'].get('guid', 0))
          if g != 0:
            attr_guids.add(g)
          nm = s['_id'].get('newmod_id')
          if nm:
            attr_nmids.add(nm)
          iph = int(s['_id'].get('ip', 0))
          if is_usable_ip(iph):
            attr_ips.add(iph)

      # Sort sessions on this profile: attributed sessions first (by matches DESC), then indirect sessions (by matches DESC)
      def sess_sort_key(s_doc):
        sk = player_lib.canonical_session_key(s_doc['_id'])
        has_a = 1 if (sk in session_attr and session_attr[sk]['account'] == acc) else 0
        return (has_a, int(s_doc.get('num_matches', 0)), int(s_doc.get('num_games', 0)))

      slist_sorted = sorted(slist, key=sess_sort_key, reverse=True)
      MAX_INLINE_SESS = 60
      s_rows = []
      for s in slist_sorted[:MAX_INLINE_SESS]:
        sk = player_lib.canonical_session_key(s['_id'])
        ainfo = session_attr.get(sk)
        if ainfo and ainfo['account'] == acc:
          s_rows.append(_format_session_row(s, alias_map, attr_info=ainfo, ip_net_map=ip_net_map))
        else:
          g = int(s['_id'].get('guid', 0))
          nm = s['_id'].get('newmod_id')
          iph = int(s['_id'].get('ip', 0))
          if nm and nm in attr_nmids:
            why = u'Co-resides via shared CUID %s...' % nm[:10]
          elif g != 0 and g in attr_guids:
            why = u'Co-resides via shared GUID %d on profile' % g
          elif is_usable_ip(iph) and iph in attr_ips:
            why = u'Co-resides via shared IP %d on profile' % iph
          else:
            why = u'Historical session on existing profile (no direct enhanced.db row)'
          s_rows.append(_format_session_row(s, alias_map, attr_info=None, indirect_reason=why, ip_net_map=ip_net_map))

      fg_list = [s.get('first_game') for s in slist if s.get('first_game')]
      lg_list = [s.get('last_game') for s in slist if s.get('last_game')]

      p_summaries.append({
        'playerid': str(mpid),
        'is_home': bool(mpid == home_pid),
        'name': prof_aliases['best_name'],
        'last_name': mp.get('last_name', ''),
        'top_names': prof_aliases['top_names'][:4],
        'verified': bool('name' in mp),
        'verified_name': mp.get('name'),
        'enhanced_account': mp.get('enhanced_account'),
        'num_matches': m_cnt,
        'num_games': int(mp.get('num_games', 0)),
        'total_sessions': len(slist),
        'attributed_sessions': attr_sess_cnt,
        'attributed_matches': attr_match_cnt,
        'omitted_sessions': max(0, len(slist) - MAX_INLINE_SESS),
        'first_game': min(fg_list) if fg_list else None,
        'last_game': max(lg_list) if lg_list else None,
        'methods': sorted(list(prof_methods)),
        'servers': sorted(list(prof_servers)),
        'raw_accounts': sorted(list(prof_raw_accs)),
        'session_rows': s_rows,
      })

    proposals.append({
      '_id': make_prop_id('merge', prop_key),
      'type': 'conflict' if has_verified_conflict else 'merge',
      'subtype': 'verified_vs_verified_merge' if has_verified_conflict else 'account_merge',
      'tier_auto': is_auto,
      'accounts': [acc],
      'target_account': acc,
      'home_playerid': home_pid,
      'playerids': sorted(mergeable_pids),
      'sessions': [],
      'summary': '%s %d profiles (%d total matches) for enhanced.db account "%s"' % (
        'Conflict: Two Verified profiles matched across' if has_verified_conflict else 'Merge',
        len(mergeable_pids), total_m, acc
      ),
      'evidence': {
        'methods': sorted(list(methods_union)),
        'servers': sorted(list(servers_union)),
        'raw_accounts': sorted(list(raw_accs_union)),
        'profiles': p_summaries,
      },
    })

  # 7d. Account & Verified Name Tagging (with top session evidence so it's clear WHY!)
  for acc, home_pid in sorted(acc_home_pid.iteritems()):
    hp = all_players.get(home_pid, {})
    cur_ea = hp.get('enhanced_account')
    has_name = bool(hp.get('name'))
    if cur_ea != acc or not has_name:
      slist = pid_sessions.get(home_pid, [])
      prof_aliases = session_alias_lib.summarize_profile_aliases(slist, alias_map, hp)
      info = pid_acc_breakdown.get(home_pid, {}).get(acc, {'methods': set(), 'servers': set(), 'sessions': []})
      top_attr_s = sorted(
        info.get('sessions', []),
        key=lambda x: (int(x.get('num_matches', 0)), int(x.get('num_games', 0))),
        reverse=True
      )[:10]
      s_rows = [
        _format_session_row(
          s,
          alias_map,
          attr_info=session_attr.get(player_lib.canonical_session_key(s['_id'])),
          ip_net_map=ip_net_map
        )
        for s in top_attr_s
      ]
      tag_raw_accs = sorted(list({ra for r in s_rows for ra in r.get('raw_accounts', [])}))
      proposals.append({
        '_id': make_prop_id('tag', '%s:%s' % (acc, str(home_pid))),
        'type': 'setaccount',
        'tier_auto': True,
        'accounts': [acc],
        'target_account': acc,
        'playerids': [home_pid],
        'sessions': [],
        'summary': 'Tag profile "%s" (%s) with enhanced.db account "%s"%s' % (
          strip_colors(prof_aliases['best_name']),
          str(home_pid)[:8],
          acc,
          '' if has_name else (' and Verified name "^7%s"' % acc)
        ),
        'evidence': {
          'methods': sorted(list(info.get('methods', []))),
          'servers': sorted(list(info.get('servers', []))),
          'raw_accounts': tag_raw_accs,
          'best_name': prof_aliases['best_name'],
          'top_names': prof_aliases['top_names'][:4],
          'had_verified_name': has_name,
          'num_matches': int(hp.get('num_matches', 0)),
          'session_rows': s_rows,
        },
      })

  # 7e. Uncorroborated IP-only match proposals (review queue)
  for cand in uncorroborated_ip_candidates[:100]:
    s = cand['session']
    pid = s.get('playerid')
    acc = cand['account']
    home_pid = acc_home_pid.get(acc)
    if pid == home_pid:
      continue
    if pid and home_pid and _pair_key(pid, home_pid) in dnm_pairs:
      continue
    pseudo_attr = {
      'account': acc,
      'primary_method': 'ip_only_uncorroborated',
      'servers': cand['servers'],
      'raw_accounts': cand['raw_accounts'],
      'server_accounts': cand.get('server_accounts', []),
      'signals': [{
        'account': acc,
        'method': 'ip_only_uncorroborated',
        'tier': 4,
        'ip': cand['ip'],
        'ip_kinds': cand['ip_kinds'],
        'servers': cand['servers'],
        'raw_accounts': cand['raw_accounts'],
        'server_accounts': cand.get('server_accounts', []),
        'enhanced_nicks': cand['enhanced_nicks'],
      }],
    }
    srow = _format_session_row(s, alias_map, attr_info=pseudo_attr, ip_net_map=ip_net_map)
    proposals.append({
      '_id': make_prop_id('ip_cand', '%s->%s' % (cand['skey'], acc)),
      'type': 'ip_corroboration',
      'tier_auto': False,
      'accounts': [acc],
      'target_account': acc,
      'from_playerid': pid,
      'target_playerid': home_pid,
      'playerids': [p for p in [pid, home_pid] if p is not None],
      'sessions': [s['_id']],
      'summary': 'Uncorroborated IP match (%s): session "%s" (%d matches) -> enhanced.db account "%s"' % (
        ', '.join(cand['servers']),
        strip_colors(srow['primary_name']),
        int(s.get('num_matches', 0)),
        acc
      ),
      'evidence': {
        'methods': ['ip_only_uncorroborated'],
        'servers': cand['servers'],
        'raw_accounts': cand['raw_accounts'],
        'session_rows': [srow],
      },
    })

  return {
    'sync_statuses': sync_statuses,
    'attributed_sessions_count': len(session_attr),
    'shared_nmids_count': len(shared_nmids),
    'shared_guids_count': len(shared_guids),
    'proposals': proposals,
    'session_attr': session_attr,
  }


def sync_proposals_to_db(db, proposals):
  now = datetime.datetime.utcnow()
  existing_resolved = set([
    d['_id'] for d in db.profileLinkProposals.find(
      {'status': {'$in': ['approved', 'rejected', 'auto_applied']}},
      {'_id': 1}
    )
  ])
  active_ids = set()
  for p in proposals:
    pid = p['_id']
    if pid in existing_resolved:
      continue
    active_ids.add(pid)
    doc = dict(p)
    doc['status'] = 'pending'
    doc['updated_at'] = now
    old = db.profileLinkProposals.find_one({'_id': pid}, {'created_at': 1})
    doc['created_at'] = old['created_at'] if (old and 'created_at' in old) else now
    db.profileLinkProposals.replace_one({'_id': pid}, doc, upsert=True)

  stale_q = {'status': 'pending'}
  if active_ids:
    stale_q['_id'] = {'$nin': list(active_ids)}
  db.profileLinkProposals.delete_many(stale_q)


def execute_single_proposal(
  db,
  prop,
  source='proposal_approved',
  recalc_ratings=True,
  selected_sessions=None,
  selected_playerids=None,
  chosen_account=None
):
  ptype = prop.get('type')
  subtype = prop.get('subtype', '')
  acc = prop.get('target_account')

  if ptype == 'conflict' and subtype == 'ambiguous_session' and chosen_account:
    cacc = chosen_account.strip().lower()
    s_ids = prop.get('sessions', [])
    if not s_ids:
      return {'action': 'noop'}
    existing_dest = db.sessionPlayers.find_one({'enhanced_account': cacc})
    target_pid = existing_dest['_id'] if existing_dest else None
    if target_pid is None:
      ref_prop = db.profileLinkProposals.find_one({
        'status': 'pending',
        'target_account': cacc,
        'type': {'$in': ['merge', 'conflict', 'setaccount']}
      })
      if ref_prop:
        cand = ref_prop.get('home_playerid') or (ref_prop.get('playerids') or [None])[0]
        if cand is not None:
          cand_oid = cand if isinstance(cand, ObjectId) else ObjectId(str(cand))
          if db.sessionPlayers.find_one({'_id': cand_oid}) is not None:
            target_pid = cand_oid

    s_doc = db.sessions.find_one({'_id': s_ids[0]})
    cur_pid = s_doc.get('playerid') if s_doc else None
    reason_str = 'Assigned ambiguous session to @%s (%s)' % (cacc, prop.get('summary', ''))

    if target_pid is not None and cur_pid == target_pid:
      player_lib._pin_sessions_in_config(db, s_ids, target_pid, source)
      res = player_lib.set_player_name_or_account(
        target_pid, enhanced_account=cacc, auto_name_if_missing=True, source=source, reason=reason_str, record_audit=True
      )
      return {'action': 'pin_existing', 'result': res}

    if target_pid is None and cur_pid is not None and db.sessions.find({'playerid': cur_pid}).count() == len(s_ids):
      cur_pdoc = db.sessionPlayers.find_one({'_id': cur_pid}) or {}
      if not cur_pdoc.get('enhanced_account') or cur_pdoc.get('enhanced_account') == cacc:
        player_lib._pin_sessions_in_config(db, s_ids, cur_pid, source)
        res = player_lib.set_player_name_or_account(
          cur_pid, enhanced_account=cacc, auto_name_if_missing=True, source=source, reason=reason_str, record_audit=True
        )
        return {'action': 'tag_standalone', 'result': res}

    res = player_lib.split_players(
      s_ids,
      target_playerid=target_pid,
      enhanced_account=cacc,
      source=source,
      reason=reason_str,
      recalc_ratings=recalc_ratings,
      pin_sessions=True,
      record_audit=True
    )
    return {'action': 'split', 'result': res}

  if ptype == 'split' or ptype == 'ip_corroboration':
    all_s_ids = prop.get('sessions', [])
    if selected_sessions is not None:
      sel_keys = set([player_lib.canonical_session_key(x) for x in selected_sessions])
      s_ids = [s for s in all_s_ids if player_lib.canonical_session_key(s) in sel_keys]
      excluded = [s for s in all_s_ids if player_lib.canonical_session_key(s) not in sel_keys]
    else:
      s_ids = all_s_ids
      excluded = []

    if not s_ids:
      return {'action': 'noop'}

    # Pin any sessions the reviewer explicitly unchecked so they don't keep getting re-proposed!
    if excluded and prop.get('from_playerid'):
      player_lib._pin_sessions_in_config(db, excluded, prop['from_playerid'], 'reviewer_unchecked_split')

    target_pid = prop.get('target_playerid')
    if target_pid is not None and db.sessionPlayers.find_one({'_id': target_pid}) is None and acc:
      alt = db.sessionPlayers.find_one({'enhanced_account': acc})
      target_pid = alt['_id'] if alt else None
    res = player_lib.split_players(
      s_ids,
      target_playerid=target_pid,
      enhanced_account=acc,
      source=source,
      reason=prop.get('summary', ''),
      recalc_ratings=recalc_ratings,
      pin_sessions=(source != 'enhanced_auto' and source != 'enhanced_backfill'),
      record_audit=True
    )
    return {'action': 'split', 'result': res}

  if ptype == 'merge' or (ptype == 'conflict' and subtype == 'verified_vs_verified_merge'):
    raw_candidates = prop.get('playerids', [])
    if selected_playerids is not None:
      sel_set = set([str(x) for x in selected_playerids])
      chosen_raw = [x for x in raw_candidates if str(x) in sel_set]
      excluded_raw = [str(x) for x in raw_candidates if str(x) not in sel_set]
    else:
      chosen_raw = raw_candidates
      excluded_raw = []

    # Block excluded profiles from re-proposing against home_playerid
    if excluded_raw and chosen_raw:
      cfg = player_lib._ensure_config_doc(db)
      dnm = cfg.get('do_not_merge_pairs', [])
      anchor_str = str(prop.get('home_playerid') or chosen_raw[0])
      changed_dnm = False
      for ex_id in excluded_raw:
        if ex_id != anchor_str and [anchor_str, ex_id] not in dnm and [ex_id, anchor_str] not in dnm:
          dnm.append([anchor_str, ex_id])
          changed_dnm = True
      if changed_dnm:
        db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'do_not_merge_pairs': dnm}})

    pids = []
    for raw_pid in chosen_raw:
      oid = raw_pid if isinstance(raw_pid, ObjectId) else ObjectId(str(raw_pid))
      if db.sessionPlayers.find_one({'_id': oid}) is not None:
        pids.append(oid)
    if len(pids) >= 1:
      res = player_lib.merge_players(
        pids,
        source=source,
        reason=prop.get('summary', ''),
        enhanced_account=acc,
        recalc_ratings=recalc_ratings,
        pin_sessions=(source == 'proposal_approved'),
        record_audit=True
      )
      return {'action': 'merge', 'result': res}
    return {'action': 'noop'}

  if ptype == 'setaccount':
    oid = prop['playerids'][0]
    oid = oid if isinstance(oid, ObjectId) else ObjectId(str(oid))
    if db.sessionPlayers.find_one({'_id': oid}) is None and acc:
      return {'action': 'noop'}
    res = player_lib.set_player_name_or_account(
      oid,
      enhanced_account=acc,
      auto_name_if_missing=True,
      source=source,
      reason=prop.get('summary', ''),
      record_audit=True
    )
    return {'action': 'setaccount', 'result': res}

  raise ValueError('Proposal type "%s" (%s) requires manual session split/merge selection' % (ptype, subtype))


def run_sync(mode='dryrun'):
  with player_lib.mutation_lock():
    return _run_sync_unlocked(mode=mode)


def _run_sync_unlocked(mode='dryrun'):
  requested_mode = mode
  db = MongoClient("mongodb").demos
  cfg = player_lib._ensure_config_doc(db)
  if mode == 'cron' and not cfg.get('auto_cron_enabled', False):
    mode = 'dryrun'

  plan = build_identity_plan(db)
  proposals = plan['proposals']
  auto_props = [p for p in proposals if p.get('tier_auto')]

  applied_counts = {'split': 0, 'merge': 0, 'setaccount': 0}
  earliest_affected = None
  affected_pids = set()

  if mode in ('backfill', 'cron'):
    source_tag = 'enhanced_backfill' if mode == 'backfill' else 'enhanced_auto'
    ordered = (
      [p for p in auto_props if p['type'] == 'split'] +
      [p for p in auto_props if p['type'] == 'merge'] +
      [p for p in auto_props if p['type'] == 'setaccount']
    )
    audit_start = datetime.datetime.utcnow()
    for p in ordered:
      try:
        execute_single_proposal(db, p, source=source_tag, recalc_ratings=False)
        applied_counts[p['type']] = applied_counts.get(p['type'], 0) + 1
        now = datetime.datetime.utcnow()
        p_doc = dict(p)
        p_doc['status'] = 'auto_applied'
        p_doc['updated_at'] = now
        p_doc['created_at'] = now
        db.profileLinkProposals.replace_one({'_id': p['_id']}, p_doc, upsert=True)
      except Exception:
        print traceback.format_exc()

    post_plan = build_identity_plan(db)
    for p in post_plan['proposals']:
      if p.get('tier_auto'):
        try:
          execute_single_proposal(db, p, source=source_tag, recalc_ratings=False)
          applied_counts[p['type']] = applied_counts.get(p['type'], 0) + 1
          now = datetime.datetime.utcnow()
          p_doc = dict(p)
          p_doc['status'] = 'auto_applied'
          p_doc['updated_at'] = now
          p_doc['created_at'] = now
          db.profileLinkProposals.replace_one({'_id': p['_id']}, p_doc, upsert=True)
        except Exception:
          print traceback.format_exc()

    final_plan = build_identity_plan(db)
    sync_proposals_to_db(db, final_plan['proposals'])

    for aud in db.profileLinkAudit.find({'timestamp': {'$gte': audit_start}}):
      et = aud.get('earliest_match_time')
      if et is not None and (earliest_affected is None or et < earliest_affected):
        earliest_affected = et
      if aud.get('target_playerid'):
        affected_pids.add(aud['target_playerid'])
      for fp in aud.get('from_playerids', []):
        affected_pids.add(fp)

    import calculate_ranks_v4
    rank_res = None
    if mode == 'backfill':
      db.profileLinkConfig.delete_one({'_id': 'pending_rank_rewind'})
      db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'auto_cron_enabled': True, 'backfill_completed_at': datetime.datetime.utcnow()}})
      rank_res = calculate_ranks_v4.trigger_recalculate_since(datetime.datetime(2000, 1, 1), sync_max_matches=0)
    elif earliest_affected is not None:
      rank_res = calculate_ranks_v4.trigger_recalculate_since(earliest_affected, extra_playerids=list(affected_pids))
  else:
    sync_proposals_to_db(db, proposals)
    rank_res = None

  # Auto-flush any deferred manual rating rewind once idle >= 10 minutes during cron runs
  if requested_mode == 'cron':
    import calculate_ranks_v4
    pend = db.profileLinkConfig.find_one({'_id': 'pending_rank_rewind'})
    if pend and pend.get('updated_at') and not calculate_ranks_v4.get_rank_status().get('running'):
      idle_secs = (datetime.datetime.utcnow() - pend['updated_at']).total_seconds()
      if idle_secs >= 600:
        rank_res = calculate_ranks_v4.flush_pending_rank_rewind()

  summary = {
    'mode': mode,
    'ran_at': datetime.datetime.utcnow(),
    'sync_statuses': plan['sync_statuses'],
    'attributed_sessions_count': plan['attributed_sessions_count'],
    'shared_nmids_count': plan['shared_nmids_count'],
    'shared_guids_count': plan['shared_guids_count'],
    'auto_ready_counts': {
      'split': len([p for p in auto_props if p['type'] == 'split']),
      'merge': len([p for p in auto_props if p['type'] == 'merge']),
      'setaccount': len([p for p in auto_props if p['type'] == 'setaccount']),
    },
    'review_queue_count': db.profileLinkProposals.find({'status': 'pending', 'tier_auto': False}).count(),
    'pending_total_count': db.profileLinkProposals.find({'status': 'pending'}).count(),
    'applied_counts': applied_counts,
    'ratings_trigger': rank_res,
  }
  db.profileLinkConfig.replace_one({'_id': 'last_sync_summary'}, dict(summary, _id='last_sync_summary'), upsert=True)
  return summary
