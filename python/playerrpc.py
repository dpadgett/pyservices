#!/usr/bin/python -u
import datetime
import json
import os
import re
import sys
import time
import traceback
import urlparse

import bson
import bson.json_util
import pymongo
from bson.objectid import ObjectId
from pymongo import MongoClient

import calculate_ranks_v4
import enhanced_profile_lib
import network_disambig_lib
import player_lib
import session_alias_lib


def run(script, args):
  cwd = '/home/pyservices/'
  if script[0:1] != '/':
    script = cwd + script
  origargv = sys.argv
  sys.argv = [script]
  sys.argv.extend(args)
  d = dict(locals(), **globals())
  try:
    execfile(script, d, d)
  except Exception:
    print traceback.format_exc()
  sys.argv = origargv


raw_body = sys.stdin.read()

print 'Content-type: text/plain'
print 'Status: 200 OK'
print ''


def _cgi_excepthook(exc_type, exc_val, exc_tb):
  if issubclass(exc_type, (SystemExit, KeyboardInterrupt)):
    return
  traceback.print_exception(exc_type, exc_val, exc_tb, None, sys.stderr)
  try:
    print json.dumps({'error': '%s: %s' % (exc_type.__name__, str(exc_val))})
  except Exception:
    pass


sys.excepthook = _cgi_excepthook

args = urlparse.parse_qs(urlparse.urlparse(os.environ.get('REQUEST_URI', '')).query)

parsed_body = None
rpc = None
if 'rpc' in args:
  rpc = args['rpc'][0]
if raw_body != '':
  try:
    parsed_body = bson.json_util.loads(raw_body)
    if rpc is None and isinstance(parsed_body, dict) and 'rpc' in parsed_body:
      rpc = parsed_body['rpc']
  except Exception:
    parsed_body = None

if rpc is None:
  print 'Missing RPC'
  exit()

db = MongoClient("mongodb").demos


_EPOCH_NAIVE = datetime.datetime(1970, 1, 1)


def _fast_bson_default(o):
  if isinstance(o, ObjectId):
    return {'$oid': str(o)}
  if isinstance(o, datetime.datetime):
    if o.tzinfo is not None:
      o = o.astimezone(bson.tz_util.utc).replace(tzinfo=None)
    return {'$date': int(round((o - _EPOCH_NAIVE).total_seconds() * 1000))}
  return bson.json_util.default(o)


def fast_bson_dumps(obj):
  return json.dumps(obj, default=_fast_bson_default)


def wrap_result(result):
  if args.get('jsonp', None) == ['true']:
    return args['rpc'][0] + ' = ' + fast_bson_dumps(result) + ';'
  else:
    return fast_bson_dumps(result)


def _admin_live_meta():
  return {
    'counts': {
      'pending_review': db.profileLinkProposals.find({'status': 'pending', 'tier_auto': False}).count(),
      'pending_auto': db.profileLinkProposals.find({'status': 'pending', 'tier_auto': True}).count(),
      'approved': db.profileLinkProposals.find({'status': 'approved'}).count(),
      'rejected': db.profileLinkProposals.find({'status': 'rejected'}).count(),
      'auto_applied': db.profileLinkProposals.find({'status': 'auto_applied'}).count(),
      'shared_identifiers': db.profileSharedIdentifiers.count(),
    },
    'rank_status': calculate_ranks_v4.get_rank_status(),
    'pending_rank_rewind': db.profileLinkConfig.find_one({'_id': 'pending_rank_rewind'}),
  }


def checkmerge():
  ip = os.environ.get('HTTP_X_REAL_IP', os.environ.get('REMOTE_ADDR', ''))
  allowed = ip in ['135.180.72.208', '192.168.86.1']
  return allowed


def require_admin():
  if not checkmerge():
    print wrap_result({'error': 'Not allowed'})
    exit()


def strip_colors(string):
  ''' Returns the string with color codes stripped'''
  if not string:
    return ''
  idx = 0
  result = ''
  while idx < len(string):
    if string[idx] == '^' and idx < len(string) - 1 and ord(string[idx + 1]) >= ord('0') and ord(string[idx + 1]) <= ord('9'):
      idx += 2
      continue
    result += string[idx]
    idx += 1
  return result


def search_players(searchplayers):
  playerdb = db.sessionPlayers
  sessiondb = db.sessions
  response = []
  for sessionid in searchplayers:
    playerids = sessiondb.distinct('playerid', {'_id': sessionid, 'playerid': {'$exists': True}})
    players = [p for p in playerdb.find({'_id': {'$in': playerids}, 'rating': {'$exists': True}})]
    result_arr = []
    for p in players:
      if 'name' in p:
        p['name'] += u' \u00ae'
      result_obj = {'name': p.get('name', p['last_name']), 'rating': p['rating']}
      result_arr.append(result_obj)
    response.append({'result': result_arr})
  return response


def _hydrate_proposals(props):
  if not props:
    return props
  pids = set()
  ev_best_by_pid = {}
  for p in props:
    ev = p.get('evidence') or {}
    for pf in ev.get('profiles', []):
      if pf.get('playerid') and pf.get('name'):
        ev_best_by_pid[str(pf['playerid'])] = (pf['name'], pf.get('top_names', [])[:3])
    fpf = ev.get('from_profile')
    if fpf and fpf.get('playerid') and fpf.get('best_name'):
      ev_best_by_pid[str(fpf['playerid'])] = (fpf['best_name'], fpf.get('top_names', [])[:3])
    for pid in p.get('playerids', []):
      if isinstance(pid, ObjectId):
        pids.add(pid)
    if isinstance(p.get('from_playerid'), ObjectId):
      pids.add(p['from_playerid'])
    if isinstance(p.get('target_playerid'), ObjectId):
      pids.add(p['target_playerid'])
  pmap = {}
  if pids:
    pid_list = list(pids)
    pdocs = {pl['_id']: pl for pl in db.sessionPlayers.find({'_id': {'$in': pid_list}})}
    # Only query sessions for unverified profiles not already summarized in proposal evidence
    need_sess_pids = [
      oid for oid, pl in pdocs.iteritems()
      if ('name' not in pl) and (str(oid) not in ev_best_by_pid)
    ]
    sess_by_pid = {}
    alias_map = {}
    if need_sess_pids:
      sub_s = list(db.sessions.find({'playerid': {'$in': need_sess_pids}}))
      alias_map = session_alias_lib.ensure_session_aliases(db, sub_s)
      for s in sub_s:
        sess_by_pid.setdefault(s['playerid'], []).append(s)
    for oid, pl in pdocs.iteritems():
      oid_str = str(oid)
      if 'name' in pl and pl.get('name'):
        best_nm = pl['name']
        top_nms = ev_best_by_pid.get(oid_str, (None, []))[1]
      elif oid_str in ev_best_by_pid:
        best_nm, top_nms = ev_best_by_pid[oid_str]
      else:
        prof_al = session_alias_lib.summarize_profile_aliases(sess_by_pid.get(oid, []), alias_map, pl)
        best_nm = prof_al['best_name']
        top_nms = prof_al['top_names'][:3]
      pmap[oid_str] = {
        'id': oid_str,
        'name': best_nm,
        'last_name': pl.get('last_name', ''),
        'top_names': top_nms,
        'verified': bool('name' in pl),
        'enhanced_account': pl.get('enhanced_account'),
        'num_matches': int(pl.get('num_matches', 0)),
        'num_games': int(pl.get('num_games', 0)),
        'rating': pl.get('rating'),
      }
  for p in props:
    p['hydrated_players'] = [pmap.get(str(pid)) for pid in p.get('playerids', []) if str(pid) in pmap]
    if p.get('from_playerid') and str(p['from_playerid']) in pmap:
      p['hydrated_from'] = pmap[str(p['from_playerid'])]
    if p.get('target_playerid') and str(p['target_playerid']) in pmap:
      p['hydrated_target'] = pmap[str(p['target_playerid'])]
  return props


if rpc == 'teams':
  scriptargs = []
  req = parsed_body if parsed_body is not None else bson.json_util.loads(raw_body)
  for team in ['red', 'blue']:
    scriptargs.append(team[0].upper())
    for player in search_players(req[team]):
      player = player['result']
      if len(player) > 0:
        rating = player[0]['rating']['raw']
        scriptargs.append("%f,%f" % (rating['mu'], rating['sigma']))
      else:
        scriptargs.append('unknown')
  run('teams.py', scriptargs)

elif rpc == 'lookup':
  playerdb = db.sessionPlayers
  sessiondb = db.sessions
  player = {}
  playerid = args['id'][0]
  oid = ObjectId(playerid)
  for player in playerdb.find({'_id': oid}):
    pass
  sessions = [session for session in sessiondb.find({'playerid': oid}).sort('last_game', pymongo.DESCENDING)]
  alias_map = session_alias_lib.ensure_session_aliases(db, sessions)
  is_admin = checkmerge()
  net_map = network_disambig_lib.lookup_cached_net_for_sessions(db, sessions) if is_admin else {}
  cfg_doc = db.profileLinkConfig.find_one({'_id': 'config'}) or {}
  pinned_map = cfg_doc.get('pinned_sessions', {})
  for session in sessions:
    session.pop('playerid', None)
    skey = player_lib.canonical_session_key(session['_id'])
    adoc = alias_map.get(skey)
    if adoc:
      session['primary_name'] = adoc.get('primary_name')
      session['top_names'] = adoc.get('top_names', [])[:4]
    if is_admin:
      iph = int(session['_id'].get('ip', 0))
      if iph in net_map:
        session['net'] = net_map[iph]
    if skey in pinned_map:
      session['pinned'] = True
  player['sessions'] = sessions
  if is_admin:
    pending = list(db.profileLinkProposals.find({'status': 'pending', 'playerids': oid}).limit(20))
    player['pending_proposals'] = _hydrate_proposals(pending)
  print fast_bson_dumps(player)

elif rpc == 'sessiongames':
  sessiondb = db.sessions
  sessiongamedb = db.sessionGames
  raw_sid = json.loads(args['id'][0])
  sid_items = [('ip', int(raw_sid['ip'])), ('guid', int(raw_sid.get('guid', 0)))]
  if 'newmod_id' in raw_sid and raw_sid['newmod_id'] is not None:
    sid_items.append(('newmod_id', raw_sid['newmod_id']))
  sessionid = bson.SON(sid_items)
  limit = int(args.get('limit', ['100'])[0])
  found_sessions = [s for s in sessiondb.find({'_id': sessionid})]
  if not found_sessions:
    print '[]'
    exit()
  session = found_sessions[0]
  query = {'_id.session': sessionid}
  if 'match' in args:
    query['is_match'] = args['match'][0] == 'true'
  games = [game for game in sessiongamedb.find(query).sort('time', pymongo.DESCENDING).limit(limit)]
  match_ids = list({game['_id']['match'] for game in games if game.get('_id') and game['_id'].get('match')})
  ratingdb = db.playerGameRatings
  ratings = {}
  mm_map = {}
  if match_ids:
    if session.get('playerid') is not None:
      ratings = {
        rating['_id']['match']: rating
        for rating in ratingdb.find({
          '_id.player': session.get('playerid'),
          '_id.match': {'$in': match_ids}
        })
      }
    mm_map = {
      m['_id']: m
      for m in db.minmatches.find(
        {'_id': {'$in': match_ids}},
        {'n': 1, 'sc.fi': 1, 'sc.rs': 1, 'sc.bs': 1, 'd.c': 1}
      )
    }
  for game in games:
    mid = game['_id']['match']
    if mid in ratings:
      game['rating'] = ratings[mid]
    mm = mm_map.get(mid)
    if mm:
      if not game.get('map') and mm.get('n'):
        game['map'] = mm['n']
      cur_sc = game.get('score') or {}
      if cur_sc.get('red') is None or cur_sc.get('blue') is None:
        chosen_sc = None
        fb_sc = None
        for sc in mm.get('sc', []):
          if sc.get('bs') is not None and sc.get('rs') is not None:
            fb_sc = sc
          chosen_sc = sc
          if sc.get('fi', 0) != 0:
            break
        use_sc = chosen_sc if (chosen_sc and chosen_sc.get('bs') is not None and chosen_sc.get('rs') is not None) else fb_sc
        if use_sc and use_sc.get('bs') is not None and use_sc.get('rs') is not None:
          game['score'] = {'blue': use_sc.get('bs'), 'red': use_sc.get('rs')}
      game['has_match_demos'] = bool(mm.get('d'))
  print fast_bson_dumps(games)

elif rpc == 'jratings':
  playerid = args['id'][0]
  ratingdb = db.playerGameRatings
  ratings = [rating for rating in ratingdb.find({'_id.player': ObjectId(playerid)})]
  r_mids = list({r['_id']['match'] for r in ratings if r.get('_id') and r['_id'].get('match')})
  if r_mids:
    mm_map = {
      m['_id']: m
      for m in db.minmatches.find(
        {'_id': {'$in': r_mids}},
        {'n': 1, 'sc.fi': 1, 'sc.rs': 1, 'sc.bs': 1}
      )
    }
    for r in ratings:
      mm = mm_map.get(r['_id']['match'])
      if mm:
        if mm.get('n'):
          r['map'] = mm['n']
        chosen_sc = None
        fb_sc = None
        for sc in mm.get('sc', []):
          if sc.get('bs') is not None and sc.get('rs') is not None:
            fb_sc = sc
          chosen_sc = sc
          if sc.get('fi', 0) != 0:
            break
        use_sc = chosen_sc if (chosen_sc and chosen_sc.get('bs') is not None and chosen_sc.get('rs') is not None) else fb_sc
        if use_sc and use_sc.get('bs') is not None and use_sc.get('rs') is not None:
          r['score'] = {'blue': use_sc.get('bs'), 'red': use_sc.get('rs')}
  print fast_bson_dumps(ratings)

elif rpc == 'checkmerge':
  allowed = checkmerge()
  if allowed and args.get('meta') == ['true']:
    pending_review = db.profileLinkProposals.find({'status': 'pending', 'tier_auto': False}).count()
    pending_auto = db.profileLinkProposals.find({'status': 'pending', 'tier_auto': True}).count()
    print wrap_result({'allowed': True, 'pending_review': pending_review, 'pending_auto': pending_auto})
  else:
    print wrap_result(allowed)

elif rpc == 'topplayers' or rpc == 'searchplayers' or rpc == 'newplayers':
  playerdb = db.sessionPlayers
  sessiondb = db.sessions
  sessiongamedb = db.sessionGames
  limit = int(args.get('limit', ['25'])[0])
  offset = int(args.get('offset', ['0'])[0])
  transform = lambda player: player
  if rpc == 'topplayers':
    cursor = playerdb.find().sort('num_matches', pymongo.DESCENDING)
  elif rpc == 'newplayers':
    cursor = playerdb.find({'num_matches': {'$gt': 0}}).sort('_id', pymongo.DESCENDING)
    def updateFirstSeen(player):
      player['first_game'] = player['_id'].generation_time
      firstSession = sessiondb.find({'playerid': player['_id']}).sort('first_game', pymongo.ASCENDING).limit(1)
      for sess in firstSession:
        player['first_game'] = sess['first_game']
        player['first_game_from_session'] = True
      return player
    transform = lambda player: updateFirstSeen(player)
  elif rpc == 'searchplayers':
    q_raw = args.get('name', [''])[0]
    q_clean = strip_colors(q_raw).strip().lower()
    matched_pids = set()
    if q_raw:
      sessions = sessiongamedb.distinct('_id.session', {'games.names.name': q_raw})
      if sessions:
        matched_pids.update(sessiondb.distinct('playerid', {'_id': {'$in': sessions}}))
    if q_clean:
      # Also match canonical enhanced_account and Verified display names (color-insensitive substring)
      for cand in playerdb.find(
        {'$or': [{'enhanced_account': {'$exists': True}}, {'name': {'$exists': True}}]},
        {'_id': 1, 'name': 1, 'enhanced_account': 1}
      ):
        ea = (cand.get('enhanced_account') or '').lower()
        nm = strip_colors(cand.get('name', '')).lower()
        if q_clean in ea or q_clean in nm:
          matched_pids.add(cand['_id'])
      # Also allow direct ObjectId lookup in player search
      if len(q_clean) == 24 and re.match(r'^[0-9a-f]{24}$', q_clean):
        try:
          matched_pids.add(ObjectId(q_clean))
        except Exception:
          pass
    cursor = playerdb.find({'_id': {'$in': [p for p in matched_pids if p is not None]}}).sort('num_matches', pymongo.DESCENDING)
  total = cursor.count()
  players = [transform(player) for player in cursor.skip(offset).limit(limit)]
  print wrap_result({'result': players, 'offset': offset, 'limit': limit, 'total': total})

elif rpc == 'mergeplayers':
  require_admin()
  ids = [bson.ObjectId(id) for id in parsed_body['ids']]
  reason = parsed_body.get('reason', 'Manual merge via Web UI')
  updated = player_lib.merge_players(ids, source='manual_web', reason=reason, recalc_ratings='defer')
  print wrap_result(updated)

elif rpc == 'splitplayers':
  require_admin()
  ids = [
    (bson.json_util.loads(id) if isinstance(id, basestring) else id)
    for id in parsed_body['ids']
  ]
  target_pid = parsed_body.get('target_playerid') or None
  if target_pid:
    target_pid = bson.ObjectId(target_pid)
  reason = parsed_body.get('reason', 'Manual split via Web UI')
  players = player_lib.split_players(
    ids,
    target_playerid=target_pid,
    source='manual_web',
    reason=reason,
    recalc_ratings='defer'
  )
  print wrap_result(players)

elif rpc == 'setname':
  require_admin()
  oid = bson.ObjectId(parsed_body['id'])
  name_val = parsed_body.get('name', None)
  acc_val = parsed_body.get('enhanced_account', None)
  reason = parsed_body.get('reason', 'Manual profile update via Web UI')
  updated = player_lib.set_player_name_or_account(
    oid,
    name=name_val,
    enhanced_account=acc_val,
    source='manual_web',
    reason=reason,
    record_audit=True
  )
  print wrap_result(updated)

elif rpc == 'admin_overview':
  require_admin()
  cfg = player_lib._ensure_config_doc(db)
  last_sync = db.profileLinkConfig.find_one({'_id': 'last_sync_summary'})
  pending_rewind = db.profileLinkConfig.find_one({'_id': 'pending_rank_rewind'})
  review_props = _hydrate_proposals(list(
    db.profileLinkProposals.find({'status': 'pending', 'tier_auto': False}).sort('created_at', pymongo.DESCENDING).limit(200)
  ))
  auto_props = _hydrate_proposals(list(
    db.profileLinkProposals.find({'status': 'pending', 'tier_auto': True}).sort([('type', 1), ('target_account', 1)]).limit(400)
  ))
  audits = list(db.profileLinkAudit.find().sort('timestamp', pymongo.DESCENDING).limit(100))
  # Hydrate player names on audits for cleaner display
  audit_pids = set()
  for a in audits:
    if isinstance(a.get('target_playerid'), ObjectId):
      audit_pids.add(a['target_playerid'])
    for fp in a.get('from_playerids', []):
      if isinstance(fp, ObjectId):
        audit_pids.add(fp)
  audit_pmap = {}
  if audit_pids:
    for pl in db.sessionPlayers.find({'_id': {'$in': list(audit_pids)}}, {'name': 1, 'last_name': 1, 'enhanced_account': 1}):
      audit_pmap[str(pl['_id'])] = pl.get('name') or pl.get('last_name', str(pl['_id']))
  for a in audits:
    if a.get('target_playerid'):
      a['target_player_label'] = audit_pmap.get(str(a['target_playerid']), str(a['target_playerid']))

  print wrap_result({
    'config': cfg,
    'last_sync': last_sync,
    'rank_status': calculate_ranks_v4.get_rank_status(),
    'pending_rank_rewind': pending_rewind,
    'proposals_review': review_props,
    'proposals_auto': auto_props,
    'counts': {
      'pending_review': db.profileLinkProposals.find({'status': 'pending', 'tier_auto': False}).count(),
      'pending_auto': db.profileLinkProposals.find({'status': 'pending', 'tier_auto': True}).count(),
      'approved': db.profileLinkProposals.find({'status': 'approved'}).count(),
      'rejected': db.profileLinkProposals.find({'status': 'rejected'}).count(),
      'auto_applied': db.profileLinkProposals.find({'status': 'auto_applied'}).count(),
      'shared_identifiers': db.profileSharedIdentifiers.count(),
    },
    'audit_log': audits,
  })

elif rpc == 'admin_decide_proposal':
  require_admin()
  with player_lib.mutation_lock():
    prop_id = parsed_body['proposal_id']
    decision = parsed_body['decision']
    prop = db.profileLinkProposals.find_one({'_id': prop_id})
    if prop is None:
      print wrap_result({'error': 'Proposal not found'})
      exit()
    if prop.get('status') != 'pending':
      print wrap_result({'ok': True, 'already_decided': prop.get('status'), 'decided_ids': [prop_id], 'meta': _admin_live_meta()})
      exit()
    now = datetime.datetime.utcnow()
    if decision == 'approve':
      sel_sessions = parsed_body.get('selected_sessions')
      if sel_sessions is not None:
        sel_sessions = [
          (bson.json_util.loads(x) if isinstance(x, basestring) else x)
          for x in sel_sessions
        ]
      sel_pids = parsed_body.get('selected_playerids')
      chosen_acc = parsed_body.get('chosen_account')
      exec_res = enhanced_profile_lib.execute_single_proposal(
        db,
        prop,
        source='proposal_approved',
        recalc_ratings='defer',
        selected_sessions=sel_sessions,
        selected_playerids=sel_pids,
        chosen_account=chosen_acc
      )
      db.profileLinkProposals.update_one(
        {'_id': prop_id},
        {'$set': {'status': 'approved', 'decided_at': now}}
      )
      meta = _admin_live_meta()
      print wrap_result({
        'ok': True,
        'decision': 'approved',
        'decided_ids': [prop_id],
        'result': exec_res,
        'pending_rank_rewind': meta['pending_rank_rewind'],
        'meta': meta,
      })
    elif decision == 'reject':
      cfg = player_lib._ensure_config_doc(db)
      if prop.get('type') in ('merge', 'conflict') and len(prop.get('playerids', [])) >= 2:
        p0, p1 = str(prop['playerids'][0]), str(prop['playerids'][1])
        dnm = cfg.get('do_not_merge_pairs', [])
        if [p0, p1] not in dnm and [p1, p0] not in dnm:
          dnm.append([p0, p1])
          db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'do_not_merge_pairs': dnm}})
      elif prop.get('type') in ('split', 'ip_corroboration') and prop.get('from_playerid') and prop.get('sessions'):
        player_lib._pin_sessions_in_config(
          db, prop['sessions'], prop['from_playerid'], 'rejected_proposal'
        )
      db.profileLinkProposals.update_one(
        {'_id': prop_id},
        {'$set': {'status': 'rejected', 'decided_at': now}}
      )
      meta = _admin_live_meta()
      print wrap_result({'ok': True, 'decision': 'rejected', 'decided_ids': [prop_id], 'meta': meta})
    else:
      print wrap_result({'error': 'Invalid decision'})

elif rpc == 'admin_decide_proposals_batch':
  require_admin()
  with player_lib.mutation_lock():
    raw_items = parsed_body.get('items') if isinstance(parsed_body, dict) else None
    if not isinstance(raw_items, list):
      print wrap_result({'error': 'items must be a list'})
      exit()
    if len(raw_items) == 0:
      print wrap_result({'ok': True, 'approved': 0, 'rejected': 0, 'skipped': 0, 'errors': []})
      exit()

    req_by_id = {}
    for item in raw_items:
      if isinstance(item, dict) and item.get('proposal_id') and item.get('decision') in ('approve', 'reject'):
        req_by_id[item['proposal_id']] = item

    pending_docs = {
      d['_id']: d
      for d in db.profileLinkProposals.find({'_id': {'$in': list(req_by_id.keys())}, 'status': 'pending'})
    }

    def _type_rank(ptype):
      if ptype in ('split', 'ip_corroboration'):
        return 0
      if ptype in ('merge', 'conflict'):
        return 1
      if ptype == 'setaccount':
        return 2
      return 3

    approve_pairs = []
    reject_pairs = []
    skipped_n = 0
    for pid_key, item in req_by_id.iteritems():
      prop = pending_docs.get(pid_key)
      if prop is None:
        skipped_n += 1
        continue
      if item['decision'] == 'approve':
        if prop.get('subtype') == 'ambiguous_session':
          skipped_n += 1
          continue
        approve_pairs.append((prop, item))
      else:
        reject_pairs.append((prop, item))

    approve_pairs.sort(key=lambda pair: _type_rank(pair[0].get('type')))

    audit_start = datetime.datetime.utcnow()
    approved_n = 0
    rejected_n = 0
    decided_ids = []
    errors = []

    for prop, item in approve_pairs:
      try:
        sel_sessions = item.get('selected_sessions')
        if sel_sessions is not None:
          sel_sessions = [
            (bson.json_util.loads(x) if isinstance(x, basestring) else x)
            for x in sel_sessions
          ]
        sel_pids = item.get('selected_playerids')
        enhanced_profile_lib.execute_single_proposal(
          db,
          prop,
          source='proposal_approved',
          recalc_ratings=False,
          selected_sessions=sel_sessions,
          selected_playerids=sel_pids
        )
        db.profileLinkProposals.update_one(
          {'_id': prop['_id']},
          {'$set': {'status': 'approved', 'decided_at': datetime.datetime.utcnow()}}
        )
        approved_n += 1
        decided_ids.append(prop['_id'])
      except Exception as ex:
        errors.append({'proposal_id': prop['_id'], 'error': str(ex)})

    for prop, item in reject_pairs:
      try:
        cfg = player_lib._ensure_config_doc(db)
        if prop.get('type') in ('merge', 'conflict') and len(prop.get('playerids', [])) >= 2:
          p0, p1 = str(prop['playerids'][0]), str(prop['playerids'][1])
          dnm = cfg.get('do_not_merge_pairs', [])
          if [p0, p1] not in dnm and [p1, p0] not in dnm:
            dnm.append([p0, p1])
            db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'do_not_merge_pairs': dnm}})
        elif prop.get('type') in ('split', 'ip_corroboration') and prop.get('from_playerid') and prop.get('sessions'):
          player_lib._pin_sessions_in_config(
            db, prop['sessions'], prop['from_playerid'], 'rejected_proposal'
          )
        db.profileLinkProposals.update_one(
          {'_id': prop['_id']},
          {'$set': {'status': 'rejected', 'decided_at': datetime.datetime.utcnow()}}
        )
        rejected_n += 1
        decided_ids.append(prop['_id'])
      except Exception as ex:
        errors.append({'proposal_id': prop['_id'], 'error': str(ex)})

    if approved_n > 0:
      earliest_time = None
      affected_pids = set()
      for aud in db.profileLinkAudit.find({'timestamp': {'$gte': audit_start}}):
        et = aud.get('earliest_match_time')
        if et is not None and (earliest_time is None or et < earliest_time):
          earliest_time = et
        if aud.get('target_playerid'):
          affected_pids.add(aud['target_playerid'])
        for fp in aud.get('from_playerids', []):
          affected_pids.add(fp)
        for ap in aud.get('absorbed_playerids', []):
          affected_pids.add(ap)
      if earliest_time is not None:
        calculate_ranks_v4.queue_or_trigger_recalculate(
          earliest_time,
          extra_playerids=list(affected_pids)
        )

    meta = _admin_live_meta()
    print wrap_result({
      'ok': True,
      'approved': approved_n,
      'rejected': rejected_n,
      'skipped': skipped_n,
      'decided_ids': decided_ids,
      'errors': errors,
      'pending_rank_rewind': meta['pending_rank_rewind'],
      'meta': meta,
    })

elif rpc == 'admin_flush_ratings':
  require_admin()
  with player_lib.mutation_lock():
    res = calculate_ranks_v4.flush_pending_rank_rewind()
    print wrap_result(res)

elif rpc == 'admin_rank_status':
  require_admin()
  print wrap_result({
    'rank_status': calculate_ranks_v4.get_rank_status(),
    'pending_rank_rewind': db.profileLinkConfig.find_one({'_id': 'pending_rank_rewind'}),
  })

elif rpc == 'admin_update_config':
  require_admin()
  with player_lib.mutation_lock():
    cfg = player_lib._ensure_config_doc(db)
    op = parsed_body.get('op')
    if op == 'set_alias':
      al = (parsed_body.get('alias') or '').strip().lower()
      ca = (parsed_body.get('canonical') or '').strip().lower()
      if al and ca and al != ca:
        amap = cfg.get('account_aliases', {})
        amap[al] = ca
        db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'account_aliases': amap}})
    elif op == 'delete_alias':
      al = (parsed_body.get('alias') or '').strip().lower()
      amap = cfg.get('account_aliases', {})
      amap.pop(al, None)
      db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'account_aliases': amap}})
    elif op == 'add_ignored':
      acc = (parsed_body.get('account') or '').strip().lower()
      ign = cfg.get('ignored_accounts', [])
      if acc and acc not in ign:
        ign.append(acc)
        db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'ignored_accounts': ign}})
    elif op == 'remove_ignored':
      acc = (parsed_body.get('account') or '').strip().lower()
      ign = [x for x in cfg.get('ignored_accounts', []) if x != acc]
      db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'ignored_accounts': ign}})
    elif op == 'add_dnm':
      p0 = str(parsed_body.get('a') or '').strip()
      p1 = str(parsed_body.get('b') or '').strip()
      if p0 and p1 and p0 != p1:
        dnm = cfg.get('do_not_merge_pairs', [])
        if [p0, p1] not in dnm and [p1, p0] not in dnm:
          dnm.append([p0, p1])
          db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'do_not_merge_pairs': dnm}})
    elif op == 'remove_dnm':
      pair = parsed_body.get('pair') or []
      if len(pair) == 2:
        dnm = [x for x in cfg.get('do_not_merge_pairs', []) if set(x) != set([str(pair[0]), str(pair[1])])]
        db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'do_not_merge_pairs': dnm}})
    elif op == 'unpin_session':
      skey = parsed_body.get('session_key')
      pinned = cfg.get('pinned_sessions', {})
      pinned.pop(skey, None)
      db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'pinned_sessions': pinned}})
    elif op == 'set_auto_cron':
      enabled = bool(parsed_body.get('enabled'))
      db.profileLinkConfig.update_one({'_id': 'config'}, {'$set': {'auto_cron_enabled': enabled}})
    print wrap_result({'ok': True, 'config': db.profileLinkConfig.find_one({'_id': 'config'})})

elif rpc == 'admin_run_sync':
  require_admin()
  mode = parsed_body.get('mode', 'dryrun')
  if mode not in ('dryrun', 'backfill', 'cron'):
    print wrap_result({'error': 'Invalid sync mode'})
    exit()
  summary = enhanced_profile_lib.run_sync(mode=mode)
  print wrap_result({'ok': True, 'summary': summary})

elif rpc == 'admin_undo':
  require_admin()
  audit_id = parsed_body['audit_id']
  res = player_lib.undo_audit_action(audit_id, recalc_ratings='defer')
  print wrap_result(res)

elif rpc == 'admin_trace_ip':
  require_admin()
  iph = int(parsed_body.get('ip_hash', 0))
  hdoc = db.ipHashCache.find_one({'_id': iph})
  if not hdoc or not hdoc.get('ip'):
    print wrap_result({'error': 'Cleartext IP not known for this session hash (may be a pepper-constant or outside known ISP CIDRs)'})
    exit()
  ip_str = hdoc['ip']
  trace_res = network_disambig_lib.run_single_ip_trace(ip_str)
  trace_res['ip_source'] = hdoc.get('source', 'enhanced_db')
  s24 = network_disambig_lib.ip_to_subnet24(ip_str)
  if s24:
    db.ipSubnetCache.update_one(
      {'_id': s24},
      {'$set': {
        'traced': True,
        'far_hops': trace_res.get('hops', [])[-3:],
        'pop_sig': trace_res.get('pop_sig', ''),
        'updated_at': datetime.datetime.utcnow(),
      }},
      upsert=True
    )
  print wrap_result(trace_res)

elif rpc == 'searchplayer':
  if raw_body != '':
    searchplayers = json.loads(raw_body)
  else:
    sessionid = {'ip': int(args['ip'][0]), 'guid': int(args['guid'][0])}
    if 'newmod_id' in args:
      sessionid['newmod_id'] = args['newmod_id'][0]
    searchplayers = [sessionid]
  response = search_players(searchplayers)
  print wrap_result(response)

elif rpc == 'ratings':
  playerid = args['id'][0]
  ratingdb = db.playerGameRatings
  ratings = [rating for rating in ratingdb.find({'_id.player': ObjectId(playerid)}).sort([('time', 1)])]
  for rating in ratings:
    print '%s,%s,%s' % (rating['time'], rating['rating']['start']['friendly'], rating['rating']['updated']['friendly'])
