#!/usr/bin/python -u
from contextlib import contextmanager
import datetime
import fcntl
import json
import os
import sys
import time
import traceback
import urlparse

import bson.json_util
import pymongo
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

LOCK_DIR = '/tmp/jactf_locks'
MUTATION_LOCK_PATH = '/tmp/jactf_locks/profile_mutation.lock'
_mutation_lock_depth = 0
_mutation_lock_file = None


def ensure_lock_dir():
  if not os.path.isdir(LOCK_DIR):
    old_umask = os.umask(0)
    try:
      os.makedirs(LOCK_DIR, 0777)
    except OSError:
      pass
    finally:
      os.umask(old_umask)
  try:
    os.chmod(LOCK_DIR, 0777)
  except OSError:
    pass


def open_shared_lock_file(path):
  ensure_lock_dir()
  old_umask = os.umask(0)
  try:
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0666)
  finally:
    os.umask(old_umask)
  try:
    os.chmod(path, 0666)
  except OSError:
    pass
  return os.fdopen(fd, 'r+')


@contextmanager
def mutation_lock():
  global _mutation_lock_depth, _mutation_lock_file
  if _mutation_lock_depth == 0:
    lf = open_shared_lock_file(MUTATION_LOCK_PATH)
    fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
    _mutation_lock_file = lf
  _mutation_lock_depth += 1
  try:
    yield
  finally:
    _mutation_lock_depth -= 1
    if _mutation_lock_depth == 0 and _mutation_lock_file is not None:
      try:
        fcntl.flock(_mutation_lock_file.fileno(), fcntl.LOCK_UN)
      except Exception:
        pass
      try:
        _mutation_lock_file.close()
      except Exception:
        pass
      _mutation_lock_file = None


def canonical_session_key(sess_id):
  return json.dumps(sess_id, sort_keys=True, separators=(',', ':'))


def recompute_player_summary(db, playerid):
  playerdb = db.sessionPlayers
  sessiondb = db.sessions
  sessions = list(sessiondb.find({'playerid': playerid}))
  if len(sessions) == 0:
    playerdb.delete_one({'_id': playerid})
    return None

  num_sessions = len(sessions)
  num_games = sum([int(s.get('num_games', 0)) for s in sessions])
  num_matches = sum([int(s.get('num_matches', 0)) for s in sessions])
  total_time = sum([int(s.get('time', 0)) for s in sessions])
  sessions_sorted = sorted(
    sessions,
    key=lambda s: s.get('last_game') or datetime.datetime(1970, 1, 1),
    reverse=True
  )
  last_name = sessions_sorted[0].get('last_name', u'^7Padawan')

  playerdb.update_one(
    {'_id': playerid},
    {'$set': {
      'num_sessions': num_sessions,
      'num_games': num_games,
      'num_matches': num_matches,
      'time': total_time,
      'last_name': last_name,
    }},
    upsert=True
  )
  return playerdb.find_one({'_id': playerid})


def _earliest_rated_match_time(db, session_ids):
  if not session_ids:
    return None
  cur = db.sessionGames.find(
    {'_id.session': {'$in': session_ids}, 'is_match': True},
    {'time': 1}
  ).sort('time', pymongo.ASCENDING).limit(1)
  for doc in cur:
    return doc.get('time')
  return None


def _ensure_config_doc(db):
  cfg = db.profileLinkConfig.find_one({'_id': 'config'})
  if cfg is None:
    cfg = {
      '_id': 'config',
      'ignored_accounts': [
        'newguy1', 'newguy2', 'newguy3', 'newguy4', 'newguy5', 'newguy6',
        'test', 'ratings'
      ],
      'account_aliases': {
        'fm': 'fullmetal',
        'rdl': 'radle',
      },
      'do_not_merge_pairs': [],
      'pinned_sessions': {},
      'auto_cron_enabled': False,
    }
    db.profileLinkConfig.insert_one(cfg)
  return cfg


def _pin_sessions_in_config(db, session_ids, playerid, source):
  cfg = _ensure_config_doc(db)
  pinned = cfg.get('pinned_sessions', {})
  prev_pinned = {}
  now_iso = datetime.datetime.utcnow().isoformat() + 'Z'
  for sid in session_ids:
    k = canonical_session_key(sid)
    prev_pinned[k] = pinned.get(k)
    pinned[k] = {
      'playerid': str(playerid),
      'locked': True,
      'source': source,
      'updated_at': now_iso,
    }
  db.profileLinkConfig.update_one(
    {'_id': 'config'},
    {'$set': {'pinned_sessions': pinned}}
  )
  return prev_pinned


def _restore_pinned_in_config(db, prev_pinned):
  if not prev_pinned:
    return
  cfg = _ensure_config_doc(db)
  pinned = cfg.get('pinned_sessions', {})
  for k, old_val in prev_pinned.iteritems():
    if old_val is None:
      pinned.pop(k, None)
    else:
      pinned[k] = old_val
  db.profileLinkConfig.update_one(
    {'_id': 'config'},
    {'$set': {'pinned_sessions': pinned}}
  )


def _trigger_ratings_if_needed(earliest_match_time, extra_playerids, recalc_ratings):
  if not recalc_ratings or earliest_match_time is None:
    return None
  import calculate_ranks_v4
  if recalc_ratings == 'defer':
    return calculate_ranks_v4.queue_or_trigger_recalculate(
      earliest_match_time,
      extra_playerids=extra_playerids
    )
  return calculate_ranks_v4.trigger_recalculate_since(
    earliest_match_time,
    extra_playerids=extra_playerids
  )


def merge_players(
  ids,
  source='manual_web',
  reason='',
  enhanced_account=None,
  recalc_ratings=True,
  pin_sessions=None,
  record_audit=True
):
  with mutation_lock():
    return _merge_players_unlocked(
      ids,
      source=source,
      reason=reason,
      enhanced_account=enhanced_account,
      recalc_ratings=recalc_ratings,
      pin_sessions=pin_sessions,
      record_audit=record_audit
    )


def _merge_players_unlocked(
  ids,
  source='manual_web',
  reason='',
  enhanced_account=None,
  recalc_ratings=True,
  pin_sessions=None,
  record_audit=True
):
  db = MongoClient("mongodb").demos
  playerdb = db.sessionPlayers
  sessiondb = db.sessions
  ratingdb = db.playerGameRatings

  # Deduplicate ObjectIds preserving validity
  uniq = []
  for x in ids:
    oid = x if isinstance(x, ObjectId) else ObjectId(str(x))
    if oid not in uniq:
      uniq.append(oid)
  if len(uniq) == 0:
    return None
  if len(uniq) == 1:
    if enhanced_account:
      set_player_name_or_account(
        uniq[0],
        enhanced_account=enhanced_account,
        auto_name_if_missing=True,
        source=source,
        reason=reason,
        record_audit=record_audit
      )
    return recompute_player_summary(db, uniq[0])

  if pin_sessions is None:
    pin_sessions = (source == 'manual_web')

  # Sort oldest first; if only one profile already has a manual Verified name, keep its oldest/verified identity
  uniq.sort()
  before_players = [p for p in playerdb.find({'_id': {'$in': uniq}})]
  by_id = {p['_id']: p for p in before_players}

  # Keep oldest ObjectId (historical convention across demos.jactf.com), carrying over Verified name/account if missing
  keep_id = uniq[0]
  absorb_ids = uniq[1:]

  carried_name = by_id.get(keep_id, {}).get('name')
  carried_acc = enhanced_account or by_id.get(keep_id, {}).get('enhanced_account')
  for aid in absorb_ids:
    ap = by_id.get(aid, {})
    if not carried_name and ap.get('name'):
      carried_name = ap['name']
    if not carried_acc and ap.get('enhanced_account'):
      carried_acc = ap['enhanced_account']

  moved_sessions_docs = list(sessiondb.find({'playerid': {'$in': absorb_ids}}))
  moved_session_ids = [s['_id'] for s in moved_sessions_docs]
  prev_session_pids = [
    {'session': s['_id'], 'playerid': s.get('playerid')}
    for s in moved_sessions_docs
  ]
  earliest_time = _earliest_rated_match_time(db, moved_session_ids)

  prev_pinned = {}
  if pin_sessions and moved_session_ids:
    prev_pinned = _pin_sessions_in_config(db, moved_session_ids, keep_id, source)

  for aid in absorb_ids:
    sessiondb.update_many({'playerid': aid}, {'$set': {'playerid': keep_id}})
    ratings = list(ratingdb.find({'_id.player': aid}))
    ratingdb.delete_many({'_id.player': aid})
    for r in ratings:
      r['_id']['player'] = keep_id
    if len(ratings) > 0:
      try:
        ratingdb.insert_many(ratings, ordered=False)
      except BulkWriteError as bwe:
        pass
    playerdb.delete_one({'_id': aid})

  extra_set = {}
  if carried_acc:
    extra_set['enhanced_account'] = carried_acc.strip().lower()
    if not carried_name:
      carried_name = u'^7' + carried_acc.strip().lower()
  if carried_name:
    extra_set['name'] = carried_name
  if extra_set:
    playerdb.update_one({'_id': keep_id}, {'$set': extra_set}, upsert=True)

  updated = recompute_player_summary(db, keep_id)

  if record_audit:
    db.profileLinkAudit.insert_one({
      'action': 'merge',
      'source': source,
      'reason': reason or ('Merged %d profiles into %s' % (len(uniq), str(keep_id))),
      'target_playerid': keep_id,
      'absorbed_playerids': absorb_ids,
      'moved_sessions': moved_session_ids,
      'previous_session_playerids': prev_session_pids,
      'before_players': before_players,
      'previous_pinned': prev_pinned,
      'earliest_match_time': earliest_time,
      'timestamp': datetime.datetime.utcnow(),
      'undone': False,
    })

  _trigger_ratings_if_needed(earliest_time, uniq, recalc_ratings)
  return updated


def split_players(
  ids,
  target_playerid=None,
  enhanced_account=None,
  source='manual_web',
  reason='',
  recalc_ratings=True,
  pin_sessions=None,
  record_audit=True
):
  with mutation_lock():
    return _split_players_unlocked(
      ids,
      target_playerid=target_playerid,
      enhanced_account=enhanced_account,
      source=source,
      reason=reason,
      recalc_ratings=recalc_ratings,
      pin_sessions=pin_sessions,
      record_audit=record_audit
    )


def _split_players_unlocked(
  ids,
  target_playerid=None,
  enhanced_account=None,
  source='manual_web',
  reason='',
  recalc_ratings=True,
  pin_sessions=None,
  record_audit=True
):
  db = MongoClient("mongodb").demos
  playerdb = db.sessionPlayers
  sessiondb = db.sessions
  ratingdb = db.playerGameRatings
  sessiongamedb = db.sessionGames

  if not ids:
    return []

  if pin_sessions is None:
    pin_sessions = (source == 'manual_web')

  sess_docs = list(sessiondb.find({'_id': {'$in': ids}}))
  if not sess_docs:
    return []

  old_playerids = list({s['playerid'] for s in sess_docs if 'playerid' in s and s['playerid'] is not None})
  before_pids = list(old_playerids)
  if target_playerid is not None:
    target_playerid = target_playerid if isinstance(target_playerid, ObjectId) else ObjectId(str(target_playerid))
    if target_playerid not in before_pids:
      before_pids.append(target_playerid)

  before_players = list(playerdb.find({'_id': {'$in': before_pids}}))
  prev_session_pids = [
    {'session': s['_id'], 'playerid': s.get('playerid')}
    for s in sess_docs
  ]
  moved_session_ids = [s['_id'] for s in sess_docs]
  earliest_time = _earliest_rated_match_time(db, moved_session_ids)

  created_playerid = None
  if target_playerid is None:
    first_game = None
    for s in sorted(sess_docs, key=lambda x: x.get('first_game') or datetime.datetime.utcnow()):
      first_game = s.get('first_game')
      if first_game is not None:
        break
    if first_game is not None:
      playerid = ObjectId(str(ObjectId.from_datetime(first_game))[:12] + str(ObjectId())[12:])
    else:
      playerid = ObjectId()
    init_doc = {'_id': playerid}
    if enhanced_account:
      acc_clean = enhanced_account.strip().lower()
      init_doc['enhanced_account'] = acc_clean
      init_doc['name'] = u'^7' + acc_clean
    playerdb.replace_one({'_id': playerid}, init_doc, upsert=True)
    created_playerid = playerid
  else:
    playerid = target_playerid
    if enhanced_account:
      acc_clean = enhanced_account.strip().lower()
      existing_target = playerdb.find_one({'_id': playerid}) or {'_id': playerid}
      upd = {'enhanced_account': acc_clean}
      if not existing_target.get('name'):
        upd['name'] = u'^7' + acc_clean
      playerdb.update_one({'_id': playerid}, {'$set': upd}, upsert=True)

  sessiondb.update_many({'_id': {'$in': moved_session_ids}}, {'$set': {'playerid': playerid}})

  # Move immediate rating docs so profile reflects approximate rating even before background replay completes
  splitmatches = sessiongamedb.distinct('_id.match', {'_id.session': {'$in': moved_session_ids}})
  if splitmatches and old_playerids:
    for oldpid in old_playerids:
      if oldpid == playerid:
        continue
      # Only move ratings for matches where oldpid has no other remaining session in that match
      remaining_sids = [s['_id'] for s in sessiondb.find({'playerid': oldpid}, {'_id': 1})]
      still_in_match = set()
      if remaining_sids:
        still_in_match = set(sessiongamedb.distinct(
          '_id.match',
          {'_id.session': {'$in': remaining_sids}, '_id.match': {'$in': splitmatches}}
        ))
      movable_matches = [m for m in splitmatches if m not in still_in_match]
      if movable_matches:
        ratings = list(ratingdb.find({'_id.player': oldpid, '_id.match': {'$in': movable_matches}}))
        ratingdb.delete_many({'_id.player': oldpid, '_id.match': {'$in': movable_matches}})
        for r in ratings:
          r['_id']['player'] = playerid
        if ratings:
          try:
            ratingdb.insert_many(ratings, ordered=False)
          except BulkWriteError:
            pass

  prev_pinned = {}
  added_shared_ids = []
  if pin_sessions:
    prev_pinned = _pin_sessions_in_config(db, moved_session_ids, playerid, source)
    # Also record any shared guid/newmod_id between split sessions and remaining sessions on old_playerids
    if old_playerids:
      rem_sessions = list(sessiondb.find({'playerid': {'$in': old_playerids}}, {'_id': 1}))
      rem_guids = {rs['_id'].get('guid') for rs in rem_sessions if rs['_id'].get('guid', 0) > 1000}
      rem_nmids = {rs['_id'].get('newmod_id') for rs in rem_sessions if rs['_id'].get('newmod_id')}
      for msid in moved_session_ids:
        g = msid.get('guid', 0)
        if g > 1000 and g in rem_guids:
          sh_id = 'guid:%d' % g
          if db.profileSharedIdentifiers.find_one({'_id': sh_id, 'manual_split': True}) is None:
            added_shared_ids.append(sh_id)
          db.profileSharedIdentifiers.update_one(
            {'_id': sh_id},
            {'$set': {'type': 'guid', 'value': g, 'manual_split': True}},
            upsert=True
          )
        nm = msid.get('newmod_id')
        if nm and nm in rem_nmids:
          sh_id = 'newmod_id:%s' % nm
          if db.profileSharedIdentifiers.find_one({'_id': sh_id, 'manual_split': True}) is None:
            added_shared_ids.append(sh_id)
          db.profileSharedIdentifiers.update_one(
            {'_id': sh_id},
            {'$set': {'type': 'newmod_id', 'value': nm, 'manual_split': True}},
            upsert=True
          )

  updated_old = []
  for oldpid in old_playerids:
    if oldpid != playerid:
      u = recompute_player_summary(db, oldpid)
      if u is not None:
        updated_old.append(u)
  newplayer = recompute_player_summary(db, playerid)

  if record_audit:
    db.profileLinkAudit.insert_one({
      'action': 'split',
      'source': source,
      'reason': reason or ('Split %d session(s) to %s' % (len(moved_session_ids), str(playerid))),
      'target_playerid': playerid,
      'created_playerid': created_playerid,
      'from_playerids': old_playerids,
      'moved_sessions': moved_session_ids,
      'previous_session_playerids': prev_session_pids,
      'before_players': before_players,
      'previous_pinned': prev_pinned,
      'added_shared_ids': added_shared_ids,
      'earliest_match_time': earliest_time,
      'timestamp': datetime.datetime.utcnow(),
      'undone': False,
    })

  all_affected = list(old_playerids) + [playerid]
  _trigger_ratings_if_needed(earliest_time, all_affected, recalc_ratings)
  return (updated_old[:1] if updated_old else [None]) + [newplayer]


def set_player_name_or_account(
  playerid,
  name=None,
  enhanced_account=None,
  auto_name_if_missing=False,
  source='manual_web',
  reason='',
  record_audit=True
):
  with mutation_lock():
    return _set_player_name_or_account_unlocked(
      playerid,
      name=name,
      enhanced_account=enhanced_account,
      auto_name_if_missing=auto_name_if_missing,
      source=source,
      reason=reason,
      record_audit=record_audit
    )


def _set_player_name_or_account_unlocked(
  playerid,
  name=None,
  enhanced_account=None,
  auto_name_if_missing=False,
  source='manual_web',
  reason='',
  record_audit=True
):
  db = MongoClient("mongodb").demos
  playerdb = db.sessionPlayers
  oid = playerid if isinstance(playerid, ObjectId) else ObjectId(str(playerid))
  before = playerdb.find_one({'_id': oid})
  if before is None:
    return None

  set_fields = {}
  unset_fields = {}

  if name is not None:
    if name == '':
      unset_fields['name'] = 1
    else:
      set_fields['name'] = name

  if enhanced_account is not None:
    acc_clean = enhanced_account.strip().lower()
    if acc_clean == '':
      unset_fields['enhanced_account'] = 1
    else:
      set_fields['enhanced_account'] = acc_clean
      if auto_name_if_missing and not before.get('name') and 'name' not in set_fields:
        set_fields['name'] = u'^7' + acc_clean

  # Check if anything actually changes
  noop = True
  for k, v in set_fields.iteritems():
    if before.get(k) != v:
      noop = False
  for k in unset_fields.keys():
    if k in before:
      noop = False
  if noop:
    return before

  update_doc = {}
  if set_fields:
    update_doc['$set'] = set_fields
  if unset_fields:
    update_doc['$unset'] = unset_fields

  playerdb.update_one({'_id': oid}, update_doc)
  after = playerdb.find_one({'_id': oid})

  if record_audit:
    action_type = 'setaccount' if (enhanced_account is not None and name is None) else 'setname'
    db.profileLinkAudit.insert_one({
      'action': action_type,
      'source': source,
      'reason': reason or ('Updated profile metadata on %s' % str(oid)),
      'target_playerid': oid,
      'moved_sessions': [],
      'previous_session_playerids': [],
      'before_players': [before],
      'previous_pinned': {},
      'earliest_match_time': None,
      'timestamp': datetime.datetime.utcnow(),
      'undone': False,
    })
  return after


def undo_audit_action(audit_id, recalc_ratings=True):
  with mutation_lock():
    return _undo_audit_action_unlocked(audit_id, recalc_ratings=recalc_ratings)


def _undo_audit_action_unlocked(audit_id, recalc_ratings=True):
  db = MongoClient("mongodb").demos
  playerdb = db.sessionPlayers
  sessiondb = db.sessions

  aoid = audit_id if isinstance(audit_id, ObjectId) else ObjectId(str(audit_id))
  entry = db.profileLinkAudit.find_one({'_id': aoid})
  if entry is None:
    raise ValueError('Audit entry not found: %s' % str(aoid))
  if entry.get('undone'):
    raise ValueError('Audit entry %s was already undone' % str(aoid))

  # 1. Restore session -> playerid assignments
  affected_pids = set()
  for item in entry.get('previous_session_playerids', []):
    sid = item['session']
    prev_pid = item.get('playerid')
    cur = sessiondb.find_one({'_id': sid}, {'playerid': 1})
    if cur and cur.get('playerid') is not None:
      affected_pids.add(cur['playerid'])
    if prev_pid is not None:
      sessiondb.update_one({'_id': sid}, {'$set': {'playerid': prev_pid}})
      affected_pids.add(prev_pid)
    else:
      sessiondb.update_one({'_id': sid}, {'$unset': {'playerid': 1}})

  # 2. Restore before_players documents exactly
  for bp in entry.get('before_players', []):
    playerdb.replace_one({'_id': bp['_id']}, bp, upsert=True)
    affected_pids.add(bp['_id'])

  if entry.get('target_playerid') is not None:
    affected_pids.add(entry['target_playerid'])
  if entry.get('created_playerid') is not None:
    affected_pids.add(entry['created_playerid'])

  # 3. Recompute summaries (deletes created_playerid if orphaned, restores exact counts on existing)
  for pid in list(affected_pids):
    if entry.get('action') in ('merge', 'split'):
      res_doc = recompute_player_summary(db, pid)
      if res_doc is None:
        db.playerGameRatings.delete_many({'_id.player': pid})

  # 4. Restore pinned_sessions config and any manual shared identifiers added by split
  _restore_pinned_in_config(db, entry.get('previous_pinned', {}))
  for sh_id in entry.get('added_shared_ids', []):
    db.profileSharedIdentifiers.delete_one({'_id': sh_id, 'manual_split': True})

  db.profileLinkAudit.update_one(
    {'_id': aoid},
    {'$set': {'undone': True, 'undone_at': datetime.datetime.utcnow()}}
  )

  earliest_time = entry.get('earliest_match_time')
  rank_res = _trigger_ratings_if_needed(earliest_time, list(affected_pids), recalc_ratings)
  return {'ok': True, 'undone_id': str(aoid), 'ratings': rank_res}


def reset_playerid(id):
  db = MongoClient("mongodb").demos
  playerdb = db.sessionPlayers
  sessiondb = db.sessions
  ratingdb = db.playerGameRatings
  playergamedb = db.playerGames
  sessionplayerdb = db.sessionPlayers

  first_game = None
  firstSession = sessiondb.find({'playerid': id}).sort('first_game', pymongo.ASCENDING).limit(1)
  playerid = None
  for sess in firstSession:
    first_game = sess['first_game']
    sessionid = sess['_id']
    if 'guid' in sessionid and 'ip' in sessionid:
      oldmatches = set([
        p['_id']['player']
        for p in playergamedb.find(
          {'names': {'$elemMatch': {'guid_hash': sessionid['guid'], 'ip_hash': sessionid['ip']}}},
          {'_id.player'}
        ).hint("names.guid_hash_1_names.ip_hash_1")
      ])
      if len(oldmatches) == 1:
        playerid = next(iter(oldmatches))
        exists = len([p for p in sessionplayerdb.find({'_id': playerid})]) > 0
        if exists:
          playerid = None

  if first_game is None:
    raise Exception('No sessions found for playerid %s' % str(id))
  if playerid is None:
    playerid = ObjectId(str(ObjectId.from_datetime(first_game))[:12] + str(id)[12:])

  sessiondb.update_many({'playerid': id}, {'$set': {'playerid': playerid}})
  ratings = [r for r in ratingdb.find({'_id.player': id})]
  for rating in ratings:
    rating['_id']['player'] = playerid
  ratingdb.delete_many({'_id.player': id})
  if len(ratings) > 0:
    try:
      ratingdb.insert_many(ratings, ordered=False)
    except BulkWriteError as bwe:
      print bwe.details
  player = playerdb.find_one({'_id': id})
  player['_id'] = playerid
  playerdb.delete_one({'_id': id})
  playerdb.replace_one({'_id': playerid}, player, upsert=True)
  return player
