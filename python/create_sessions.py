#!/usr/bin/python
# script to generate user match histories based on ip / guid logs

from os import listdir, stat, utime
from os.path import isfile, isdir, join, exists, dirname, basename
import hashlib
import struct
import re
import sys

from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *
import commands

import datetime

import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
from pymongo.errors import InvalidOperation
import traceback

import merge_metadata
from bson.objectid import ObjectId
import bson
import bson.json_util

import shrinker
import user_lib

KNOWN_CONST_IPS = set([
  0, -1, 750090492, -19591632, 592937230,
  -1912860523, 1964675042, -1344502913, -1123061075
])

def make_session_son(ip_val, guid_val, newmod_id=None):
  items = [('ip', int(ip_val)), ('guid', int(guid_val))]
  if newmod_id is not None:
    items.append(('newmod_id', newmod_id))
  return bson.SON(items)

def getPlayers(names, teams, newmod):
  clients = []
  for clientid, name in names.iteritems():
    client = None
    for name in name:
      if name['name_end_time_raw'] <= name['name_start_time_raw']:
        # skip 0 length nodes, seems it can sometimes happen before newmod_id is set?
        continue
      if client == None or client['end'] < name['name_start_time_raw']:
        if client != None:
          clients.append(client)
        client = {'clientid': int(clientid),
                  'start': name['name_start_time_raw'],
                  'end': name['name_end_time_raw'],
                  'ip': name.get('ip_hash', 0),
                  'guid': name.get('guid_hash', 0)}
      else:
        client['end'] = name['name_end_time_raw']
        client['ip'] = name.get('ip_hash', 0)
        client['guid'] = name.get('guid_hash', 0)
    if client != None:
      clients.append(client)
  for client in clients:
    client['teams'] = []
    team = teams['%d' % client['clientid']]
    for team in team:
      if team['team_start_time_raw'] >= client['start'] and team['team_end_time_raw'] <= client['end']:
        # also include non-raw times which account for pausing the clock
        client['teams'].append({'team': team['team'], 'start': team['team_start_time_raw'], 'end': team['team_end_time_raw'], 'gamestart': team['team_start_time'], 'gameend': team['team_end_time']})

    client['names'] = []
    name = names['%d' % client['clientid']]
    for name in name:
      if name['name_start_time_raw'] >= client['start'] and name['name_end_time_raw'] <= client['end']:
        # TODO: add color-stripped copy of name
        client['names'].append({'name': name['name'], 'start': name['name_start_time_raw'], 'end': name['name_end_time_raw']})

    nm = newmod.get('%d' % client['clientid'], [])
    for nm in nm:
      if nm['newmod_start_time_raw'] >= client['start'] and nm['newmod_end_time_raw'] <= client['end']:
        client['newmod_id'] = nm['newmod_id']
  return clients

def canonical_skey(sid):
  d = {'ip': int(sid['ip']), 'guid': int(sid.get('guid', 0))}
  if 'newmod_id' in sid and sid['newmod_id'] is not None:
    d['newmod_id'] = sid['newmod_id']
  return bson.json_util.dumps(d, sort_keys=True, separators=(',', ':'))

def to_session_son(sid):
  return make_session_son(sid['ip'], sid.get('guid', 0), sid.get('newmod_id'))

def getScore(match):
  scores = None
  fallback = None
  for sc in match.get('sc', []):
    if sc.get('bs') is not None and sc.get('rs') is not None:
      fallback = sc
    scores = sc
    if sc.get('fi', 0) != 0:
      break
  chosen = scores if (scores and scores.get('bs') is not None and scores.get('rs') is not None) else (fallback or scores or {})
  return {'blue': chosen.get('bs'), 'red': chosen.get('rs')}

if __name__ == '__main__':
  db = MongoClient("mongodb").demos
  #demodb = db.demos
  mindemodb = db.mindemos
  matchdb = db.minmatches
  ipdb = db.minips
  playerdb = db.players2
  playergamedb = db.playerGames
  sessiongamedb = db.sessionGames
  sessiondb = db.sessions
  sessionplayerdb = db.sessionPlayers
  basedir = u'/cygdrive/U/demos/'

  #start = parse(commands.getoutput("/bin/date") + ' -0800') + relativedelta(hours=-12)
  #start = datetime.datetime(2020, 4, 23, 12, 0, 0)
  start = datetime.datetime(2016, 8, 15)

  clean_start = False
  pending_ids = []
  if len(sys.argv) > 1:
    matches = matchdb.find({'_id': {'$in': sys.argv[1:]}}).sort('t', pymongo.ASCENDING).skip(0).batch_size(30)
  else:
    for sessiongame in sessiongamedb.find().sort('time', pymongo.DESCENDING).limit(1):
      start = sessiongame['time']
      start += datetime.timedelta(hours=-3)
      print 'Start:', start
    pending_doc = db.profileLinkConfig.find_one({'_id': 'pending_session_matches'})
    if pending_doc and pending_doc.get('matches'):
      pending_ids = list(pending_doc['matches'])
      print 'Including %d pending_session_matches from populate_db_lite' % len(pending_ids)
      matches = matchdb.find({'$or': [{'t': {'$gt': start}}, {'_id': {'$in': pending_ids}}]}).sort('t', pymongo.ASCENDING).skip(0).batch_size(30)
    else:
      matches = matchdb.find({'t': {'$gt': start}}).sort('t', pymongo.ASCENDING).skip(0).batch_size(30)

  import json
  num_matches = 0
  chunksize = 50
  bulk = sessiongamedb.initialize_unordered_bulk_op()
  sessbulk = sessiondb.initialize_unordered_bulk_op()
  sessplayerbulk = sessionplayerdb.initialize_unordered_bulk_op()
  session_player_map = {}
  processed_match_ids = []
  processed_pug_ids = []
  candidate_orphan_skeys = set()

  for match in matches:
    print num_matches, match['_id'], match['t']
    num_matches += 1
    processed_match_ids.append(match['_id'])
    if match.get('ma') == True:
      processed_pug_ids.append(match['_id'])
    # first, check for any ip/guid data recorded in the demo metadata
    demos = []
    demonames = []
    for matchdemo in match['d']:
      demodatas = mindemodb.find({'_id': matchdemo['id']}, shrinker.minimize_proj({
        'metadata.client': 1,
        'metadata.version': 1,
        'metadata.maps.names': 1,
        'metadata.maps.teams': 1,
        'metadata.maps.newmod': 1,
        'metadata.maps.bookmarks': 1,
        'metadata.maps.match_hash': 1,
        'metadata.maps.map_start_time': 1,
        'metadata.maps.map_end_time': 1}))
      demo = None
      for demo in demodatas:
        demo = shrinker.inflate(demo)
        break
      if demo == None:
        print 'Unknown demo', matchdemo['id']
        continue
      demos.append(demo['metadata'])
      demonames.append(matchdemo['id'])
    olddemos = [idx for idx, d in enumerate(demos) if d['version'] < 5]
    if len(olddemos) > 0:
      print 'demos with wrong version!'
      print "\n".join([demonames[idx] for idx in olddemos])
    names = merge_metadata.merge_history(demos, match['_id'], 'name')
    teams = merge_metadata.merge_history(demos, match['_id'], 'team')
    newmod = merge_metadata.merge_history(demos, match['_id'], 'newmod')

    players = getPlayers(names, teams, newmod)
    for player in players:
      player['demos'] = []
      for idx, demo in enumerate(demos):
        if demo['client']['id'] == player['clientid']:
          _, map = merge_metadata.findmap(demo['maps'], match['_id'])
          name = map['names'].get('%d' % player['clientid'], None)
          if name == None:
            continue
          if name[0]['name_start_time_raw'] >= player['start'] and name[-1]['name_end_time_raw'] <= player['end']:
            demoidx = len(player['demos'])
            player['demos'].append(demonames[idx])
            if 'bookmarks' in map:
              if 'bookmarks' not in player:
                player['bookmarks'] = []
              player['bookmarks'].extend([{'d': demoidx, 'time': bookmark['time'], 'time_raw': bookmark['time_raw'], 'mark': bookmark.get('mark', '?')} for bookmark in map['bookmarks']])

    # Find previously saved sessionGames for this match and preserve any backfilled real IPs & Nice Shots
    prev_docs = [] if clean_start else list(sessiongamedb.find({'_id.match': match['_id']}))
    prevsessiongames = {canonical_skey(game['_id']['session']): game for game in prev_docs}
    prev_real_ip_by_cid_guid = {}
    prev_real_ip_by_nm = {}
    prev_ns_by_cid_demos = {}
    for pdoc in prev_docs:
      p_ip = int(pdoc['_id']['session'].get('ip', 0))
      for pg in pdoc.get('games', []):
        if p_ip not in KNOWN_CONST_IPS:
          prev_real_ip_by_cid_guid[(pg.get('clientid'), int(pg.get('guid', 0)))] = p_ip
          if pg.get('newmod_id'):
            prev_real_ip_by_nm[pg['newmod_id']] = p_ip
        if 'ns' in pg and pg.get('demos'):
          prev_ns_by_cid_demos[(pg.get('clientid'), tuple(pg['demos']))] = pg['ns']

    for player in players:
      if int(player.get('ip', 0)) in KNOWN_CONST_IPS:
        rec_ip = prev_real_ip_by_cid_guid.get((player.get('clientid'), int(player.get('guid', 0))))
        if rec_ip is None and player.get('newmod_id'):
          rec_ip = prev_real_ip_by_nm.get(player['newmod_id'])
        if rec_ip is not None:
          player['ip'] = rec_ip
      if player.get('demos'):
        old_ns = prev_ns_by_cid_demos.get((player.get('clientid'), tuple(player['demos'])))
        if old_ns is not None:
          player['ns'] = old_ns

    sessions = {}
    session_sons = {}
    for player in players:
      sessionid = make_session_son(player['ip'], player['guid'], player.get('newmod_id'))
      if sessionid['ip'] == 0 and sessionid['guid'] == 0 and sessionid.get('newmod_id', '') == '':
        continue
      key = canonical_skey(sessionid)
      session_sons[key] = sessionid
      cur = sessions.get(key, [])
      sessions[key] = cur + [player]

    lookup_keys = [k for k in (set(sessions.keys()) | set(prevsessiongames.keys())) if k not in session_player_map]
    if lookup_keys:
      lookup_sons = [to_session_son(json.loads(k)) for k in lookup_keys]
      for session in sessiondb.find({'_id': {'$in': lookup_sons}}):
        session_player_map[canonical_skey(session['_id'])] = session.get('playerid', None)
      for k in lookup_keys:
        if k not in session_player_map:
          session_player_map[k] = None

    score_obj = getScore(match)
    map_name = match.get('n')
    for key, session in sessions.iteritems():
      inc_match = 0
      last_name = ''
      total_time = 0
      for game in session:
        for team in game['teams']:
          total_time += team['end'] - team['start']
          if match.get('ma') == True:
            if team['team'] == 'RED' or team['team'] == 'BLUE':
              inc_match = 1
        last_name = game['names'][-1]['name']
      sessionid = session_sons[key]
      sg_id = bson.SON([('session', sessionid), ('match', match['_id'])])
      sg_doc = {
        'games': session,
        'time': match['t'],
        'is_match': inc_match == 1,
        'map': map_name,
        'score': score_obj,
        'dur': total_time,
      }
      bulk.find({'_id': sg_id}).upsert().replace_one(sg_doc)
      sessbulk.find({'_id': sessionid}).upsert().update_one({
        '$max': {'last_game': match['t']},
        '$min': {'first_game': match['t']},
        '$inc': {'num_games': 1, 'num_matches': inc_match, 'time': total_time},
        '$set': {'last_name': last_name}
      })
      playerid = session_player_map.get(key)
      if playerid is not None:
        sessplayerbulk.find({'_id': playerid}).update_one({
          '$inc': {'num_games': 1, 'num_matches': inc_match, 'time': total_time},
          '$set': {'last_name': last_name}
        })
      prev = prevsessiongames.pop(key, None)
      if prev is not None:
        prev_inc = 1 if prev.get('is_match') else 0
        prev_dur = prev.get('dur', 0)
        sessbulk.find({'_id': sessionid}).update_one({'$inc': {'num_games': -1, 'num_matches': -prev_inc, 'time': -prev_dur}})
        if playerid is not None:
          sessplayerbulk.find({'_id': playerid}).update_one({'$inc': {'num_games': -1, 'num_matches': -prev_inc, 'time': -prev_dur}})

    for rem_key, prev in prevsessiongames.iteritems():
      bulk.find({'_id': prev['_id']}).remove_one()
      rem_sid = to_session_son(prev['_id']['session'])
      prev_inc = 1 if prev.get('is_match') else 0
      prev_dur = prev.get('dur', 0)
      sessbulk.find({'_id': rem_sid}).update_one({'$inc': {'num_games': -1, 'num_matches': -prev_inc, 'time': -prev_dur}})
      rem_pid = session_player_map.get(rem_key)
      if rem_pid is not None:
        sessplayerbulk.find({'_id': rem_pid}).update_one({'$inc': {'num_games': -1, 'num_matches': -prev_inc, 'time': -prev_dur}})
      candidate_orphan_skeys.add(rem_key)

    print json.dumps(sessions.keys(), sort_keys=True, indent=2, separators=(',', ': '))
    if (num_matches % chunksize) == 0:
      try:
        print bulk.execute()
      except BulkWriteError as bwe:
        print bwe.details
      except InvalidOperation as ivo:
        print ivo
      try:
        print sessbulk.execute()
      except BulkWriteError as bwe:
        print bwe.details
      except InvalidOperation as ivo:
        print ivo
      try:
        print sessplayerbulk.execute()
      except BulkWriteError as bwe:
        print bwe.details
      except InvalidOperation as ivo:
        print ivo
      bulk = sessiongamedb.initialize_unordered_bulk_op()
      sessbulk = sessiondb.initialize_unordered_bulk_op()
      sessplayerbulk = sessionplayerdb.initialize_unordered_bulk_op()

  try:
    print bulk.execute()
  except BulkWriteError as bwe:
    print bwe.details
  except InvalidOperation as ivo:
    print ivo
  try:
    print sessbulk.execute()
  except BulkWriteError as bwe:
    print bwe.details
  except InvalidOperation as ivo:
    print ivo

  # Clean any session entries orphaned by removed sessionGames
  if candidate_orphan_skeys:
    orphan_bulk = sessiondb.initialize_unordered_bulk_op()
    has_orphan = False
    for okey in candidate_orphan_skeys:
      oson = to_session_son(json.loads(okey))
      if sessiongamedb.find({'_id.session': oson}).limit(1).count() == 0:
        orphan_bulk.find({'_id': oson}).remove_one()
        has_orphan = True
        print 'Removed orphaned session:', oson
    if has_orphan:
      try:
        print orphan_bulk.execute()
      except BulkWriteError as bwe:
        print bwe.details
      except InvalidOperation as ivo:
        print ivo

  try:
    print sessplayerbulk.execute()
  except BulkWriteError as bwe:
    print bwe.details
  except InvalidOperation as ivo:
    print ivo

  if pending_ids and processed_match_ids:
    db.profileLinkConfig.update_one(
      {'_id': 'pending_session_matches'},
      {'$pullAll': {'matches': processed_match_ids}}
    )
  if len(sys.argv) <= 1 and processed_pug_ids:
    db.profileLinkConfig.update_one(
      {'_id': 'pending_rank_matches'},
      {'$addToSet': {'matches': {'$each': processed_pug_ids}}},
      upsert=True
    )
