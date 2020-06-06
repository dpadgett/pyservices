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

import shrinker
import user_lib

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

def getScore(match):
  scores = None
  for sc in match['sc']:
    scores = sc
    if sc['fi'] != 0:
      break
  return {'blue': scores.get('bs'), 'red': scores.get('rs')}

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
  if len(sys.argv) > 1:
    matches = matchdb.find({'_id': {'$in': sys.argv[1:]}}).sort('t', pymongo.ASCENDING).skip(0).batch_size(30)
  else:
    #matches = matchdb.find({'t': {'$gt': start}, 'ma': True}).sort('t', pymongo.ASCENDING).skip(0).batch_size(30)
    '''
    matches = matchdb.find({'t': {'$gt': start}}).sort('t', pymongo.ASCENDING).skip(0).batch_size(30)
    sessiongamedb.drop()
    sessiondb.drop()
    sessiongamedb.create_index([('_id.match',1)])
    sessiongamedb.create_index([('_id.session',1),('time':-1)])
    sessiondb.create_index([('_id.newmod_id',1),('last_game',-1)])
    sessiondb.create_index([('_id.guid',1),('last_game',-1)])
    sessiondb.create_index([('playerid',1),('last_game',-1)])
    clean_start = True
    '''
    for sessiongame in sessiongamedb.find().sort('time', pymongo.DESCENDING).limit(1):
      start = sessiongame['time']
      print 'Start:', start
    matches = matchdb.find({'t': {'$gt': start}}).sort('t', pymongo.ASCENDING).skip(0).batch_size(30)
    

  num_matches = 0
  chunksize = 50
  bulk = sessiongamedb.initialize_unordered_bulk_op()
  sessbulk = sessiondb.initialize_unordered_bulk_op()
  sessplayerbulk = sessionplayerdb.initialize_unordered_bulk_op()
  session_player_map = {}
  for match in matches:
    print num_matches, match['_id'], match['t']
    num_matches += 1
    # first, check for any ip/guid data recorded in the demo metadata
    demos = []
    demonames = []
    for matchdemo in match['d']:
      ips = []
      guids = []
      demodatas = mindemodb.find({'_id': matchdemo['id']}, shrinker.minimize_proj({
        'metadata.client': 1,
        'metadata.version': 1,
        'metadata.maps.names': 1,
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
    olddemos = [idx for idx, d in enumerate(demos) if d['version'] != 5]
    if len(olddemos) > 0:
      print 'demos with wrong version!'
      print "\n".join([demonames[idx] for idx in olddemos])
    names = merge_metadata.merge_history(demos, match['_id'], 'name')
    import json
    #print json.dumps(names, indent=2)
    #print json.dumps(teams, indent=2)
    #print json.dumps(demonames, indent=2)
    start = 'name_start_time_raw'
    end = 'name_end_time_raw'

    import json
    #print json.dumps(demos, sort_keys=True, indent=2, separators=(',', ': '))
    #print json.dumps(names, sort_keys=True, indent=2, separators=(',', ': '))
    #print json.dumps(getPlayers(names), sort_keys=True, indent=2, separators=(',', ': '))
    
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
            player['demos'].append(demonames[idx])
    #print json.dumps(players, sort_keys=True, indent=2, separators=(',', ': '))

    sessions = {}
    for player in players:
      sessionid = {'ip': player['ip'], 'guid': player['guid']}
      if 'newmod_id' in player:
        sessionid['newmod_id'] = player['newmod_id']
      if sessionid['ip'] == 0 and sessionid['guid'] == 0 and sessionid.get('newmod_id', '') == '':
        continue
      key = json.dumps(sessionid)
      cur = sessions.get(key, [])
      sessions[key] = cur + [player]
      #sessions.append({'ip': player['ip'], 'guid': player['guid']})


    # find any previously saved sessions for updating.
    prevsessiongames = {} if clean_start else {json.dumps(game['_id']['session']): game for game in sessiongamedb.find({'_id.match': match['_id']})}
    for session in sessiondb.find({'_id': {'$in': [json.loads(key) for key in sessions.keys() if key not in session_player_map]}}):
      session_player_map[json.dumps(session['_id'])] = session.get('playerid', None)
    # if not found, it doesn't exist, so playerid is None.
    for key in sessions.keys():
      if key not in session_player_map:
        session_player_map[key] = None
        
    for key, session in sessions.iteritems():
      inc_match = 0
      last_name = ''
      total_time = 0
      for game in session:
        for team in game['teams']:
          total_time += team['end'] - team['start']
          if match['ma'] == True:
            if team['team'] == 'RED' or team['team'] == 'BLUE':
              inc_match = 1
        last_name = game['names'][-1]['name']
      sessionid = json.loads(key)
      bulk.find({'_id': {'session': sessionid, 'match': match['_id']}}).upsert().replace_one({'games': session, 'time': match['t'], 'is_match': inc_match == 1, 'score': getScore(match), 'dur': total_time})
      sessbulk.find({'_id': sessionid}).upsert().update_one({'$max': {'last_game': match['t']}, '$min': {'first_game': match['t']}, '$inc': {'num_games': 1, 'num_matches': inc_match, 'time': total_time}, '$set': {'last_name': last_name}})
      playerid = session_player_map[key]
      if playerid is not None:
        sessplayerbulk.find({'_id': playerid}).update_one({'$inc': {'num_games': 1, 'num_matches': inc_match, 'time': total_time}, '$set': {'last_name': last_name}})
      prev = prevsessiongames.pop(key, None)
      if prev is not None:
        inc_match = 1 if prev['is_match'] else 0
        total_time = prev['dur']
        sessbulk.find({'_id': sessionid}).update_one({'$inc': {'num_games': -1, 'num_matches': -inc_match, 'time': -total_time}})
        if playerid is not None:
          sessplayerbulk.find({'_id': playerid}).update_one({'$inc': {'num_games': -1, 'num_matches': -inc_match, 'time': -total_time}})
    for prev in prevsessiongames.values():
      bulk.find({'_id': prev['_id']}).remove_one()
    #print json.dumps(sessions, sort_keys=True, indent=2, separators=(',', ': '))
    print json.dumps(sessions.keys(), sort_keys=True, indent=2, separators=(',', ': '))
    if (num_matches % chunksize) == 0:
      #exit()
      try:
        print bulk.execute()
      except BulkWriteError as bwe:
        print bwe.details
      try:
        print sessbulk.execute()
      except BulkWriteError as bwe:
        print bwe.details
      try:
        print sessplayerbulk.execute()
      except BulkWriteError as bwe:
        print bwe.details
      except InvalidOperation as ivo:
        print ivo
      bulk = sessiongamedb.initialize_unordered_bulk_op()
      sessbulk = sessiondb.initialize_unordered_bulk_op()
      sessplayerbulk = sessionplayerdb.initialize_unordered_bulk_op()
    #break
  try:
    print bulk.execute()
  except BulkWriteError as bwe:
    print bwe.details
  try:
    print sessbulk.execute()
  except BulkWriteError as bwe:
    print bwe.details
  try:
    print sessplayerbulk.execute()
  except BulkWriteError as bwe:
    print bwe.details
  except InvalidOperation as ivo:
    print ivo
