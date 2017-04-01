#!/usr/bin/python

from subprocess import Popen, PIPE
from os import listdir, stat
from os.path import isfile, isdir, join, exists
import locale
import sys
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import json
from dateutil.parser import *
from dateutil.tz import *
from datetime import *
from dateutil.relativedelta import *
from pytz import timezone
import math
from bson.code import Code
from collections import deque
import hashlib
import struct
import traceback

import find_demos
import demometa_lib

# bump version if mongo wrapper format changes (match hash, etc)
mongo_version = 3

#exit(0)

#basedir = u'/cygdrive/U/demos'
#basepaths = [basedir]#[basedir + u'/japlus', basedir + u'/pug']

#directories = [f for f in basepaths]
#tovisit = deque(basepaths)
#while len(tovisit) > 0:
#  dir = tovisit.popleft()
#  childdirs = [ join(dir,f) for f in listdir(dir) if isdir(join(dir,f)) and join(dir,f) != dir ]
#  tovisit.extend(childdirs)
#  directories.extend(childdirs)

#demos = [ join(d,file) for d in directories for file in listdir(d)
#  if not ".dm_meta" in file and (exists(join(d,file + ".dm_meta"))
#    and stat(join(d,file)).st_mtime <= stat(join(d,file + ".dm_meta")).st_mtime) ]

demos = sys.argv[1:]
if len(demos) == 0:
  demos = find_demos.find_demos_all()

def map_is_match( data, map ):
  map_start = map['map_start_time']
  map_end = map['map_end_time']
  map_duration = map_end - map_start
  # compute a hash to identify this match
  match_hash = ''
  if 'version' in data and data['version'] >= 2:
    match_id = ((map['serverId'] & 0xFFFFFFFF) << 32) + (map['checksumFeed'] & 0xFFFFFFFF)
    match_hash = struct.pack('!Q', match_id).encode('hex')
  elif 'scores' in map:
    match_id = {'map': map['mapname'], 'sv_hostname': data['sv_hostname']}
    teams = [x for x in ['red', 'blue', 'free'] if x + 'players' in map['scores']]
    playerscores = [player for x in teams for player in map['scores'][x + 'players']]
    match_id['players'] = [{'score': client['score'], 'name': client['client_name'], 'client': client['client'], 'team': client['team']} for client in playerscores]
    hasher = hashlib.sha512()
    hasher.update(json.dumps(match_id))
    match_hash = hasher.hexdigest()
  # only a pug if map_start is (near) 0.  otherwise, noone was on when map started
  # actual map_restart min start time is 400.  but, server probably lags a few frames to reload everything
  # so say it's ok if map_start is <1s.  this filters out map changes, since clients take longer than .6s to load
  if map_start >= 10000:
    #print 'Not a pug - map_start was ' + str(map_start) + ': ' + directory
    return (False, match_hash)
  if map_duration < 10 * 60 * 1000:
    return (False, match_hash)
  # track team changes
  #( name_history, team_history, frag_history ) = merge_histories( demo_datas )
  name_history = map['names']
  team_history = map['teams']
  frag_history = map['frags']
  starting_teams = {'RED': [], 'BLUE': [], 'FREE': [], 'SPECTATOR': []}
  for (client, teams) in team_history.items():
    if teams[0]['team_start_time'] == map_start:
      starting_teams[teams[0]['team']].append(client)
  if len(starting_teams['RED']) < 2 or len(starting_teams['BLUE']) < 2:
    print 'Few players at start: ', starting_teams
    return (False, match_hash)
  if len(starting_teams['RED']) != len(starting_teams['BLUE']):
    print 'Not equal number of players: ', starting_teams
    return (False, match_hash)
  ending_teams = {'RED': [], 'BLUE': [], 'FREE': [], 'SPECTATOR': []}
  # still count players that quit less than a minute before map end time ("RQ")
  ending_team_time = map_end - 60 * 1000
  for (client, teams) in team_history.items():
    if teams[-1]['team_start_time'] <= ending_team_time and teams[-1]['team_end_time'] >= ending_team_time:
      ending_teams[teams[-1]['team']].append(client)
  for team in ['RED', 'BLUE']:
    if len(starting_teams[team]) != len(ending_teams[team]):
      print 'Too many left ' + team + ': ', starting_teams[team], ending_teams[team]
      return (False, match_hash)
  # check that not many players were replaced
  ending_players = [client for players in [ending_teams['RED'], ending_teams['BLUE']] for client in players]
  starting_players = [client for players in [starting_teams['RED'], starting_teams['BLUE']] for client in players]
  for client in starting_players:
    if (name_history[client][0]['is_bot'] == 1):
      # any bots? not a match.
      return (False, match_hash)
  original_players = []
  for client in ending_players:
    if (team_history[client][0]['team_start_time'] == map_start and
        team_history[client][0]['team'] == team_history[client][-1]['team'] and
        team_history[client][-1]['team_start_time'] <= ending_team_time and
        team_history[client][-1]['team_end_time'] >= ending_team_time):
      original_players.append(client)
  lost_players = [client for client in starting_players if client not in original_players]
  if len(ending_players) - len(original_players) > 2:
    print 'More than 2 substitutions were made: ', lost_players
    return (False, match_hash)
  # map must end in intermission (scoreboard shown).  demo file will only record scores during intermission
  intermission = map['scores']['is_final'] == 1
  if (not intermission):
    return (False, match_hash)
  print 'Is a pug - map_start was ' + str(map_start) + ', ' + str(len(starting_teams['RED'])) + '\'s'
  return (True, match_hash)

def is_match( data ):
  # pick the longest map from the demo to check
  map = data['maps'][0]
  map_start = map['map_start_time']
  map_end = map['map_end_time']
  map_duration = map_end - map_start
  for amap in data['maps']:
    if (amap['map_end_time'] - amap['map_start_time'] > map_duration):
      map = amap
      map_start = map['map_start_time']
      map_end = map['map_end_time']
      map_duration = map_end - map_start
  return map_is_match( data, map )

db = MongoClient().demos
demodb = db.demos
existing_demos = {}
'''
for doc in demodb.find({}, { 'metadata_mtime': 1, 'mongo_version': 1 }):
  demo = None
  if 'metadata_mtime' in doc:
    demo = {'mtime': doc['metadata_mtime']}
    if 'mongo_version' in doc:
      demo['mongo_version'] = doc['mongo_version']
    existing_demos[doc['_id']] = demo
    if len(existing_demos) % 100 == 0:
      print 'Fetched', len(existing_demos), 'demos from local db'
'''
current_demos = {}
for demo in demos:
  mtime = stat( demo + u'.dm_meta' ).st_mtime
  if demo not in existing_demos:
    for doc in demodb.find({'_id': demo}, { 'metadata_mtime': 1, 'mongo_version': 1 }):
      d = None
      if 'metadata_mtime' in doc:
        d = {'mtime': doc['metadata_mtime']}
        if 'mongo_version' in doc:
          d['mongo_version'] = doc['mongo_version']
        existing_demos[doc['_id']] = d
        if len(existing_demos) % 100 == 0:
          print 'Fetched', len(existing_demos), 'demos from local db'
  if demo in existing_demos and existing_demos[demo]['mtime']  >= mtime:
    #print 'Skipping already-present demo'
    if 'mongo_version' in existing_demos[demo] and existing_demos[demo]['mongo_version'] >= mongo_version:
      #print 'Skipping', demo, existing_demos[demo]
      continue
    #pass
  print 'Processing demo: ' + demo.encode('utf8')
  try:
    demometafd = open( demo + u'.dm_meta', u'r' )
  except:
    print traceback.format_exc()
    #print sys.exc_info()[0]
    continue
  try:
    demometa = json.loads(demometafd.read().decode('utf-8'))
  except:
    print traceback.format_exc()
    #print sys.exc_info()[0]
    continue
  if (demo.find(' ') != -1):
    tmstr = demo.rsplit(' ', 1)[1][0:-6]
  else:
    tmstr = ''
  if (any(c.isalpha() for c in tmstr)):
    tmstr = ''
  if (tmstr.find('-') == -1 and tmstr != ''):
    tmstr = ' '.join(demo.rsplit(' ', 2)[-2:])[0:-6]
  if (tmstr.find('-') == -1 or (len(tmstr) != 17 and len(tmstr) != 19)):
    print 'Warning: creation timestamp couldn\'t be found in filename, calculating from mtime'
    timemillis = stat(demo).st_mtime * 1000
    for map in demometa['maps']:
      timemillis -= map['map_end_time'] - map['map_start_time']
    tm = datetime.fromtimestamp(round(timemillis / 1000))
  else:
    if (tmstr.find('_') == -1):
      (date, time) = tmstr.split(' ')
    else:
      (date, time) = tmstr.split('_')
    tm = parse(date + ' ' + time.replace('-', ':'))
  tzone = demometa_lib.timezone_for_demo(demo)
  tm = tzone.localize(tm, is_dst=True)
  wrappeddemometa = { '_id': demo, 'time_created': tm, 'metadata_mtime': mtime, 'metadata': demometa }
  # write client name separately since mongodb can't query it properly
  if 'maps' in demometa and len(demometa['maps']) > 0:
    for idx, map in enumerate(demometa['maps']):
      (match, match_hash) = map_is_match(demometa, map)
      wrappeddemometa['metadata']['maps'][idx]['is_match'] = match
      wrappeddemometa['metadata']['maps'][idx]['match_hash'] = match_hash
    namefreq = {}
    allnames = [map['names'][str(demometa['client']['id'])] for map in demometa['maps'] if 'names' in map and str(demometa['client']['id']) in map['names']]
    allnames = [name for names in allnames for name in names]
    for name in allnames:
      if name['name'] in namefreq:
        curval = namefreq[name['name']]
      else:
        curval = 0
      namefreq[name['name']] = curval + name['name_end_time'] - name['name_start_time']
    maxnametime = 0
    maxname = ''
    for name in namefreq.keys():
      if namefreq[name] > maxnametime:
        maxname = name
        maxnametime = namefreq[name]
    wrappeddemometa['player'] = maxname
    (match, match_hash) = is_match(demometa)
    wrappeddemometa['is_match'] = match
    wrappeddemometa['match_hash'] = match_hash
    wrappeddemometa['mongo_version'] = mongo_version
  try:
    demoid = demodb.update({'_id': demo}, wrappeddemometa, upsert = True)
    current_demos[wrappeddemometa['_id']] = mtime
    #demoid = wrappeddemometa['_id']
    print 'Upserted', demoid
  except DuplicateKeyError:
    print 'Skipped duplicate, already in db'
    pass

#for demo in existing_demos.keys():
#  if demo not in current_demos:
#    print 'Removing demo', demo, 'which no longer exists'
#    demodb.remove({'_id': demo})

'''
result = db.command({
  'aggregate': 'demos',
  'pipeline': [
    {'$project': {
      "metadata.maps.mapname": 1,
      "metadata.maps.map_start_time": 1,
      "metadata.maps.map_end_time": 1,
    }},
    {'$unwind': "$metadata.maps"},
    {'$project': {
      "mapname": "$metadata.maps.mapname",
      "time": {'$subtract': ["$metadata.maps.map_end_time", "$metadata.maps.map_start_time"]},
    }},
    {'$group': {
      '_id': "$mapname",
      'value': {'$sum': "$time"},
    }},
    {'$out': "maps"},
  ],
  'allowDiskUse': True,
})
print 'Executed map frequency pipeline:', result

result = db.command({
  'aggregate': 'demos',
  'pipeline': [
    {'$project': {
      "sv_hostname": "$metadata.sv_hostname",
      "metadata.maps.map_start_time": 1,
      "metadata.maps.map_end_time": 1,
    }},
    {'$unwind': "$metadata.maps"},
    {'$project': {
      "sv_hostname": 1,
      "time": {'$subtract': ["$metadata.maps.map_end_time", "$metadata.maps.map_start_time"]},
    }},
    {'$group': {
      '_id': "$sv_hostname",
      'value': {'$sum': "$time"},
    }},
    {'$out': "servers"},
  ],
  'allowDiskUse': True,
})
print 'Executed server frequency pipeline:', result

result = db.command({
  'aggregate': 'demos',
  'pipeline': [
    {'$project': {
      "is_match": 1,
      "match_hash": 1,
      "time_created": 1,
      "metadata.maps.scores": 1,
      "metadata.maps.mapname": 1,
      "metadata.maps.map_start_time": 1,
      "metadata.maps.map_end_time": 1,
      "metadata.maps.serverId": 1,
      "metadata.maps.checksumFeed": 1,
      "metadata.sv_hostname": 1,
      "demo_id": { "id": "$_id", "client_id": "$metadata.client.id", "name": "$player" }
    }},
    {'$group': {
      '_id': "$match_hash",
      'demos': {'$push': "$demo_id"},
      'maps': {'$push': "$metadata.maps"},
      'sv_hostname': {'$first': "$metadata.sv_hostname"},
      'time_created': {'$min': "$time_created"},
      'is_match': {'$max': "$is_match"},
    }},
    {'$out': "matches"},
  ],
  'allowDiskUse': True,
})
print 'Executed match aggregation pipeline:', result

#result = db.command({
#  'aggregate': 'demos',
#  'pipeline': [
#    {'$project': {
#      "name": "$player",
#      "metadata.maps.map_start_time": 1,
#      "metadata.maps.map_end_time": 1,
#    }},
#    {'$unwind': "$metadata.maps"},
#    {'$project': {
#      "name": 1,
#      "time": {'$subtract': ["$metadata.maps.map_end_time", "$metadata.maps.map_start_time"]},
#    }},
#    {'$group': {
#      '_id': "$name",
#      'value': {'$sum': "$time"},
#    }},
#    {'$out': "names"},
#  ],
#  'allowDiskUse': True,
#})
# use names from scoreboard data instead, lower resolution times but more coverage
result = db.command({
  'aggregate': 'matches',
  'pipeline': [
    {'$unwind': '$maps'},
    {'$project': {
      'clients1': '$maps.scores.blueplayers',
      'clients2': '$maps.scores.redplayers',
      'clients3': '$maps.scores.freeeplayers',
      'clients4': '$maps.scores.specplayers',
      'idx': {'$literal': [1,2,3,4]}
    }},
    {'$unwind': '$idx'},
    {'$project': {
      'client': {'$cond': [{'$eq': ['$idx', 1]}, '$clients1',
        {'$cond': [{'$eq': ['$idx', 2]}, '$clients2',
        {'$cond': [{'$eq': ['$idx', 3]}, '$clients3',
        '$clients4']}]}]}
    }},
    {'$unwind': '$client'},
    {'$unwind': '$client'},
    {'$group': {'_id': '$client.client_name', 'value': {'$sum': '$client.time'}}},
    {'$out': 'names'}
  ],
  'allowDiskUse': True,
})

print 'Executed name frequency pipeline:', result
'''
