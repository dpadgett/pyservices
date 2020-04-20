#!/usr/bin/python -u

from subprocess import Popen, PIPE
from os import listdir, stat
from os.path import isfile, isdir, join, exists
import locale
import sys
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
import json
from dateutil.parser import *
from dateutil.tz import *
from datetime import *
from dateutil.relativedelta import *
import pytz
from pytz import timezone
import math
from bson.code import Code
from collections import deque
import hashlib
import struct
import traceback

import find_demos

import shrinker
import copy
import demometa_lib

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

db = MongoClient("mongodb").demos
basedir = u'/cygdrive/U/demos/'
player_remap = {
  'c': '$$player.client',
  'n': '$$player.client_name',
  's': '$$player.score',
  'p': '$$player.ping',
  't': '$$player.time',
}

names_remap = {
  '$$name.name': 'n',
  '$$name.guid_hash': 'g',
  '$$name.ip_hash': 'i',
  '$$name.is_bot': 'b',
  '$$name.name_start_time': 's',
  '$$name.name_end_time': 'e',
}

all_names_remap = {}
for clientid in range(32):
  all_names_remap['%d' % (clientid)] = {
    '$map': {
      'input': '$$mapp.names.%d' % (clientid),
      'as': 'name',
      'in': names_remap,
    }
  }

'''
result = db.command({
  'aggregate': 'demos',
  'pipeline': [
    #{'$match': {
    #  '_id': basedir + 'japlus/mp_ctf1 2014-02-05 082924/3 ^5Gabriel mp_ctf1 2014-02-05 084531.dm_26'
    #}},
    {'$project': {
      '_id': { '$substr': ['$_id', len(basedir), -1] },
      't': '$time_created',
      'mt': '$metadata_mtime',
      'p': '$player',
      'ma': '$is_match',
      'h': { '$substr': ['$match_hash', 0, 32] },
      'm': {
        'c': '$metadata.client',
        'h': '$metadata.sv_hostname',
        'm': { '$map': {
          'input': '$metadata.maps',
          'as': 'mapp',
          'in': {
            'n': '$$mapp.mapname',
            's': '$$mapp.map_start_time',
            'e': '$$mapp.map_end_time',
            'ma': '$$mapp.is_match',
            'h': '$$mapp.match_hash',
            'na': all_names_remap
            'sc': {
              'fi': '$$mapp.scores.is_final',
              'rs': '$$mapp.scores.red_score',
              'bs': '$$mapp.scores.blue_score',
              'f': { '$map': {
                'input': '$$mapp.scores.freeplayers',
                'as': 'player',
                'in': player_remap
              }},
              'r': { '$map': {
                'input': '$$mapp.scores.redplayers',
                'as': 'player',
                'in': player_remap
              }},
              'b': { '$map': {
                'input': '$$mapp.scores.blueplayers',
                'as': 'player',
                'in': player_remap
              }},
              's': { '$map': {
                'input': '$$mapp.scores.specplayers',
                'as': 'player',
                'in': player_remap
              }}
            }
          }
        }}
      }
    }},
    {'$out': "mindemos"},
  ],
  'allowDiskUse': True,
})
print 'Executed mindemo pipeline:', result

exit()
'''

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
  # UPDATE: this no longer works due to openjk server time reset.
  if map_start >= 20000:
    print 'Not a pug - map_start was ' + str(map_start)
    return (False, match_hash)
  if map_duration < 10 * 60 * 1000:
    print 'Not a pug - map duration was ' + str(map_duration)
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
  if 'scores' in map:
    teams = [x for x in ['red', 'blue', 'free'] if x + 'players' in map['scores']]
    playerscores = [player for x in teams for player in map['scores'][x + 'players']]
    mapdur = map_end / 1000 / 60
    times = [client['time'] for client in playerscores]
    if len([t for t in times  if t >= mapdur - 1]) == 0:
      print 'Not a pug - no player stayed for full map duration of ' + str(mapdur) + ' - only ' + str(times)
      return (False, match_hash)
  # map must end in intermission (scoreboard shown).  demo file will only record scores during intermission
  intermission = map['scores']['is_final'] == 1
  if (not intermission):
    print 'Not a pug - scores not sent during intermission'
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

def update_maps(old, new, map_updates):
  if old != None:
    for map in old['m']['m']:
      cur_value = 0
      if map['n'] in map_updates:
        cur_value = map_updates[map['n']]
      map_updates[map['n']] = cur_value - (map['e'] - map['s'])
  for map in new['m']['m']:
    cur_value = 0
    if map['n'] in map_updates:
      cur_value = map_updates[map['n']]
    map_updates[map['n']] = cur_value + (map['e'] - map['s'])
  #print 'map updates', map_updates
  return map_updates

def update_servers(old, new, server_updates):
  if old != None and 'h' in old['m']:
    server = old['m']['h']
    for map in old['m']['m']:
      cur_value = 0
      if server in server_updates:
        cur_value = server_updates[server]
      server_updates[server] = cur_value - (map['e'] - map['s'])
  if 'h' not in new['m']:
    print 'Error: Couldn\'t find server in', new
    return server_updates
  server = new['m']['h']
  for map in new['m']['m']:
    cur_value = 0
    if server in server_updates:
      cur_value = server_updates[server]
    server_updates[server] = cur_value + (map['e'] - map['s'])
  #print 'server updates', server_updates
  return server_updates

basedir = u'/cygdrive/U/demos/'
#db = MongoClient('mongodb://stuffer:plz2gold@sjc.jactf.com,pub.jactf.com,whoracle.jactf.com/demos?replicaSet=ctfpug', tz_aware = True).demos
#db = MongoClient('mongodb://localhost:27018/demos', tz_aware = True).demos
demodb = db.mindemos

base_matches = {}
def get_match(id, match_updates):
  global db, base_matches
  if id in match_updates:
    return match_updates[id]
  cursor = db.minmatches.find({'_id': id})
  for match in cursor:
    match_updates[id] = match
    base_matches[id] = match
    return match
  match_updates[id] = None
  return None

def update_matches(old, new, match_updates):
  # can skip the initial projection, which is done just to save space in unwind
  if old != None:
    for map in old['m']['m']:
      id = map['h']
      print id
      match = get_match(id, match_updates)
      if match != None:
        # find the index of this demo in the demo list
        indexes = []
        for index, val in enumerate(match['d']):
          if val['id'] == old['_id']:
            indexes.append(index)
        # remove it from the demo list and its scores from the scores list
        match['d'][:] = [d for index, d in enumerate(match['d']) if index not in indexes]
        match['sc'][:] = [sc for index, sc in enumerate(match['sc']) if index not in indexes]
        if len(match['d']) == 0:
          match_updates[id] = None
        # the rest can't be properly resolved without re-fetching everything so don't bother
  if new != None:
    for map in new['m']['m']:
      id = map['h']
      print id
      match = get_match(id, match_updates)
      if match == None:
        match = {'_id': id, 'd': [], 's': None, 'e': None, 'n': None, 'sc': [], 'h': None, 't': None, 'ma': None}
        match_updates[id] = match
      match['d'].append({'id': new['_id'], 'c': new['m']['c']['id'], 'n': new['p']})
      match['sc'].append(map['sc'])
      if match['h'] == None:
        match['h'] = new['m']['h']
      if match['s'] == None:
        match['s'] = map['s']
      else:
        match['s'] = min(match['s'], map['s'])
      if match['e'] == None:
        match['e'] = map['e']
      else:
        match['e'] = max(match['e'], map['e'])
      if match['n'] == None:
        match['n'] = map['n']
      if match['t'] == None:
        match['t'] = new['t']
      else:
        #print match['t'], new['t']
        matchtime = match['t']
        if matchtime.tzinfo is None:
          matchtime = matchtime.replace(tzinfo=pytz.UTC)
        match['t'] = min(matchtime, new['t'])
      if match['ma'] == None:
        match['ma'] = map['ma']
      else:
        match['ma'] = match['ma'] or map['ma']
      #print 'match updates', match_updates
  return match_updates

def update_names(match_updates):
  name_updates = {}
  global base_matches
  for id, match in base_matches.iteritems():
    if match == None:
      continue
    for scores in match['sc']:
      for team in ['b', 'r', 's', 'f']:
        if team not in scores or scores[team] == None:
          continue
        for player in scores[team]:
          cur_value = 0
          if player['n'] in name_updates:
            cur_value = name_updates[player['n']]
          name_updates[player['n']] = cur_value - player['t']
  for id, match in match_updates.iteritems():
    if match == None:
      continue
    for scores in match['sc']:
      for team in ['b', 'r', 's', 'f']:
        if team not in scores or scores[team] == None:
          continue
        for player in scores[team]:
          cur_value = 0
          if player['n'] in name_updates:
            cur_value = name_updates[player['n']]
          name_updates[player['n']] = cur_value + player['t']
  final_name_updates = {}
  for name, value in name_updates.iteritems():
    if value != 0:
      final_name_updates[name] = value
  #print 'name updates', name_updates
  return final_name_updates

#existing_demos = {}
#for doc in demodb.find({}, { 'mt': 1 }):
#  if 'mt' in doc:
#    existing_demos[basedir + doc['_id']] = doc['mt']
current_demos = {}
map_updates = {}
server_updates = {}
match_updates = {}
for demo in demos:
  mtime = stat( (demo + u'.dm_meta').encode('utf8') ).st_mtime
  existing_mtime = -1
  for doc in demodb.find({'_id': demo[len(basedir):]}, { 'mt': 1 }):
    if 'mt' in doc:
      existing_mtime = doc['mt'] = doc['mt']
  #if demo in existing_demos and existing_demos[demo] >= mtime:
  if existing_mtime >= mtime:
    print 'Skipping already-present demo', demo.encode('utf8')
    continue
    #pass
  print 'Processing demo: ' + demo.encode('utf8')
  try:
    demometafd = open( (demo + u'.dm_meta').encode('utf8'), u'r' )
  except:
    print sys.exc_info()[0]
    continue
  try:
    demometa = json.loads(demometafd.read().decode('utf-8'))
  except:
    print sys.exc_info()[0]
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
    timemillis = stat(demo.encode('utf8')).st_mtime * 1000
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
  print 'Time:', tm
  wrappeddemometa = shrinker.minimize({'_id': demo, 'time_created': tm, 'metadata_mtime': mtime, 'metadata': copy.deepcopy(demometa)})
  # write client name separately since mongodb can't query it properly
  if 'maps' in demometa and len(demometa['maps']) > 0:
    for idx, map in enumerate(demometa['maps']):
      (match, match_hash) = map_is_match(demometa, map)
      wrappeddemometa['m']['m'][idx]['ma'] = match
      wrappeddemometa['m']['m'][idx]['h'] = match_hash
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
    wrappeddemometa['p'] = maxname
    (match, match_hash) = is_match(demometa)
    wrappeddemometa['ma'] = match
    wrappeddemometa['h'] = match_hash
  try:
    oldwrappeddemometa = demodb.find_and_modify(query = {'_id': wrappeddemometa['_id']}, update = wrappeddemometa, upsert = True)
    #oldwrappeddemometacursor = demodb.find({'_id': wrappeddemometa['_id']})
    #oldwrappeddemometa = None
    #for m in oldwrappeddemometacursor:
    #  oldwrappeddemometa = m
    #  break
    update_maps(oldwrappeddemometa, wrappeddemometa, map_updates)
    update_servers(oldwrappeddemometa, wrappeddemometa, server_updates)
    update_matches(oldwrappeddemometa, wrappeddemometa, match_updates)
    current_demos[wrappeddemometa['_id']] = mtime
    demoid = wrappeddemometa['_id']
    print 'Upserted', demoid.encode('utf8')
  except DuplicateKeyError:
    print 'Skipped duplicate, already in db'
    pass
  except:
    print 'Error on', wrappeddemometa
    print traceback.format_exc()
    exit()
name_updates = update_names(match_updates)

'''
print 'map updates:', map_updates
print 'server updates:', server_updates
print 'name updates:', name_updates
print 'match updates:', match_updates
'''

# apply the various updates
if len(map_updates) > 0:
  bulk = db.minmaps.initialize_unordered_bulk_op()
  for map, delta in map_updates.iteritems():
    bulk.find({'_id': map}).upsert().update({'$inc': {'t': delta}})
  map_result = bulk.execute()
  print 'executed map writes:', map_result

if len(server_updates) > 0:
  bulk = db.minservers.initialize_unordered_bulk_op()
  for server, delta in server_updates.iteritems():
    bulk.find({'_id': server}).upsert().update({'$inc': {'t': delta}})
  server_result = bulk.execute()
  print 'executed server writes:', server_result

if len(name_updates) > 0:
  bulk = db.minnames.initialize_unordered_bulk_op()
  for name, delta in name_updates.iteritems():
    bulk.find({'_id': name}).upsert().update({'$inc': {'t': delta}})
  name_result = bulk.execute()
  print 'executed name writes:', name_result

if len(match_updates) > 0:
  bulk = db.minmatches.initialize_unordered_bulk_op()
  for hash, match in match_updates.iteritems():
    if match == None:
      bulk.find({'_id': hash}).remove_one()
    else:
      bulk.find({'_id': hash}).upsert().replace_one(match)
  try:
    match_result = bulk.execute()
  except BulkWriteError as bwe:
    match_result = bwe.details
  print 'executed match writes:', match_result

# exit()


# #for demo in existing_demos.keys():
# #  if demo not in current_demos:
# #    print 'Removing demo', demo, 'which no longer exists'
# #    demodb.remove({'_id': demo})


# result = db.command({
  # 'aggregate': 'mindemos',
  # 'pipeline': [
    # {'$project': {
      # "m.m.n": 1,
      # "m.m.s": 1,
      # "m.m.e": 1,
    # }},
    # {'$unwind': "$m.m"},
    # {'$project': {
      # "n": "$m.m.n",
      # "t": {'$subtract': ["$m.m.e", "$m.m.s"]},
    # }},
    # {'$group': {
      # '_id': "$n",
      # 't': {'$sum': "$t"},
    # }},
    # {'$out': "minmaps"},
  # ],
  # 'allowDiskUse': True,
# })
# print 'Executed map frequency pipeline:', result

# result = db.command({
  # 'aggregate': 'mindemos',
  # 'pipeline': [
    # {'$project': {
      # "h": "$m.h",
      # "m.m.s": 1,
      # "m.m.e": 1,
    # }},
    # {'$unwind': "$m.m"},
    # {'$project': {
      # "h": 1,
      # "t": {'$subtract': ["$m.m.e", "$m.m.s"]},
    # }},
    # {'$group': {
      # '_id': "$h",
      # 't': {'$sum': "$t"},
    # }},
    # {'$out': "minservers"},
  # ],
  # 'allowDiskUse': True,
# })
# print 'Executed server frequency pipeline:', result

# '''result = db.command({
  # 'aggregate': 'mindemos',
  # 'pipeline': [
    # {'$project': {
      # "ma": 1,
      # "h": 1,
      # "t": 1,
      # "m.m.sc": 1,
      # "m.m.n": 1,
      # "m.m.s": 1,
      # "m.m.e": 1,
      # "m.h": 1,
      # "d": { "id": "$_id", "c": "$m.c.id", "n": "$p" }
    # }},
    # {'$group': {
      # '_id': "$h",
      # 'd': {'$push': "$d"},
      # 'm': {'$first': "$m.m"},
      # 'h': {'$first': "$m.h"},
      # 't': {'$min': "$t"},
      # 'ma': {'$max': "$ma"},
    # }},
    # {'$out': "minmatches"},
  # ],
  # 'allowDiskUse': True,
# })'''

# '''
# result = db.command({
  # 'aggregate': 'mindemos',
  # 'pipeline': [
    # {'$group': {
      # '_id': "$h",
      # 'd': {'$push': { "id": "$_id", "c": "$m.c.id", "n": "$p" }},
      # 'm': {'$first': { "sc": "$m.m.sc", "n": "$m.m.n", "s": "$m.m.s", "e": "$m.m.e" }},
      # 'h': {'$first': "$m.h"},
      # 't': {'$min': "$t"},
      # 'ma': {'$max': "$ma"},
    # }},
    # {'$out': "minmatches"},
  # ],
  # 'allowDiskUse': True,
# })
# '''

# result = db.command({
  # 'aggregate': 'mindemos',
  # 'pipeline': [
    # {'$group': {
      # '_id': "$h",
      # 'd': {'$push': { "id": "$_id", "c": "$m.c.id", "n": "$p" }},
      # 'm': {'$first': { "sc": "$m.m.sc", "n": "$m.m.n", "s": "$m.m.s", "e": "$m.m.e" }},
      # 'h': {'$first': "$m.h"},
      # 't': {'$min': "$t"},
      # 'ma': {'$max': "$ma"},
    # }},
    # {'$out': "minmatches"},
  # ],
  # 'allowDiskUse': True,
# })

# result = db.command({
  # 'aggregate': 'mindemos',
  # 'pipeline': [
    # {'$project': {
      # "t": 1,
      # "m.m.sc": 1,
      # "m.m.n": 1,
      # "m.m.s": 1,
      # "m.m.e": 1,
      # "m.m.ma": 1,
      # "m.m.h": 1,
      # "m.h": 1,
      # "d": { "id": "$_id", "c": "$m.c.id", "n": "$p" }
    # }},
    # {'$unwind': "$m.m"},
    # {'$project': {
      # "t": 1,
      # "m.m": {
        # "sc": "$m.m.sc",
        # "n": "$m.m.n",
        # "s": "$m.m.s",
        # "e": "$m.m.e",
        # "d": {'$subtract': ["$m.m.e", "$m.m.s"]},
        # "ma": "$m.m.ma",
        # "h": "$m.m.h"
      # },
      # "m.h": 1,
      # "d": 1
    # }},
    # {'$sort': {
      # "m.m.sc.fi": -1,
      # "m.m.d": -1
    # }},
    # {'$group': {
      # '_id': "$m.m.h",
      # 'd': {'$push': "$d"},
      # 's': {'$min': "$m.m.s"},
      # 'e': {'$max': "$m.m.e"},
      # 'n': {'$first': "$m.m.n"},
      # 'sc': {'$push': "$m.m.sc"},
      # 'h': {'$first': "$m.h"},
      # 't': {'$min': "$t"},
      # 'ma': {'$max': "$m.m.ma"},
    # }},
    # {'$out': "minmatches"},
  # ],
  # 'allowDiskUse': True,
# })
# print 'Executed match aggregation pipeline:', result

# #result = db.command({
# #  'aggregate': 'demos',
# #  'pipeline': [
# #    {'$project': {
# #      "name": "$player",
# #      "metadata.maps.map_start_time": 1,
# #      "metadata.maps.map_end_time": 1,
# #    }},
# #    {'$unwind': "$metadata.maps"},
# #    {'$project': {
# #      "name": 1,
# #      "time": {'$subtract': ["$metadata.maps.map_end_time", "$metadata.maps.map_start_time"]},
# #    }},
# #    {'$group': {
# #      '_id': "$name",
# #      'value': {'$sum': "$time"},
# #    }},
# #    {'$out': "names"},
# #  ],
# #  'allowDiskUse': True,
# #})
# # use names from scoreboard data instead, lower resolution times but more coverage
# result = db.command({
  # 'aggregate': 'minmatches',
  # 'pipeline': [
    # {'$project': {
      # 'clients1': '$sc.b',
      # 'clients2': '$sc.r',
      # 'clients3': '$sc.f',
      # 'clients4': '$sc.s',
      # 'idx': {'$literal': [1,2,3,4]}
    # }},
    # {'$unwind': '$idx'},
    # {'$project': {
      # 'client': {'$cond': [{'$eq': ['$idx', 1]}, '$clients1',
        # {'$cond': [{'$eq': ['$idx', 2]}, '$clients2',
        # {'$cond': [{'$eq': ['$idx', 3]}, '$clients3',
        # '$clients4']}]}]}
    # }},
    # {'$unwind': '$client'},
    # {'$unwind': '$client'},
    # {'$group': {'_id': '$client.n', 't': {'$sum': '$client.t'}}},
    # {'$out': 'minnames'}
  # ],
  # 'allowDiskUse': True,
# })

# print 'Executed name frequency pipeline:', result
