#!/usr/bin/python
# -*- coding: utf-8 -*-
# version of merge_matches which uses prediction to generate any missing povs
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import pymongo
import re
import random
import json
import shlex
from os.path import isfile, isdir, join, exists
from os import listdir
import zipfile
import sys
import hashlib
import struct

from dateutil.parser import *
from dateutil.tz import *
from datetime import *
from dateutil.relativedelta import *
from pytz import timezone

from subprocess import Popen, PIPE, call
from os import listdir, stat, remove
from os.path import isfile, isdir, join, exists, basename

import trueskill

import merge_metadata
import shrinker
import traceback

def strip_non_ascii(string):
  ''' Returns the string without non ASCII characters'''
  stripped = (c for c in string if 0 < ord(c) < 127)
  return ''.join(stripped)

def strip_fs(string):
  ''' Returns the string without any special fs characters'''
  stripped = (c for c in strip_non_ascii(string) if c != '.' and c != '/' and c != "\\")
  return ''.join(stripped)

def strip_colors(string):
  ''' Returns the string with color codes stripped'''
  idx = 0
  result = ''
  while idx < len(string):
    if string[idx] == '^' and idx < len(string) - 1 and ord(string[idx + 1]) >= ord('0') and ord(string[idx + 1]) <= ord('9'):
      idx += 2
      continue
    result += string[idx]
    idx += 1
  return result

def strip_html(string):
  ''' Returns the string without any special html characters'''
  stripped = (c for c in string if c != '<' and c != '>')
  return ''.join(stripped)

def format_time(time):
  return ("%02d" % (time / 1000 / 60 / 60)) + ':' + ("%02d" % (time / 1000 / 60 % 60)) + ':' + ("%02d" % (time / 1000 % 60)) + '.' + ("%03d" % (time % 1000))

def map_match_hash( map ):
  if 'match_hash' in map:
    return map['match_hash']
  # compute a hash to identify this match
  match_id = ((map['serverId'] & 0xFFFFFFFF) << 32) + (map['checksumFeed'] & 0xFFFFFFFF)
  match_hash = struct.pack('!Q', match_id).encode('hex')
  return match_hash

def findmap(maps, match_hash):
  for idx, map in enumerate(maps):
    if map_match_hash(map) == match_hash:
      return (idx, map)
  raise Exception("Couldn't find map with hash %s" % (match_hash))

def mergematchmeta(match):
  global maps
  db = MongoClient("mongodb").demos
  #demodb = db.demos
  mindemodb = db.mindemos
  events = []
  demosbyid = {}
  demofiles = []
  demometabyid = {}
  for demo in match['d']:
    # overloading this for now
    demosbyid[demo['c']] = 'U:/demos/' + demo['id']
    demofiles.append('U:/demos/' + demo['id'])
  #demofiles = demosbyid.values()
  mapstart = None
  mapend = None
  for demo in match['d']:
    #print 'Checking', demo['id']
    client = demo['c']
    #demodata = demodb.find({'_id':'/cygdrive/U/demos/' + demo['id']})[0]
    #demodata = shrinker.inflate(mindemodb.find({'_id': demo['id']})[0])
    try:
      with open(('/cygdrive/U/demos/' + demo['id'] + '.dm_meta').encode('utf-8')) as metaf:
        demodata = {'_id': '/cygdrive/U/demos/' + demo['id'], 'metadata': json.loads(metaf.read())}
    except:
      print 'failed to find', demo['id'].encode('utf-8')
      continue
    demo['meta'] = demodata['metadata']
    mapidx, map = findmap(demodata['metadata']['maps'], match['_id'])
    demo['mapidx'] = mapidx
    if mapstart == None:
      mapstart = map['map_start_time']
    else:
      mapstart = min(mapstart, map['map_start_time'])
    if mapend == None:
      mapend = map['map_end_time']
    else:
      mapend = max(mapend, map['map_end_time'])
  #print match['demos']
  #return
  for demo in match['d']:
    #print 'Processing', demo['id']
    if 'meta' not in demo:
      continue
    client = demo['c']
    demodata = {'_id': '/cygdrive/U/demos/' + demo['id'], 'metadata': demo['meta']}
    if client in demometabyid and demometabyid[client]['metadata']['maps'][demosbyid[client][0]]['scores']['is_final'] == 1:
      # multiple demos for the same client?  prefer one with final scoreboard.
      continue
    demometabyid[client] = demodata
    mapidx = demo['mapidx']
    map = demodata['metadata']['maps'][mapidx]
    demosbyid[client] = (mapidx, demo['id'].replace('/cygdrive/U/', 'U:/').replace('/cygdrive/C/', 'C:/'))
    for event in map.get('ctfevents', []):
      if 'attacker' in event:
        attacker = event['attacker']
        if event['eventtype'] == 'FRAGGED_FLAG_CARRIER':
          continue # this is easier to do by scanning frags instead
        #if attacker != client:
        #  continue
        humantime = event['human_time']
        timeparts = humantime.replace('.', ':').split(':')
        time = ((int(timeparts[0]) * 60 + int(timeparts[1])) * 60 + int(timeparts[2])) * 1000 + int(timeparts[3])
        if time < 0:
          raise Exception("Weird event: " + json.dumps(event) + " in demo " + demo['id'] + "\nFull metadata:\n" + json.dumps(demo))
        events.append( {'attacker': attacker, 'time': time, 'event': event['eventtype'], 'team': event['team']} )
    for frag in map.get('frags', []):
      if frag['target_had_flag'] == 0 and frag['attacker'] != frag['target']:
        continue
      attacker = frag['attacker']
      #if attacker != client:
      #  continue
      humantime = frag['human_time']
      timeparts = humantime.replace('.', ':').split(':')
      #time = ((int(humantime[0:2]) * 60 + int(humantime[3:5])) * 60 + int(humantime[6:8])) * 1000 + int(humantime[9:12])
      time = ((int(timeparts[0]) * 60 + int(timeparts[1])) * 60 + int(timeparts[2])) * 1000 + int(timeparts[3])
      if frag['target_had_flag'] != 0:
        event = 'FRAGGED_FLAG_CARRIER'
      else:
        event = 'SELFKILLED' # need to record these as they don't store whether or not player had the flag
      events.append( {'attacker': attacker, 'time': time, 'event': event, 'team': frag['attacker_team'], 'target': frag['target']} )
  events = sorted(events, key = lambda event: event['attacker'])
  events = sorted(events, key = lambda event: event['event'], reverse=True)
  events = sorted(events, key = lambda event: event['time'])

  scores = match['sc'][0]
  for sc in match['sc']:
    if sc['fi']:
      scores = sc
      #print 'Found final scores'
      break
  
  stats = {}
  basestat = {'grabs': 0, 'caps': 0, 'holdtime': 0, 'enemyholdtime': 0, 'fcfrags': 0, 'returns': 0, 'lasttouchtime': -1}
  for clientid in demometabyid.keys():
    stats[clientid] = basestat.copy()
  for t in ['r', 'b']:
    for player in scores[t]:
      stats[player['c']] = basestat.copy()

  lastevent = {'time': -1, 'attacker': -1, 'event': ''}
  for event in events:
    if lastevent['attacker'] == event['attacker'] and lastevent['event'] == event['event']:
      if event['time'] - lastevent['time'] < 500:
        #print 'Skipping duplicate event at time', event['time']
        continue
    lastevent = event
    #print 'Processing event', event
    if event['attacker'] not in stats:
      stats[event['attacker']] = basestat.copy()
    if 'target' in event and event['target'] not in stats:
      stats[event['target']] = basestat.copy()
    if event['event'] == 'FRAGGED_FLAG_CARRIER' or (event['event'] == 'SELFKILLED' and stats[event['target']]['lasttouchtime'] != -1):
      stats[event['attacker']]['fcfrags'] += 1
      if stats[event['target']]['lasttouchtime'] != -1:
        stats[event['target']]['holdtime'] += event['time'] - stats[event['target']]['lasttouchtime']
        stats[event['attacker']]['enemyholdtime'] += event['time'] - stats[event['target']]['lasttouchtime']
        stats[event['target']]['lasttouchtime'] = -1
      else:
        #print 'Unknown lasttouchtime:', match['_id'], event
        pass
    elif event['event'] == 'PLAYER_GOT_FLAG':
      stats[event['attacker']]['grabs'] += 1
      stats[event['attacker']]['lasttouchtime'] = event['time']
    elif event['event'] == 'PLAYER_CAPTURED_FLAG':
      stats[event['attacker']]['caps'] += 1
      if stats[event['attacker']]['lasttouchtime'] != -1:
        stats[event['attacker']]['holdtime'] += event['time'] - stats[event['attacker']]['lasttouchtime']
        stats[event['attacker']]['lasttouchtime'] = -1
      else:
        #print 'Unknown lasttouchtime'
        pass
    elif event['event'] == 'PLAYER_RETURNED_FLAG':
      stats[event['attacker']]['returns'] += 1
  
  allnames = merge_metadata.merge_history([meta['metadata'] for meta in demometabyid.values()], match['_id'], 'name')
  allteams = merge_metadata.merge_history([meta['metadata'] for meta in demometabyid.values()], match['_id'], 'team')
  
  finalstats = []
  for clientid, stat in stats.iteritems():
    if stat['lasttouchtime'] != -1:
      stat['holdtime'] += mapend - stat['lasttouchtime']
    del stat['lasttouchtime']
    teamtime = {'RED': 0, 'BLUE': 0}
    for team in allteams['%d' % clientid]:
      if team['team'] in teamtime:
        suffix = ''
        if 'team_end_time_raw' in team:
          suffix = '_raw'
        teamtime[team['team']] += team['team_end_time' + suffix] - team['team_start_time' + suffix]
    stat['playtime'] = teamtime['RED'] + teamtime['BLUE']
    if stat['playtime'] == 0:
      continue
    if teamtime['RED'] > teamtime['BLUE']:
      team = 'RED'
    else:
      team = 'BLUE'
    stat['team'] = team
    names = {}
    for name in allnames['%d' % clientid]:
      suffix = ''
      if 'name_end_time_raw' in name:
        suffix = '_raw'
      nametime = name['name_end_time' + suffix] - name['name_start_time' + suffix]
      if name['name'] not in names:
        names[name['name']] = 0
      names[name['name']] += nametime
    maxname = {'name': '', 'time': -1}
    for name, time in names.iteritems():
      if time > maxname['time']:
        maxname = {'name': name, 'time': time}
    stat['name'] = maxname['name']#strip_html(strip_colors(maxname['name']))
    stat['clientid'] = clientid
    #print clientid, stat
    finalstats.append(stat)
  
  return (finalstats, scores, mapend - mapstart)

# trim the fat of mergematchmeta so it runs fast for elo bot
import shrinker
import copy
def tinystats(tinymatch):
  db = MongoClient("mongodb").demos
  metas = [shrinker.inflate(m) for m in db.mindemos.find({'_id': {'$in': [demo['id'] for demo in tinymatch['d']]}})]
  print 'Read metas:', len(metas)
  #print shrinker.inflate(metas.next())
  stats = {}
  for demodata in metas:
    mapidx, map = findmap(demodata['metadata']['maps'], tinymatch['_id'])
    clientid = demodata['metadata']['client']['id']
    scores = map['scores']
    for team in ['red', 'blue']:
      for player in scores.get(team + 'players', []):
        if player['client'] == clientid:
          stats[clientid] = {'clientid': clientid, 'team': player['team'], 'playtime': player['time']}
  match = shrinker.inflate_match(copy.deepcopy(tinymatch))
  scores = tinymatch['sc'][0]
  for sc in tinymatch['sc']:
    if sc['fi']:
      scores = sc
      #print 'Found final scores'
      break
  return (stats.values(), sc, match['map_end_time'] - match['map_start_time'])

ratings = {}

def updaterank(match):
  global ratings
  # first see if we even know the players in this match
  db = MongoClient("mongodb").demos
  playerdb = db.players
  playergamedb = db.playerGames
  playergames = playergamedb.find({'_id.match': match['_id']}, {'client_num': 1})
  # map client_num -> player
  playermap = {}
  playergamemap = {}
  for playergame in playergames:
    playermap[playergame['client_num']] = playergame['_id']['player']
    playergamemap[playergame['_id']['player']] = playergame
  #print playermap
  if playermap == {}:
    print 'No players found'
    return
  #exit(1)
  stats, scores, maptime = mergematchmeta(match)
  result = {}
  if scores['bs'] > scores['rs']:
    winner = 'BLUE'
    result = {'RED': 1, 'BLUE': 0}
  elif scores['bs'] == scores['rs']:
    winner = 'TIE'
    result = {'RED': 0, 'BLUE': 0}
  else:
    winner = 'RED'
    result = {'RED': 0, 'BLUE': 1}
  winners = []
  losers = []
  teamrating = {'RED': {}, 'BLUE': {}}
  teamtime = {'RED': {}, 'BLUE': {}}
  for stat in stats:
    if stat['clientid'] not in playermap:
      print 'Unknown player %s in match %s' % (stat['clientid'], match['_id'])
      return
    playerid = playermap[stat['clientid']]
    if playerid not in ratings:
      # first check database
      playergames = playergamedb.find({'_id.player': playerid, 'time': {'$lt': match['t']}, 'rating.updated.raw.sigma': {'$exists': True}}).sort('time', pymongo.DESCENDING).limit(1)
      playergame = None
      for playergame in playergames:
        break
      if playergame != None:
        #numgames = playergamedb.find({'_id.player': playerid, 'time': {'$lt': match['t']}, 'rating.updated.raw.sigma': {'$exists': True}}).count()
        # for speed, skipping numgames.  it doesn't do much.
        numgames = 50
        ratings[playerid] = {'rating': trueskill.Rating(mu=playergame['rating']['updated']['raw']['mu'], sigma=playergame['rating']['updated']['raw']['sigma']), 'games': numgames}
      else:
        ratings[playerid] = {'rating': trueskill.Rating(), 'games': 0}
    #update['rating'] = ratings[playerid]['rating']
    rating = ratings[playerid]['rating']
    #if 
    teamrating[stat['team']][playerid] = rating
    teamtime[stat['team']][playerid] = stat['playtime']
  teamratings = [teamrating[team] for team in ['RED', 'BLUE']]
  teamtimes = {}
  for idx, team in enumerate(['RED', 'BLUE']):
    for id, tt in teamtime[team].iteritems():
      teamtimes[(idx, id)] = tt * 1.0 / maptime
  print teamratings, teamtimes
  quality = trueskill.quality(teamratings, weights=teamtimes)
  print('{:.1%} chance to draw'.format(quality))
  updates = trueskill.rate(teamratings, ranks=[result['RED'], result['BLUE']], weights=teamtimes)
  print updates
  pgbulk = playergamedb.initialize_unordered_bulk_op()
  pbulk = playerdb.initialize_unordered_bulk_op()
  for teamupdate in updates:
    for playerid, update in teamupdate.iteritems():
      #playergames = playergamedb.find({'_id.player': playerid, '_id.match': match['_id']})
      #for playergame in playergames:
      #  break
      playergame = playergamemap[playerid]
      start = ratings[playerid]['rating']
      def ratingObj(rating):
        return {'raw': {'mu': rating.mu, 'sigma': rating.sigma}, 'friendly': trueskill.expose(rating)}
      #playergame['rating'] = {'start': ratingObj(start), 'updated': ratingObj(update)}
      pgbulk.find({'_id': playergame['_id']}).update_one({'$set': {'rating': {'start': ratingObj(start), 'updated': ratingObj(update)}}})
      '''
      players = playerdb.find({'_id': playerid})
      player = None
      for player in players:
        break
      if player == None:
        print 'Unknown player', playerid
      player['rating'] = ratingObj(update)
      if 'matches' in player:
        del player['matches']
      '''
      pbulk.find({'_id': playerid}).update_one({'$set': {'rating': ratingObj(update)}})
      #playerdb.save(player)
      ratings[playerid]['rating'] = update
      ratings[playerid]['games'] += 1
  print pgbulk.execute()
  print pbulk.execute()
  return
  expected = {}
  delta = {}
  for team in [['RED', 'BLUE'], ['BLUE', 'RED']]:
    expected[team[0]] = 1.0 / (1 + 10.0 ** ((teamrating[team[1]] - teamrating[team[0]]) / 400.0))
  for team in ['RED', 'BLUE']:
    delta[team] = int(round(15 * (result[team] - expected[team])))
    print team, teamrating[team], result[team], expected[team], delta[team]
  for stat in stats:
    #if stat['team'] == 'RED':
    #  otherteam = 'BLUE'
    #else:
    #  otherteam = 'RED'
    #myrating = teamrating[update['team']]['rating'] / teamrating[update['team']]['count']
    #otherrating = teamrating[otherteam]['rating'] / teamrating[otherteam]['count']
    #print myrating, otherrating
    #update['expected'] = 1.0 / (1 + 10.0 ** ((myrating - otherrating) / 400.0))
    #delta = int(round(15 * (update['result'] - expected[update['team']])))
    playerid = playermap[stat['clientid']]
    ratings[playerid]['rating'] += delta[stat['team']]
    ratings[playerid]['games'] += 1

if __name__ == '__main__':
  trueskill.TrueSkill(backend='mpmath').make_as_global()
  db = MongoClient("mongodb").demos
  #demodb = db.demos
  matchdb = db.minmatches
  playerdb = db.players
  playergamedb = db.playerGames
  #matches = list(matchdb.find({'is_match': True}))
  startdate = datetime(2013, 9, 1)
  # check for usage of index rather than full row scan.
  # since $exists cannot be indexed, index is on is_match = true which should have majority matching.
  # db.playerGames.find({'is_match': true, 'rating.updated.raw.sigma': {'$exists': true}}, {'time':1}).sort({'time':-1}).explain()
  lastgame = [g for g in playergamedb.find({'is_match': True, 'rating.updated.raw.sigma': {'$exists': True}}, {'time': 1}).sort('time', pymongo.DESCENDING).limit(1)]
  if len(lastgame) > 0:
    print 'Last elo run updated up to', lastgame[0]['time']
    startdate = lastgame[0]['time']
  #startdate = datetime(2020, 2, 16)
  #startdate = startdate - timedelta(seconds=5)
  #startdate = datetime(2020, 5, 17, 22, 30, 50)
  i = 0
  while True:
    matches = matchdb.find({'ma': True, 't': {'$gt': startdate}}).sort('t', 1)#.limit(1000)
    #matches = matchdb.find({'_id': '147ce9b3dbfa58c5'}).sort('t', 1)
    try:
      for match in matches:
        i += 1
        print 'Processing match', i, match['_id']
        print match['t']
        startdate = match['t']
        updaterank(match)
      break
    except:
      print traceback.format_exc()
  sortedratings = [rating for rating in ratings.iteritems() if rating[1]['games'] > 46]  # trueskill website says 46 game minimum for 4:4 games
  sortedratings = sorted(sortedratings, key = lambda entry: trueskill.expose(entry[1]['rating']), reverse=True)
  
  print 'Ratings:'
  for entry in sortedratings[0:80]:
    player = None
    cursor = playerdb.find({'_id': entry[0]})
    for row in cursor:
      player = row
    print strip_colors(player['names'][0]['name']).encode('utf8'), entry[0], trueskill.expose(entry[1]['rating']), entry[1]
