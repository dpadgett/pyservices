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

import shrinker
import copy

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

# trim the fat of mergematchmeta so it runs fast for elo bot

ratings = {}

def updaterank(match):
  global ratings
  # first see if we even know the players in this match
  db = MongoClient("mongodb").demos
  playergamedb = db.playerGames
  sessionplayerdb = db.sessionPlayers
  sessiondb = db.sessions
  sessiongamedb = db.sessionGames
  ratingdb = db.playerGameRatings
  sessiongames = [sessiongame for sessiongame in sessiongamedb.find({'_id.match': match['_id'], 'is_match': True})]
  if len(sessiongames) == 0:
    print 'No players found'
    return
  playermap = {}
  playergamemap = {}
  for session in sessiondb.find({'_id': {'$in': [sessiongame['_id']['session'] for sessiongame in sessiongames]}}):
    if 'playerid' not in session:
      print 'Unknown player for session', session['_id']
      exit()
      return
    playermap[json.dumps(session['_id'])] = session['playerid']
    for idx, sessiongame in enumerate(sessiongames):
      if sessiongame['_id']['session'] != session['_id']:
        continue
      if session['playerid'] in playergamemap:
        # could be that they reconnected with a new IP.  just merge the 2 sessiongames
        print 'Multiple sessions in game for same player', session['playerid']
        print [n['name'] for g in sessiongame['games'] for n in g['names']]
        print [n['name'] for g in [s for s in sessiongames if s['_id'] == playergamemap[session['playerid']]][0]['games'] for n in g['names']]
        print [(g['ip'], g['guid'], g.get('newmod_id', None)) for g in sessiongame['games']]
        print [(g['ip'], g['guid'], g.get('newmod_id', None)) for s in sessiongames if s['_id'] == playergamemap[session['playerid']] for g in s['games']]
        #print sessiongame
        #print [s for s in sessiongames if s['_id'] == playergamemap[session['playerid']]][0]
        matchingsessiongame = [s for s in sessiongames if s['_id'] == playergamemap[session['playerid']]][0]
        matchingsessiongame['games'] += sessiongame['games']
        del sessiongames[idx]
        #continue
        sessiongame = matchingsessiongame
        #exit()
        #print [s for s in sessiongames if s['_id'] == playergamemap[session['playerid']]][0]
        #return
      # sanity check.  if there are overlapping times now, then there were actually different players
      allteams = [team for game in sessiongame['games'] for team in game['teams'] if team['team'] in ['RED', 'BLUE']]
      for idx, team in enumerate(allteams):
        for idx2, t2 in enumerate(allteams[idx+1:]):
          if t2['start'] <= team['end'] and team['start'] <= t2['end']:
            print 'Found overlapping team times', team, t2
            #exit()
            return
      playergamemap[session['playerid']] = sessiongame['_id']
  match = shrinker.inflate_match(copy.deepcopy(match))
  #exit(1)
  for scores in match['scores']:
    if scores['is_final']:
      break
  if not scores['is_final']:
    print "Couldn't find final scores"
    exit()
    return
  maptime = match['map_end_time'] - match['map_start_time']
  result = {}
  if scores['blue_score'] > scores['red_score']:
    winner = 'BLUE'
    result = {'RED': 1, 'BLUE': 0}
  elif scores['blue_score'] == scores['red_score']:
    winner = 'TIE'
    result = {'RED': 0, 'BLUE': 0}
  else:
    winner = 'RED'
    result = {'RED': 0, 'BLUE': 1}
  winners = []
  losers = []
  teamrating = {'RED': {}, 'BLUE': {}}
  teamtime = {'RED': {}, 'BLUE': {}}
  for sessiongame in sessiongames:
    if json.dumps(sessiongame['_id']['session']) not in playermap:
      print 'Unknown player %s in match %s' % (sessiongame['_id']['session'], match['_id'])
      exit()
      return

    teams = set([team['team'] for game in sessiongame['games'] for team in game['teams'] if team['team'] in ['RED', 'BLUE']])
    if len(teams) != 1:
      print 'Player played on different teams:', sessiongame['_id']['session'], ':', teams
      print sessiongame['games']
      #exit()
      return
    playerteam = teams.pop()
    playtime = sum([team['end'] - team['start'] for game in sessiongame['games'] for team in game['teams'] if team['team'] == playerteam])
    playerid = playermap[json.dumps(sessiongame['_id']['session'])]
    if playerid not in ratings:
      # first check database
      playerratings = ratingdb.find({'_id.player': playerid, 'time': {'$lt': match['time_created']}}).sort('time', pymongo.DESCENDING).limit(1)
      rating = None
      for rating in playerratings:
        break
      if rating != None:
        ratings[playerid] = {'rating': trueskill.Rating(mu=rating['rating']['updated']['raw']['mu'], sigma=rating['rating']['updated']['raw']['sigma']), 'games': rating['num_games']}
      else:
        # check old ratings
        playergames = playergamedb.find({'_id.player': playerid, 'time': {'$lt': match['time_created']}, 'rating.updated.raw.sigma': {'$exists': True}}).sort('time', pymongo.DESCENDING).limit(1)
        playergame = None
        for playergame in playergames:
          break
        if playergame != None:
          numgames = playergamedb.find({'_id.player': playerid, 'time': {'$lt': match['time_created']}, 'rating.updated.raw.sigma': {'$exists': True}}).count()
          ratings[playerid] = {'rating': trueskill.Rating(mu=playergame['rating']['updated']['raw']['mu'], sigma=playergame['rating']['updated']['raw']['sigma']), 'games': numgames}
        else:
          ratings[playerid] = {'rating': trueskill.Rating(), 'games': 0}

    #update['rating'] = ratings[playerid]['rating']
    rating = ratings[playerid]['rating']
    teamrating[playerteam][playerid] = rating
    teamtime[playerteam][playerid] = playtime
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
  ratingbulk = ratingdb.initialize_unordered_bulk_op()
  pbulk = sessionplayerdb.initialize_unordered_bulk_op()
  for teamupdate in updates:
    for playerid, update in teamupdate.iteritems():
      #playergames = playergamedb.find({'_id.player': playerid, '_id.match': match['_id']})
      #for playergame in playergames:
      #  break
      sessiongame = playergamemap[playerid]
      start = ratings[playerid]['rating']
      def ratingObj(rating):
        return {'raw': {'mu': rating.mu, 'sigma': rating.sigma}, 'friendly': trueskill.expose(rating)}
      ratingbulk.find({'_id': {'player': playerid, 'match': sessiongame['match']}}).upsert().update_one({
        '$set': {
          'rating': {'start': ratingObj(start), 'updated': ratingObj(update)},
          'num_games': ratings[playerid]['games'] + 1,
          'time': match['time_created']
        }})
      pbulk.find({'_id': playerid}).update_one({'$set': {'rating': ratingObj(update)}})
      ratings[playerid]['rating'] = update
      ratings[playerid]['games'] += 1
  print ratingbulk.execute()
  print pbulk.execute()
  return

if __name__ == '__main__':
  trueskill.TrueSkill(backend='mpmath').make_as_global()
  db = MongoClient("mongodb").demos
  #demodb = db.demos
  matchdb = db.minmatches
  playerdb = db.players
  playergamedb = db.playerGames
  ratingdb = db.playerGameRatings
  sessiongamedb = db.sessionGames
  sessionplayerdb = db.sessionPlayers
  
  '''
  ratingdb.drop()
  ratingdb.create_index([('time',1)])
  ratingdb.create_index([('_id.player',1), ('time',-1)])
  sessionplayerdb.update_many({'rating': {'$exists': True}}, {'$unset': {'rating':1}})
  '''
  
  #matches = list(matchdb.find({'is_match': True}))
  #startdate = datetime(2013, 9, 1)
  startdate = [g for g in sessiongamedb.find({'is_match': True}, {'time': 1}).sort('time', pymongo.ASCENDING).limit(1)][0]['time'] - timedelta(seconds=1)
  # check for usage of index rather than full row scan.
  # since $exists cannot be indexed, index is on is_match = true which should have majority matching.
  # db.playerGames.find({'is_match': true, 'rating.updated.raw.sigma': {'$exists': true}}, {'time':1}).sort({'time':-1}).explain()
  lastgame = [g for g in ratingdb.find({}, {'time': 1}).sort('time', pymongo.DESCENDING).limit(1)]
  if len(lastgame) > 0:
    print 'Last elo run updated up to', lastgame[0]['time']
    startdate = lastgame[0]['time']
  #startdate = datetime(2020, 2, 16)
  #startdate = startdate - timedelta(seconds=5)
  #startdate = datetime(2020, 5, 17, 22, 30, 50)
  i = 0
  while True:
    matches = matchdb.find({'ma': True, 't': {'$gt': startdate}}).sort('t', 1)#.limit(1000)
    #matches = matchdb.find({'_id': '038ac62eeb06d6a1'}).sort('t', 1)
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
      exit()
  sortedratings = [rating for rating in ratings.iteritems() if rating[1]['games'] > 46]  # trueskill website says 46 game minimum for 4:4 games
  sortedratings = sorted(sortedratings, key = lambda entry: trueskill.expose(entry[1]['rating']), reverse=True)
  
  print 'Ratings:'
  for entry in sortedratings[0:80]:
    player = None
    cursor = sessionplayerdb.find({'_id': entry[0]})
    for row in cursor:
      player = row
    print strip_colors(player['name'] if 'name' in player else player['last_name']).encode('utf8'), entry[0], trueskill.expose(entry[1]['rating']), entry[1]
