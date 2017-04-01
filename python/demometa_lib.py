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
#import demodb_lib

tz_mapping = {
  '/whoracle': timezone('CET'),
  '/whoracle2': timezone('CET'),
  '/whoracle3': timezone('CET'),
  '/europug': timezone('CET'),
  '/sylar': timezone('CET'),
  '/sith': timezone('CET'),
  '/demon': timezone('CET'),
  '/bra': timezone('UTC'),
  '/demobot': timezone('US/Pacific'),
  '/pug': timezone('US/Eastern'),
  '/japlus': timezone('US/Eastern'),
  '/west_coast_pug': timezone('US/Eastern'),
  '/akl': timezone('US/Eastern'),
}

def timezone_for_demo( demo ):
  """
  Returns the timezone corresponding to the server which generated the given demo path or dir.
  """
  global tz_mapping
  tzone = timezone('US/Eastern')
  for dir, tz in tz_mapping.iteritems():
    if (demo.find(dir + '/') != -1):
      tzone = tz
      break
  print 'Using timezone:', tzone
  return tzone

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
