#!/usr/bin/python
# -*- coding: utf-8 -*-
# version of merge_matches which uses prediction to generate any missing povs
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
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
  db = MongoClient().demos
  demodb = db.demos
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
    demodata = demodb.find({'_id':'/cygdrive/U/demos/' + demo['id']})[0]
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
    client = demo['c']
    demodata = {'_id': '/cygdrive/U/demos/' + demo['id'], 'metadata': demo['meta']}
    if client in demometabyid and demometabyid[client]['metadata']['maps'][demosbyid[client][0]]['scores']['is_final'] == 1:
      continue
    demometabyid[client] = demodata
    mapidx = demo['mapidx']
    map = demodata['metadata']['maps'][mapidx]
    demosbyid[client] = (mapidx, demo['id'].replace('/cygdrive/U/', 'U:/').replace('/cygdrive/C/', 'C:/'))
    for event in map['ctfevents']:
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
    for frag in map['frags']:
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
  
  finalstats = []
  for clientid, stat in stats.iteritems():
    if stat['lasttouchtime'] != -1:
      stat['holdtime'] += mapend - stat['lasttouchtime']
    del stat['lasttouchtime']
    democlientid = clientid
    if democlientid not in demometabyid:
      #print 'Missing client', democlientid, 'in match', match['_id']
      democlientid = demometabyid.keys()[0]
    demometa = demometabyid[democlientid]['metadata']
    teamtime = {'RED': 0, 'BLUE': 0}
    mapidx = demosbyid[democlientid][0]
    for team in demometa['maps'][mapidx]['teams']['%d' % clientid]:
      if team['team'] in teamtime:
        teamtime[team['team']] += team['team_end_time'] - team['team_start_time']
    stat['playtime'] = teamtime['RED'] + teamtime['BLUE']
    if stat['playtime'] == 0:
      continue
    if teamtime['RED'] > teamtime['BLUE']:
      team = 'RED'
    else:
      team = 'BLUE'
    stat['team'] = team
    names = {}
    for name in demometa['maps'][mapidx]['names']['%d' % clientid]:
      nametime = name['name_end_time'] - name['name_start_time']
      if name['name'] not in names:
        names[name['name']] = 0
      names[name['name']] += nametime
    maxname = {'name': '', 'time': -1}
    for name, time in names.iteritems():
      if time > maxname['time']:
        maxname = {'name': name, 'time': time}
    stat['name'] = maxname['name']#strip_html(strip_colors(maxname['name']))
    #print clientid, stat
    finalstats.append(stat)
  
  return (finalstats, scores)

aliases = {
  u'^01^3.^001^3|^0Corr^3µ^0ptor Zer^30': 'onasi',
  u'^0Corr^6µ^0ptor Zer^60': 'onasi',
  u'^0R.I.P. HUN': 'onasi',
  u'^2Gleskevier': 'onasi',
  u'^0hk^3\'^0\xf8nas^3i': 'onasi',
  u'^3^^0\xf8nas^3i': 'onasi',
  u'^4j^5a^7w^44': 'jawa',
  u'jaw^24': 'jawa',
  u'jaw^14': 'jawa',
  u'jaw^34': 'jawa',
  u'jaw^54': 'jawa',
  u'^1^^0w^7horacle^1\xbf^7jaw4': 'jawa',
  u'icecold^6.': 'icecold',
  u'icecold^5|^6|': 'icecold',
  u'icecold^6': 'icecold',
  u'icecold^5.': 'icecold',
  u'icecold^3.': 'icecold',
  u'ice^5c^7old^6|^0|^6': 'icecold',
  u'^7J^4a^7X^4`': 'jax',
  u'^7JaX^5``': 'jax',
  u'^4TENSOR IS TENACIOUS': 'tensor',
  u'^1\'^7nd^1.^7tens0r^1\'': 'tensor',
  u'easing rapod': 'alpha',
  u'^4DSbr^5-^7Clut^5c^4h': 'max',
  u'dsBR^4_^7clutch': 'max',
  u'dsBR^5_^7clutch': 'max',
  u'Clut^5c^4h': 'max',
  u'^4DSbr^5-^7clut^5c^4h': 'max',
  u'^7DSbr^1_^7pete': 'pete',
  u'^7dsBR^1_^7pete': 'pete',
  u'^6^4DSbr^5-^7Clut^5c^4h': 'pete',
  u'deltaaa': 'delta',
  u'sid^6': 'Cydonis',
  u'g^13^7nzu': 'Cydonis',
  u'sid(base)': 'Cydonis',
  u'CYD': 'Cydonis',
  u'^4CYD': 'Cydonis',
  u'^0Golg^1o^0tha^1.': 'Cydonis',
  u'g^53^7nzu': 'Cydonis',
  u'CYD BASE ONLY': 'Cydonis',
  u'^0SID': 'Cydonis',
  u'^7C^0y^7d^0~': 'Cydonis',
  u'^4dsBR^7_genZ^1': 'Cydonis',
  u'KING MAKRUH': 'Cydonis',
  u'cyd, perpetual nervouist': 'Cydonis',
  u'CYD BASE ONLY (for real)': 'Cydonis',
  u'Kafir^5_^7cy^5d': 'Cydonis',
  u'SID': 'Cydonis',
  u'cy^5d': 'Cydonis',
  u'cydonis': 'Cydonis',
  u'^^0<^5= ^7(cyd)': 'Cydonis',
  u'M^64^7KRUH': 'Cydonis',
  u'cydonis^1.': 'Cydonis',
  u'SID NOT CHASE': 'Cydonis',
  u'levi^5.': 'Cydonis',
  u'KHAL CYDONIS': 'Cydonis',
  u'The North Remembers': 'Cydonis',
  u'^4dsbr^7_genZ^1': 'Cydonis',
  u'^1C^3y^7donis': 'Cydonis',
  u'sid': 'Cydonis',
  u'^4DSbr^7_genZ^1': 'Cydonis',
  u'^1DSbr^3.^7sid': 'Cydonis',
  u'Cydonis (D-only)': 'Cydonis',
  u'cyd(base)': 'Cydonis',
  u'CYD D ONLY': 'Cydonis',
  u'cyd': 'Cydonis',
  u'^7>>^4Ç^4e^7a^4s^4a^4r^7>>': 'Ceasar',
  u'^1Ç^2e^3a^1s^2a^3r': 'Ceasar',
  u'^4cea^55sr': 'Ceasar',
  u'DJ Not Nice': 'Ceasar',
  u'^7>>-^1R^7ôbin^1H^7ôôd->': 'Ceasar',
  u'FWWM//Ultimate Overlord Unc Padawan': 'teh',
  u'teh@work': 'teh',
  u'§^4pä^7mb^4õt^7§^4»^7teh': 'teh',
  u'^7^^0ghetto^7teh 1337': 'teh',
  u'Æ^1\'^7teh': 'teh',
  u'^^0ghetto^7teh 1337': 'teh',
  u'bro^2/^3/^4/^7teh': 'teh',
  u'^1/^7n^1/^3so^7^4|^7teh': 'teh',
  u'^1^^0Sy^1l^0ar': 'kimble',
  u'^1^^7K^1!^7mBL^1e': 'kimble',
  u'TheK^1!^7mBLe': 'kimble',
  u'^1^^0ghetto^7K^1!^7mBL^1e': 'kimble',
  u'r^4d^7l': 'radle',
  u'rdl': 'radle',
  u'red': 'radle',
  u'^1«^0hB^1»^0Son^7iC^1+': 'mikeL',
  u'mike^4L': 'mikeL',
  u'm^4k^7L^4«': 'mikeL',
}
ratings = {}

def updaterank(match):
  global ratings
  stats, scores = mergematchmeta(match)
  result = {}
  if scores['bs'] > scores['rs']:
    winner = 'BLUE'
    result = {'RED': 0, 'BLUE': 1}
  elif scores['bs'] == scores['rs']:
    winner = 'TIE'
    result = {'RED': 0.5, 'BLUE': 0.5}
  else:
    winner = 'RED'
    result = {'RED': 1, 'BLUE': 0}
  winners = []
  losers = []
  teamrating = {'RED': {'rating': 0, 'count': 0.0}, 'BLUE': {'rating': 0, 'count': 0.0}}
  for stat in stats:
    if stat['name'] in aliases:
      stat['name'] = aliases[stat['name']]
    if stat['name'] not in ratings:
      ratings[stat['name']] = {'rating': 1000, 'games': 0}
    #update['rating'] = ratings[stat['name']]['rating']
    teamrating[stat['team']]['rating'] += ratings[stat['name']]['rating']
    teamrating[stat['team']]['count'] += 1
  for team in teamrating.keys():
    teamrating[team] = teamrating[team]['rating'] / teamrating[team]['count']
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
    ratings[stat['name']]['rating'] += delta[stat['team']]
    ratings[stat['name']]['games'] += 1

if __name__ == '__main__':
  db = MongoClient().demos
  demodb = db.demos
  matchdb = db.minmatches
  #matches = list(matchdb.find({'is_match': True}))
  id = '3d63ff946d09b50c'
  if len(sys.argv) > 1:
    id = sys.argv[1]
  #matches = list(matchdb.find({'_id': '24061e497b5ba53c951c3374e7c142ec59fe3b1f72cd0b925fe9bc7f833c4c1dd7d98bf74cbad8d609960da9a25983475349d85b3f0f663058e1e70ae483ada8'}))
  #matches = list(matchdb.find({'_id': 'ee4c29f3b73cd86631f182c4ec9cc419bacca8eedfb146afe9f805021576e99b31111b1c463e1a3b3c6ec5691cb541392e5eefdbb5ee35067c98040b572b579d'}))
  #matches = list(matchdb.find({'_id': 'b9636735cc3ac2b09155cbe323cda1ffd1df6855d304c258df9ef6588a09f5fa0b953bcd7c72badc8f34636fb4c70028a68169af87f6bf493a9001fc7f80d7eb'}))
  #matches = list(matchdb.find({'_id': '39dd8a23965328658041d9df2510a2aeb905ebdbad71bc1662d0044d56d7b4db487f52c34aaf24808ef2d2b92ea599ceecc0a6c8865e46dd5fb985b1ddc154f3'}))
  #matches = list(matchdb.find({'_id': '958c5945124f7224720f2041f831b26c9b66c01ab8bd0eb775f34eaf8ff8444dadfeffccef6275d4841825ebec2993805885eabf3a22dc271638fa2a4b9bec80'}))
  matches = matchdb.find({'ma': True}).sort('t', 1)#.limit(2000)
  i = 0
  for match in matches:
    i += 1
    print 'Processing match', i, match['_id']
    updaterank(match)
  sortedratings = ratings.iteritems()
  sortedratings = sorted(sortedratings, key = lambda entry: entry[1]['rating'], reverse=True)
  
  print 'Ratings:'
  for entry in sortedratings[0:20]:
    print entry[0], entry[1]
