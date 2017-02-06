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

import datetime

import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
import traceback

from bson.objectid import ObjectId

def map_match_hash( map ):
  if 'match_hash' in map:
    return map['match_hash']
  # compute a hash to identify this match
  match_id = ((map['serverId'] & 0xFFFFFFFF) << 32) + (map['checksumFeed'] & 0xFFFFFFFF)
  match_hash = struct.pack('!Q', match_id).encode('hex')
  return match_hash

def findmap(maps, match_hash):
  longestmapidx = 0
  longestmap = maps[0]
  for idx, map in enumerate(maps):
    if map_match_hash(map) == match_hash:
      return (idx, map)
  raise Exception("Couldn't find map with hash %s" % (match_hash))

def rekey(data, keymap):
  for min_key, key in keymap.iteritems():
    if min_key in data:
      data[key] = data[min_key]
      del data[min_key]

def inflate_scores(min_scores):
  rekey(min_scores, {
    'fi': 'is_final',
    'rs': 'red_score',
    'bs': 'blue_score',
    'f': 'freeplayers',
    'r': 'redplayers',
    'b': 'blueplayers',
    's': 'specplayers'})
  teams = {'freeplayers': 'FREE', 'redplayers': 'RED', 'blueplayers': 'BLUE', 'specplayers': 'SPECTATOR'}
  for team, teamval in teams.iteritems():
    if team in min_scores and min_scores[team] != None:
      for player in min_scores[team]:
        rekey(player, {
          'c': 'client',
          'n': 'client_name',
          's': 'score',
          'p': 'ping',
          't': 'time'})
        player['team'] = teamval

def inflate_name(name):
  rekey(name, {
    'n': 'name',
    'g': 'guid_hash',
    'i': 'ip_hash',
    'b': 'is_bot',
    's': 'name_start_time',
    'e': 'name_end_time'})

def inflate_map(map):
  rekey(map, {
    'n': 'mapname',
    's': 'map_start_time',
    'e': 'map_end_time',
    'sc': 'scores',
    'na': 'names'})
  if 'scores' in map:
    inflate_scores(map['scores'])
  if 'names' in map:
    for name in names:
      inflate_name(name)

def inflate(min_data):
  data = min_data.copy()
  data['_id'] = '/cygdrive/U/demos/' + data['_id']
  rekey(data, {
    'p': 'player',
    'ma': 'is_match',
    'h': 'match_hash',
    't': 'time_created',
    'mt': 'metadata_mtime',
    'm': 'metadata'})
  rekey(data['metadata'], {
    'c': 'client',
    'h': 'sv_hostname',
    'm': 'maps'})
  if 'metadata' in data and 'maps' in data['metadata']:
    for map in data['metadata']['maps']:
      inflate_map(map)
  return data

def hashFor(data):
  hasher = hashlib.sha1()
  hasher.update(data)
  return struct.unpack('!i', hasher.digest()[:4])[0]

def hashForIp(ip):
  pieces = [int(num) for num in ip.split('.')]
  packedip = struct.pack('BBBB', pieces[3], pieces[2], pieces[1], pieces[0])
  return hashFor(packedip)

def getScore(match, client_num):
  scores = None
  for sc in match['sc']:
    scores = sc
    if sc['fi'] != 0:
      break
  team = None
  for teamname, key in [['red', 'r'], ['blue', 'b'], ['spec', 's'], ['free', 'f']]:
    if scores.get(key) == None:
      continue
    for player in scores[key]:
      if player['c'] == client_num:
        team = teamname
        break
  return {'blue': scores.get('bs'), 'red': scores.get('rs'), 'team': team}

if __name__ == '__main__':
  db = MongoClient().demos
  demodb = db.demos
  matchdb = db.minmatches
  ipdb = db.minips
  playerdb = db.players
  playergamedb = db.playerGames
  basedir = u'/cygdrive/U/demos/'
  
  def update_player(player):
    player['ip_hash'] = sorted(player['ip_hash'], key=lambda x: x['time'], reverse=True)
    player['ip_hash'] = [ip for ip in player['ip_hash'] if ip['time'] > 0]
    player['num_ips'] = len(player['ip_hash'])
    player['guid_hash'] = sorted(player['guid_hash'], key=lambda x: x['time'], reverse=True)
    player['guid_hash'] = [guid for guid in player['guid_hash'] if guid['time'] > 0]
    player['num_guids'] = len(player['guid_hash'])
    player['names'] = sorted(player['names'], key=lambda x: x['time'], reverse=True)
    player['names'] = [name for name in player['names'] if name['time'] > 0]
    player['num_names'] = len(player['names'])
    if 'matches' in player:
      del player['matches']
    #player['matches'] = sorted(player['matches'], key=lambda x: x['time'])
    #player['num_games'] = len(player['matches'])
    #player['num_matches'] = len([match for match in player['matches'] if match['is_match']])
  
  def find_player_property(props, prop_name, prop_value):
    for prop in props:
      if prop[prop_name] == prop_value:
        return prop
    prop = {prop_name: prop_value, 'time': 0}
    props.append(prop)
    return prop
  
  def mergePlayers(player1, player2):
    print 'Merging', player1['_id'], 'and', player2['_id']
    if 'ip_hash' in player2:
      for ip in player2['ip_hash']:
        player_ip = find_player_property(player1['ip_hash'], 'ip', ip['ip'])
        player_ip['time'] += ip['time']
    if 'guid_hash' in player2:
      for guid in player2['guid_hash']:
        player_guid = find_player_property(player1['guid_hash'], 'guid', guid['guid'])
        player_guid['time'] += guid['time']
    '''
    for summary2 in player2['matches']:
      had_match = False
      for summary in player1['matches']:
        if summary['id'] == summary2['id'] and summary['client_num'] == summary2['client_num']:
          print 'Player1 already has match'
          had_match = True
          break
      if had_match:
        continue
      player1['matches'].append(summary2)
    '''
    playergames = playergamedb.find({'_id.player': player2['_id']})
    for playergame in playergames:
      #playergamedb.remove(playergame)
      playergame['_id']['player'] = player1['_id']
      #playergamedb.save(playergame)
      player1['num_games'] += 1
      if playergame['is_match']:
        player1['num_matches'] += 1
    if 'names' in player2:
      for name in player2['names']:
        player_name = find_player_property(player1['names'], 'name', name['name'])
        player_name['time'] += name['time']
    update_player(player1)
    #playerdb.remove(player2['_id'])
    #playerdb.save(player1)
  
  ''' Adds stats from the given player game to the given player. '''
  def add_player_game(player, game):
    player['num_games'] += 1
    if game['is_match']:
      player['num_matches'] += 1
    had_ip = False
    had_guid = False
    if 'names' in game:
      for name in game['names']:
        if 'ip_hash' in name:
          ip = name['ip_hash']
          if ip == hashForIp('0.0.0.0') or ip == hashFor('') or ip == 0 or ip == -1 or ip == hashForIp('104.239.162.135') or ip == hashForIp('37.187.199.239'):
            pass
          else:
            player_ip = find_player_property(player['ip_hash'], 'ip', ip)
            player_ip['time'] += name['name_end_time'] - name['name_start_time']
            had_ip = True
        if 'guid_hash' in name:
          guid = name['guid_hash']
          player_guid = find_player_property(player['guid_hash'], 'guid', guid)
          player_guid['time'] += name['name_end_time'] - name['name_start_time']
          had_guid = True
    match = None
    if not had_ip:
      if match == None:
        match = matchdb.find({'_id': game['_id']['match']}).next()
      # due to an early bug, some ip hashes were computed with an incorrect zero ip
      # these should be fixable by using the side input table
      ips = []
      demonames = [d['id'] for d in match['d'] if d['c'] == game['client_num']]
      if len(demonames) == 1:
        demoname = demonames[0]
        #print demoname
        dbips = ipdb.find({'_id': demoname})
        for an_ip in dbips:
          ips.append(an_ip['i'])
        if len(ips) > 0:
          print 'Found ips for demo', demoname
          for ip in ips:
            player_ip = find_player_property(player['ip_hash'], 'ip', ip)
            player_ip['time'] += match['e'] - match['s']
    if len(guids) > 0 and not had_guid:
      if match == None:
        match = matchdb.find({'_id': game['_id']['match']}).next()
      for guid in guids:
        player_guid = find_player_property(player['guid_hash'], 'guid', guid)
        player_guid['time'] += match['e'] - match['s']
    if 'names' in game:
      for name in game['names']:
        player_name = find_player_property(player['names'], 'name', name['name'])
        player_name['time'] += name['name_end_time'] - name['name_start_time']
    update_player(player)

  ''' Creates a new player given a list of all their player games. '''
  def create_player(games):
    player = {'names': [], 'ip_hash': [], 'guid_hash': [], 'num_games': 0, 'num_matches': 0}
    last_game = None
    last_game_elo = None
    for game in games:
      add_player_game(player, game)
      if (last_game == None or game['time'] > last_game) and 'rating' in game:
        last_game = game['time']
        last_game_elo = game['rating']
    update_player(player)
    if last_game_elo != None:
      print 'Last game at', last_game, 'had elo update', last_game_elo
      player['rating'] = last_game_elo['updated']
    return player
  
  playerid = ObjectId('56f8dce6cb15c770e81049ee')
  
  player = playerdb.find({'_id': playerid}).next()
  print len(player['guid_hash']), player['num_guids']
  #start = datetime.datetime(2015, 10, 30)
  #playerGames = [g for g in playergamedb.find({'_id.player': playerid, 'time': {'$gt': start}}).sort('time', pymongo.DESCENDING)]
  playerGames = [g for g in playergamedb.find({'_id.player': playerid}).sort('time', pymongo.DESCENDING)]
  
  #print player
  #print playerGames
  splitgames = []
  keepgames = []
  for game in playerGames:
    match = matchdb.find({'_id': game['_id']['match']}).next()
    demos = [d for d in match['d'] if d['c'] == game['client_num']]
    guids = []
    ips = []
    has_names = 'names' in game
    if has_names:
      for name in game['names']:
        if 'guid_hash' in name:
          #if guid != None and name['guid_hash'] != guid:
          #  print 'Player has different GUIDs!:', client_num, demo['_id']
          guids.append(name['guid_hash'])
        if 'ip_hash' in name:
          ip = name['ip_hash']
          #if ip != None and name['ip_hash'] != ip:
          #  print 'Player has different IPs!:', client_num, demo['_id']
          # skip ips that were set to 0, -1, hash of 0 ip, hash of empty ip, or hash of refresh server ip
          # last is since refresh operated a proxy so some users connected from refresh ip
          if ip == hashForIp('0.0.0.0') or ip == hashFor('') or ip == 0 or ip == -1 or ip == hashForIp('104.239.162.135'):
            #print 'Player has zero IP!:', client_num, demo['_id']
            pass
          else:
            ips.append(ip)
    #print match['_id'], demos, guids, ips
    #if (False and len(guids) > 0 and 691567850 not in guids and -498283481 not in guids and (len(ips) > 0 and -1 not in ips)):
    if (True and (1075779555 in guids or 1272370444 in guids or 1505689827 in ips or -928858578 in ips)):
      #if (len(ips) > 0 and -1 not in ips):
      splitgames.append(game)
      print match['_id'], guids, 'player 2'
    else:
      keepgames.append(game)
      print match['_id'], 'player 1'
  print len(splitgames), len(keepgames)
  newplayer = create_player(splitgames)
  player = create_player(keepgames)
  player['_id'] = playerid
  print len(player['guid_hash']), player['num_guids']
  print len(newplayer['guid_hash']), newplayer['num_guids']
  # save newplayer to get the new id
  if len(splitgames) > 0:
    newplayerid = playerdb.save(newplayer)
    print 'Split players id is', newplayerid
  else:
    print 'Not saving split player since it is empty'
  # update all of the split playergames to have the new id
  for game in splitgames:
    playergamedb.remove({'_id': game['_id']})
    game['_id']['player'] = newplayerid
    playergamedb.save(game)
  # update original player to remove stats from split off games
  playerid = playerdb.save(player)
  print 'Saved original player to', playerid

  #summary['_id']['player'] = playerid
  #playergamedb.save(summary)

  #print newplayer
  #print player['guid_hash']
  sys.exit(0)
  
  # filter out any that are too old to have ip data recorded
  #start = datetime.datetime(2014, 1, 1)
  start = datetime.datetime(2016, 01, 01)
  guid_start = datetime.datetime(2015, 8, 3)
  #start = guid_start
  matches = matchdb.find({'t': {'$gt': start}}).sort('t', pymongo.ASCENDING).skip(0).batch_size(30)
  num_matches = 0
  for match in matches:
    print num_matches, match['_id'], match['t']
    num_matches += 1
    # first, check for any ip/guid data recorded in the demo metadata
    for matchdemo in match['d']:
      ips = []
      guids = []
      demo = demodb.find({'_id': basedir + matchdemo['id']}, {
        'metadata.maps.names': 1,
        'metadata.maps.serverId': 1,
        'metadata.maps.checksumFeed': 1,
        'metadata.maps.map_start_time': 1,
        'metadata.maps.map_end_time': 1})[0]
      map = findmap(demo['metadata']['maps'], match['_id'])[1]
      client_num = '%d' % (matchdemo['c'])
      has_names = match['t'] > guid_start and client_num in map['names']
      if has_names:
        for name in map['names'][client_num]:
          if 'guid_hash' in name:
            #if guid != None and name['guid_hash'] != guid:
            #  print 'Player has different GUIDs!:', client_num, demo['_id']
            guids.append(name['guid_hash'])
          if 'ip_hash' in name:
            ip = name['ip_hash']
            #if ip != None and name['ip_hash'] != ip:
            #  print 'Player has different IPs!:', client_num, demo['_id']
            # skip ips that were set to 0, -1, hash of 0 ip, hash of empty ip, or hash of refresh server ip
            # last is since refresh operated a proxy so some users connected from refresh ip
            if ip == hashForIp('0.0.0.0') or ip == hashFor('') or ip == 0 or ip == -1 or ip == hashForIp('104.239.162.135'):
              #print 'Player has zero IP!:', client_num, demo['_id']
              pass
            else:
              ips.append(ip)
      #continue
      # due to an early bug, some ip hashes were computed with an incorrect zero ip
      # these should be fixable by using the side input table
      if len(ips) == 0:
        dbips = ipdb.find({'_id': matchdemo['id']})
        for an_ip in dbips:
          ips.append(an_ip['i'])
      if len(guids) + len(ips) > 0:
        # we got some valid data, look up player
        ip_matches = []
        guid_matches = []
        if len(ips) > 0:
          ip_matches = [p for p in playerdb.find({'ip_hash.ip': {'$in': ips}})]
        if len(guids) > 0:
          guid_matches = [p for p in playerdb.find({'guid_hash.guid': {'$in': guids}})]
        player = None
        '''
        def find_best_match(matches, key, valuekey, values):
          best_match = None
          best_match_time = -1
          for match in matches:
            match_time = -1
            for match_value in match[key]:
              if match_value[valuekey] in values:
                match_time = match_value['time']
                break
            if match_time > best_match_time:
              best_match = match
              best_match_time = match_time
          return best_match
        '''
        if guid_matches != []:
          if len(guid_matches) > 1:
            print 'found multiple guid matches for %s!' % (guids)
            #exit(1)
          player = guid_matches[0]
          for matches in [guid_matches[1:], [pl for pl in ip_matches if pl['guid_hash'] == []]]:
            for player2 in matches:
              if player2['_id'] != player['_id']:
                mergePlayers(player, player2)
        elif ip_matches != []:
          if len(ip_matches) > 1:
            print 'found multiple ip matches for %s!' % (ips)
            #exit(1)
          best_ip_match = None
          best_ip_match_time = -1
          for ip_match in ip_matches:
            ip_match_time = -1
            for ip_hash in ip_match['ip_hash']:
              if ip_hash['ip'] in ips:
                ip_match_time = ip_hash['time']
                break
            if ip_match_time > best_ip_match_time:
              best_ip_match = ip_match
              best_ip_match_time = ip_match_time
          player = best_ip_match
        else:
          player = {'names': [], 'ip_hash': [], 'guid_hash': [], 'num_games': 0, 'num_matches': 0}
        had_match = False
        if '_id' in player:
          for summary in playergamedb.find({'_id.player': player['_id'], '_id.match': match['_id']}):
            if summary['client_num'] == matchdemo['c']:
              print 'Player already has match'
              had_match = True
              break
        if had_match:
          continue
        summary = {'_id': {'player': '', 'match': match['_id']},
          'time': match['t'],
          'is_match': match['ma'],
          'client_num': matchdemo['c'],
          'names': [],
          'map': match['n'],
          'score': getScore(match, matchdemo['c'])}
        if client_num in map['names']:
          summary['names'] = map['names'][client_num]
        if 'matches' in player:
          del player['matches'] #.append(summary)
        player['num_games'] += 1
        if summary['is_match']:
          player['num_matches'] += 1
        had_ip = False
        had_guid = False
        if has_names:
          for name in map['names'][client_num]:
            if 'ip_hash' in name:
              ip = name['ip_hash']
              if ip == hashForIp('0.0.0.0') or ip == hashFor('') or ip == 0 or ip == -1 or ip == hashForIp('104.239.162.135'):
                pass
              else:
                player_ip = find_player_property(player['ip_hash'], 'ip', ip)
                player_ip['time'] += name['name_end_time'] - name['name_start_time']
                had_ip = True
            if 'guid_hash' in name:
              guid = name['guid_hash']
              player_guid = find_player_property(player['guid_hash'], 'guid', guid)
              player_guid['time'] += name['name_end_time'] - name['name_start_time']
              had_guid = True
        if len(ips) > 0 and not had_ip:
          for ip in ips:
            player_ip = find_player_property(player['ip_hash'], 'ip', ip)
            player_ip['time'] += map['map_end_time'] - map['map_start_time']
        if len(guids) > 0 and not had_guid:
          for guid in guids:
            player_guid = find_player_property(player['guid_hash'], 'guid', guid)
            player_guid['time'] += map['map_end_time'] - map['map_start_time']
        if client_num in map['names']:
          for name in map['names'][client_num]:
            player_name = find_player_property(player['names'], 'name', name['name'])
            player_name['time'] += name['name_end_time'] - name['name_start_time']
        update_player(player)
        #playerid = playerdb.save(player)
        #summary['_id']['player'] = playerid
        #playergamedb.save(summary)
