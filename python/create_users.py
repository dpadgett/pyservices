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
import traceback

import merge_metadata
from bson.objectid import ObjectId

import shrinker

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

def hashFor(data):
  hasher = hashlib.sha1()
  hasher.update(data)
  return struct.unpack('!i', hasher.digest()[:4])[0]

def hashForIp(ip):
  pieces = [int(num) for num in ip.split('.')]
  packedip = struct.pack('BBBB', pieces[3], pieces[2], pieces[1], pieces[0])
  return hashFor(packedip)

def ipIsBlacklisted(ip):
  return ip == hashForIp('0.0.0.0') or ip == hashFor('') or ip == 0 or ip == -1 or ip == hashForIp('104.239.162.135') or ip == hashForIp('37.187.199.239')

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

def getPlayers(names, startkey, endkey):
  clients = []
  for clientid, names in names.iteritems():
    client = None
    for name in names:
      if client == None or client['end'] != name[startkey]:
        if client != None:
          clients.append(client)
        client = {'clientid': clientid, 'start': name[startkey], 'end': name[endkey]}
      else:
        client['end'] = name[endkey]
    if client != None:
      clients.append(client)
  return clients

if __name__ == '__main__':
  db = MongoClient("mongodb").demos
  #demodb = db.demos
  mindemodb = db.mindemos
  matchdb = db.minmatches
  ipdb = db.minips
  playerdb = db.players
  playergamedb = db.playerGames
  basedir = u'/cygdrive/U/demos/'
  
  def update_player(player):
    player['ip_hash'] = sorted(player['ip_hash'], key=lambda x: x['time'], reverse=True)
    player['num_ips'] = len(player['ip_hash'])
    player['guid_hash'] = sorted(player['guid_hash'], key=lambda x: x['time'], reverse=True)
    player['num_guids'] = len(player['guid_hash'])
    player['names'] = sorted(player['names'], key=lambda x: x['time'], reverse=True)
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
    for playergame in [p for p in playergames]:  # materialize all playergames first since we are deleting rows underneath the cursor
      playergamedb.remove({'_id': playergame['_id']})
      playergame['_id']['player'] = player1['_id']
      playergamedb.save(playergame)
      player1['num_games'] += 1
      if playergame['is_match']:
        player1['num_matches'] += 1
    if 'names' in player2:
      for name in player2['names']:
        player_name = find_player_property(player1['names'], 'name', name['name'])
        player_name['time'] += name['time']
    update_player(player1)
    playerdb.remove(player2['_id'])
    playerdb.save(player1)
  
  # use the following to correct any merge failures
  '''playerid = ObjectId('55dc565fcb15c73790d8833a')
  player1 = playerdb.find({'_id': playerid}).next()
  player2 = {'_id': ObjectId('56dbedc6cb15c70c842cc208')}
  mergePlayers(player1, player2)
  exit()'''
  
  '''playerid = ObjectId('55dc3d0acb15c73790d832d7')
  playerid2 = ObjectId('56f26d3fcb15c73648190fc3')
  player1 = playerdb.find({'_id': playerid}).next()
  player2 = playerdb.find({'_id': playerid2}).next()
  mergePlayers(player1, player2)
  exit()'''

  # filter out any that are too old to have ip data recorded
  #start = datetime.datetime(2014, 1, 1)
  #start = datetime.datetime(2016, 3, 21)
  start = parse(commands.getoutput("/bin/date") + ' -0800') + relativedelta(hours=-3)#days=-3)
  guid_start = datetime.datetime(2015, 8, 3)
  #start = guid_start
  #start = datetime.datetime(2017, 1, 4)
  if len(sys.argv) > 1:
    matches = matchdb.find({'_id': {'$in': sys.argv[1:]}}).sort('t', pymongo.ASCENDING).skip(0).batch_size(30)
  else:
    matches = matchdb.find({'t': {'$gt': start}}).sort('t', pymongo.ASCENDING).skip(0).batch_size(30)
  num_matches = 0
  for match in matches:
    print num_matches, match['_id'], match['t']
    num_matches += 1
    # first, check for any ip/guid data recorded in the demo metadata
    demos = []
    demonames = []
    for matchdemo in match['d']:
      ips = []
      guids = []
      #demodatas = demodb.find({'_id': basedir + matchdemo['id']}, {
      #  'metadata.client': 1,
      #  'metadata.version': 1,
      #  'metadata.maps.names': 1,
      #  'metadata.maps.serverId': 1,
      #  'metadata.maps.checksumFeed': 1,
      #  'metadata.maps.map_start_time': 1,
      #  'metadata.maps.map_end_time': 1})
      demodatas = mindemodb.find({'_id': matchdemo['id']}, {
        'm.c': 1,
        'm.v': 1,
        'm.m.na': 1,
        'm.m.h': 1,
        'm.m.s': 1,
        'm.m.e': 1})
      demo = None
      for demo in demodatas:
        demo = shrinker.inflate(demo)
        break
      if demo == None:
        print 'Unknown demo', matchdemo['id']
        continue
      demos.append(demo['metadata'])
      demonames.append(matchdemo['id'])
    names = merge_metadata.merge_history(demos, match['_id'], 'name')
    import json
    #print json.dumps(names, indent=2)
    #print json.dumps(demonames, indent=2)
    minversion = min([d['version'] for d in demos])
    if minversion < 4:
      # no raw times then
      print 'old version', minversion
      print (u'\n'.join([demonames[idx] for idx, d in enumerate(demos) if d['version'] == minversion])).encode('utf8')
      start = 'name_start_time'
      end = 'name_end_time'
    else:
      start = 'name_start_time_raw'
      end = 'name_end_time_raw'

    import json
    #print json.dumps(demos, sort_keys=True, indent=2, separators=(',', ': '))
    #print json.dumps(names, sort_keys=True, indent=2, separators=(',', ': '))
    #print json.dumps(getPlayers(names), sort_keys=True, indent=2, separators=(',', ': '))
    
    players = getPlayers(names, start, end)
    #exit()
    #for matchdemo in match['d']:
    for client in players:
      ips = []
      guids = []
      '''demo = demodb.find({'_id': basedir + matchdemo['id']}, {
        'metadata.maps.names': 1,
        'metadata.maps.serverId': 1,
        'metadata.maps.checksumFeed': 1,
        'metadata.maps.map_start_time': 1,
        'metadata.maps.map_end_time': 1})[0]
      map = findmap(demo['metadata']['maps'], match['_id'])[1]
      client_num = '%d' % (matchdemo['c'])'''
      client_num = client['clientid']
      filtered_names = []
      if client_num in names:
        for name in names[client_num]:
          if name[start] >= client['start'] and name[end] <= client['end']:
            filtered_names.append(name)
      map = {'names': names, 'map_start_time': filtered_names[0]['name_start_time'], 'map_end_time': filtered_names[-1]['name_end_time']}
      has_names = match['t'] > guid_start and len(filtered_names) > 0
      if has_names:
        for name in filtered_names:
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
            if ipIsBlacklisted(ip):
              #print 'Player has zero IP!:', client_num, demo['_id']
              pass
            else:
              ips.append(ip)
      #continue
      # due to an early bug, some ip hashes were computed with an incorrect zero ip
      # these should be fixable by using the side input table
      if len(ips) == 0:
        # search for the demo this player had, if any
        demoidx = None
        for idx, demo in enumerate(demos):
          if demo['client']['id'] != int(client_num):
            continue
          amapidx, amap = findmap(demo['maps'], match['_id'])
          if 'names' in amap and client_num in amap['names']:
            namelist = amap['names'][client_num]
            if len(namelist) > 0:
              namestart = namelist[0]['name_start_time']
              if namestart >= client['start'] and namestart < client['end']:
                demoidx = idx
                break
        if demoidx != None:
          demoname = demonames[demoidx]
          #dbips = ipdb.find({'_id': matchdemo['id']})
          dbips = ipdb.find({'_id': demoname})
          for an_ip in dbips:
            ips.append(an_ip['i'])
      #print client_num, guids, ips
      if len(guids) + len(ips) > 0:
        # we got some valid data, look up player
        ip_matches = []
        guid_matches = []
        if len(ips) > 0:
          ip_matches = [p for p in playerdb.find({'ip_hash.ip': {'$in': ips}})]
        if len(guids) > 0:
          guid_matches = [p for p in playerdb.find({'guid_hash.guid': {'$in': guids}})]
        #print client_num, len(guid_matches), len(ip_matches)
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
            print 'found multiple guid matches for %s! %s Player: %s %s' % (guids, [p['_id'] for p in guid_matches], filtered_names, client)
            #exit(1)
          player = guid_matches[0]
          # skip automerging for now as it sometimes creates issues :?
          #for matches in [guid_matches[1:], [pl for pl in ip_matches if pl['guid_hash'] == []]]:
          #  for player2 in matches:
          #    if player2['_id'] != player['_id']:
          #      mergePlayers(player, player2)
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
            #if summary['client_num'] == matchdemo['c']:
            # cant use above since player might have reconnected under a different client_num
            # but we can't store 2 playergames for them
            print 'Player already has match'
            dirty = False
            if summary['time'] != match['t']:
              summary['time'] = match['t']
              dirty = True
            if summary['is_match'] != match['ma']:
              if summary['is_match']:
                player['num_matches'] -= 1
              summary['is_match'] = match['ma']
              if summary['is_match']:
                player['num_matches'] += 1
              playerdb.save(player)
              dirty = True
            if summary['names'] != filtered_names:
              summary['names'] = filtered_names
              dirty = True
            if dirty:
              print 'Updated existing playergame', player['_id'], match['_id']
              playergamedb.save(summary)
            had_match = True
            break
        if had_match:
          continue
        summary = {'_id': {'player': '', 'match': match['_id']},
          'time': match['t'],
          'is_match': match['ma'],
          'client_num': int(client_num),
          'names': [],
          'map': match['n'],
          'score': getScore(match, int(client_num))}
        summary['names'] = filtered_names
        if 'matches' in player:
          del player['matches'] #.append(summary)
        player['num_games'] += 1
        if summary['is_match']:
          player['num_matches'] += 1
        had_ip = False
        had_guid = False
        if has_names:
          for name in filtered_names:
            if 'ip_hash' in name:
              ip = name['ip_hash']
              if ipIsBlacklisted(ip):
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
        for name in filtered_names:
          player_name = find_player_property(player['names'], 'name', name['name'])
          player_name['time'] += name['name_end_time'] - name['name_start_time']
        update_player(player)
        playerid = playerdb.save(player)
        summary['_id']['player'] = playerid
        playergamedb.save(summary)
