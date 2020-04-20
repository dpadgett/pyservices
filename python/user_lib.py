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

def update_player(player):
  player['ip_hash'] = sorted(player['ip_hash'], key=lambda x: x['time'], reverse=True)[:5]
  player['num_ips'] = len(player['ip_hash'])
  player['guid_hash'] = sorted(player['guid_hash'], key=lambda x: x['time'], reverse=True)[:5]
  player['num_guids'] = len(player['guid_hash'])
  player['names'] = sorted(player['names'], key=lambda x: x['time'], reverse=True)[:5]
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

def recreate_player(playerid, playergames):
  player = {'_id': playerid, 'names': [], 'ip_hash': [], 'guid_hash': [], 'num_games': 0, 'num_matches': 0}
  playergames.sort(lambda x, y: -int((x['time'] - y['time']).total_seconds()))
  player['time'] = 0
  for summary in playergames:
    player['num_games'] += 1
    if summary['is_match']:
      player['num_matches'] += 1
    had_ip = False
    had_guid = False
    for name in summary['names']:
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
      player_name = find_player_property(player['names'], 'name', name['name'])
      player_name['time'] += name['name_end_time'] - name['name_start_time']
      player['time'] += name['name_end_time'] - name['name_start_time']
    if 'rating' in summary and 'rating' not in player:
      player['rating'] = summary['rating']['updated']
    update_player(player)
  return player
