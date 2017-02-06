import trueskill
import sys
import json
import math

import hashlib
import struct

import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
import traceback

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

def find_player(ip_hash, guid_hash):
  global playerdb
  ip_matches = []
  guid_matches = []
  if not ipIsBlacklisted(ip_hash):
    ip_matches = [p for p in playerdb.find({'ip_hash.ip': {'$in': [ip_hash]}})]
  if guid_hash != -1:
    guid_matches = [p for p in playerdb.find({'guid_hash.guid': {'$in': [guid_hash]}})]
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
    player = guid_matches[0]
  elif ip_matches != []:
    best_ip_match = None
    best_ip_match_time = -1
    for ip_match in ip_matches:
      ip_match_time = -1
      for found_ip_hash in ip_match['ip_hash']:
        if found_ip_hash['ip'] == ip_hash:
          ip_match_time = found_ip_hash['time']
          break
      if ip_match_time > best_ip_match_time:
        best_ip_match = ip_match
        best_ip_match_time = ip_match_time
    player = best_ip_match
  else:
    player = {'names': [], 'ip_hash': [], 'guid_hash': [], 'num_games': 0, 'num_matches': 0}
  return player

if __name__ == '__main__':
  #print 'stdin:', sys.stdin.read()
  request = json.loads(sys.stdin.read())

  #print request

  db = MongoClient().demos
  demodb = db.demos
  matchdb = db.minmatches
  ipdb = db.minips
  playerdb = db.players
  playergamedb = db.playerGames

  result = {}
  if request['blueScore'] > request['redScore']:
    winner = 'BLUE'
    result = {'RED': 1, 'BLUE': 0}
  elif request['blueScore'] == request['redScore']:
    winner = 'TIE'
    result = {'RED': 0, 'BLUE': 0}
  else:
    winner = 'RED'
    result = {'RED': 0, 'BLUE': 1}
  winners = []
  losers = []
  teamrating = {'RED': {}, 'BLUE': {}}
  for team in ['red', 'blue']:
    for client_idx in request['players'][team]:
      player_ids = request['players'][team][client_idx]
      player = find_player(player_ids[0], player_ids[1])
      if 'rating' in player:
        rating = player['rating']
        #print 'player', player_ids, ':', rating
        truerating = trueskill.Rating(mu=rating['raw']['mu'], sigma=rating['raw']['sigma'])
      else:
        #print 'player', player_ids, ': unknown'
        truerating = trueskill.Rating()
      teamrating[team.upper()][client_idx] = truerating
    if len(teamrating[team.upper()]) == 0:
      # it won't work with an empty team so add fake
      teamrating[team.upper()][''] = trueskill.Rating(mu=0, sigma=1)
  
  teamratings = [teamrating[team] for team in ['RED', 'BLUE']]
  #print teamratings
  quality = trueskill.quality(teamratings)
  #print('{:.1%} chance to draw'.format(quality))
  updates = trueskill.rate(teamratings, ranks=[result['RED'], result['BLUE']])
  #print updates
  
  is_match = abs(len(teamrating['RED']) - len(teamrating['BLUE'])) <= 1 and len(teamrating['RED']) >= 2 and len(teamrating['BLUE']) >= 2
  response = {'is_match': is_match}
  for idx, teamupdate in enumerate(updates):
    for playerid, update in teamupdate.iteritems():
      if playerid == '':
        continue
      start = teamratings[idx][playerid]
      def ratingObj(rating):
        return {'raw': {'mu': rating.mu, 'sigma': rating.sigma}, 'friendly': trueskill.expose(rating)}
      response[playerid] = {'start': ratingObj(start), 'updated': ratingObj(update)}

  print json.dumps(response)
  sys.exit()
