#!/usr/bin/python -u
import time
import sys
import os
import traceback
import urlparse

from pymongo import MongoClient
import pymongo

from bson.objectid import ObjectId
import json
import bson.json_util

import player_lib

def run(script, args):
  cwd = '/home/pyservices/'
  if script[0:1] != '/':
    script = cwd + script
  start = time.time()

  origargv = sys.argv
  sys.argv = [script]
  sys.argv.extend(args)
  #print sys.argv
  d = dict(locals(), **globals())
  try:
    execfile(script, d, d)
  except:
    print traceback.format_exc()
  sys.argv = origargv
  #print 'Elapsed:', time.time() - start

body = sys.stdin.read()

print 'Content-type: text/plain'
print 'Status: 200 OK'
print ''

args = urlparse.parse_qs(urlparse.urlparse(os.environ['REQUEST_URI']).query)

rpc = None
if 'rpc' in args:
  rpc = args['rpc'][0]
elif body != '':
  body = bson.json_util.loads(body)
  rpc = body['rpc']
  
if 'rpc' is None:
  print 'Missing RPC'
  exit()

db = MongoClient("mongodb").demos

def wrap_result(result):
  if args.get('jsonp', None) == ['true']:
    return args['rpc'][0] + ' = ' + bson.json_util.dumps(result) + ';'
  else:
    return bson.json_util.dumps(result)

def checkmerge():
  ip = os.environ['HTTP_X_REAL_IP']
  allowed = ip == '135.180.87.144'
  return allowed

if rpc == 'teams':
  playerdb = db.players
  scriptargs = []
  for team in ['RED', 'BLUE']:
    scriptargs.append(team[0])
    for (ip_hash, guid_hash) in zip(args.get(team + '_id1[]', []), args.get(team + '_id2[]', [])):
      #print team, ip_hash, guid_hash
      matches = playerdb.find({'$or': [{'guid_hash.guid': int(guid_hash)}, {'ip_hash.ip': int(ip_hash)}]})
      rating = None
      for match in matches:
        if 'rating' in match and 'raw' in match['rating']:
          rating = match['rating']['raw']
          break
      #print rating
      if rating != None:
        scriptargs.append("%f,%f" % (rating['mu'],rating['sigma']))
      else:
        scriptargs.append('unknown')
  run('teams.py', scriptargs)
elif rpc == 'lookup':
  playerdb = db.sessionPlayers
  sessiondb = db.sessions
  sessiongamedb = db.sessionGames
  player = {}
  playerid = args['id'][0]
  for player in playerdb.find({'_id': ObjectId(playerid)}):
    pass
  #player['_id'] = playerid
  sessions = [session for session in sessiondb.find({'playerid': ObjectId(playerid)}).sort('last_game', pymongo.DESCENDING)]
  for session in sessions:
    del session['playerid']
  player['sessions'] = sessions
  '''
  if len(sessions) > 0:
    # hack it
    player['last_name'] = sessions[0]['last_name']
  '''
  print bson.json_util.dumps(player)
elif rpc == 'sessiongames':
  sessiondb = db.sessions
  sessiongamedb = db.sessionGames
  sessionid = json.loads(args['id'][0])
  limit = int(args.get('limit', ['100'])[0])
  session = [s for s in sessiondb.find({'_id': sessionid})][0]
  query = {'_id.session': sessionid}
  if 'match' in args:
    query['is_match'] = args['match'][0] == 'true'
  games = [game for game in sessiongamedb.find(query).sort('time', pymongo.DESCENDING).limit(limit)]
  ratingdb = db.playerGameRatings
  ratings = {rating['_id']['match']: rating for rating in ratingdb.find({'_id.player': session['playerid'], '_id.match': {'$in': [game['_id']['match'] for game in games]}})}
  for game in games:
    if game['_id']['match'] in ratings:
      game['rating'] = ratings[game['_id']['match']]
  print bson.json_util.dumps(games)
elif rpc == 'checkmerge':
  allowed = checkmerge()
  print wrap_result(allowed)
elif rpc == 'topplayers' or rpc == 'searchplayers':
  playerdb = db.sessionPlayers
  sessiondb = db.sessions
  sessiongamedb = db.sessionGames
  limit = int(args.get('limit', ['25'])[0])
  offset = int(args.get('offset', ['0'])[0])
  if rpc == 'topplayers':
    cursor = playerdb.find().sort('num_matches', pymongo.DESCENDING)
  elif rpc == 'searchplayers':
    sessions = sessiongamedb.distinct('_id.session', {'games.names.name': args['name'][0]})
    playerids = sessiondb.distinct('playerid', {'_id': {'$in': sessions}})
    cursor = playerdb.find({'_id': {'$in': playerids}}).sort('num_matches', pymongo.DESCENDING)
  total = cursor.count()
  players = [player for player in cursor.skip(offset).limit(limit)]
  print wrap_result({'result': players, 'offset': offset, 'limit': limit, 'total': total})
elif rpc == 'mergeplayers':
  if not checkmerge():
    print 'Not allowed'
    exit()
  ids = [bson.ObjectId(id) for id in body['ids']]
  updated = player_lib.merge_players(ids)
  print wrap_result(updated)
elif rpc == 'setname':
  if not checkmerge():
    print 'Not allowed'
    exit()
  playerdb = db.sessionPlayers
  updated = playerdb.find_one_and_update({'_id': bson.ObjectId(body['id'])}, {'$set': {'name': body['name']}})
  print wrap_result(updated)
