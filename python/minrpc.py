#!/usr/bin/python -u
import time
import sys
import os
import traceback
import urlparse
import StringIO
import json

from pymongo import MongoClient
import pymongo

from bson.objectid import ObjectId
import json
import bson.json_util

import shrinker

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

args = urlparse.parse_qs(urlparse.urlparse(os.environ['REQUEST_URI']).query)

if 'rpc' not in args:
  print 'Content-type: text/plain'
  print 'Status: 200 OK'
  print ''

  print 'Missing RPC'
  exit()

if args['rpc'] == ['bundle']:
  for arg in ['start', 'end']:
    if arg not in args or len(args[arg]) != 1:
      print 'Missing', arg
      exit()
  start = int(args['start'][0])
  end = int(args['end'][0])
  run('bundle.py', [start, end])
  exit()
elif args['rpc'] == ['matchdemo']:
  if 'id' not in args or len(args['id']) != 1:
    print 'Missing id'
    exit()
  id = args['id'][0]
  run('merge_matches_predict.py', [id])
  exit()

print 'Content-type: text/plain'
print 'Status: 200 OK'
print ''

#db = MongoClient("mongodb").demos

def wrap_result(result):
  if args.get('jsonp', None) == ['true']:
    return args['rpc'][0] + ' = ' + bson.json_util.dumps(result) + ';'
  else:
    return bson.json_util.dumps(result)

def read_matches(cursor):
  db = MongoClient("mongodb").demos
  sessiongamedb = db.sessionGames
  ratingdb = db.playerGameRatings
  sessiondb = db.sessions
  matches = []
  for match in cursor:
    match = shrinker.inflate_match(match)
    sessiongames = [s for s in sessiongamedb.find({'_id.match': match['_id']})]
    sessions = {bson.json_util.dumps(s['_id']): s for s in sessiondb.find({'_id': {'$in': [sg['_id']['session'] for sg in sessiongames]}})}
    #print match['_id'], sessiongames
    ratings = {str(r['_id']['player']): r for r in ratingdb.find({'_id.match': match['_id']})}
    for demo in match['demos']:
      for sg in sessiongames:
        if len([g for g in sg['games'] if demo['id'][len('/cygdrive/U/demos/'):] in g['demos']]) > 0:
          demo['player'] = str(sessions[bson.json_util.dumps(sg['_id']['session'])]['playerid'])
          if demo['player'] in ratings:
            demo['rating'] = ratings[demo['player']]['rating']
    matches.append(match)
  return matches

if args['rpc'] == ['endmatch']:
  if 'demo' not in args or len(args['demo']) != 1:
    print 'Missing demo'
    exit()
  demo = args['demo'][0]
  origstdin = sys.stdin
  sys.stdin = StringIO.StringIO(json.dumps({'demo': demo}))
  run('endmatch3.py', [])
  sys.stdin = origstdin
elif args['rpc'] == ['newdemos']:
  for arg in ['since']:
    if arg not in args or len(args[arg]) != 1:
      print 'Missing', arg
      exit()
  since = int(args['since'][0])
  run('newdemos.py', [since])
  exit()
elif args['rpc'] == ['recentmatches']:
  db = MongoClient("mongodb").demos
  matchdb = db.minmatches

  limit = int(args.get('limit', ['5'])[0])
  offset = int(args.get('offset', ['0'])[0])

  cursor = matchdb.find({'ma': True}).sort('t', -1)
  total = cursor.count()
  cursor = cursor.skip(offset).limit(limit)
  matches = read_matches(cursor)
  print wrap_result({'result': matches, 'offset': offset, 'limit': limit, 'total': total})
elif args['rpc'] == ['lookupmatch']:
  db = MongoClient("mongodb").demos
  matchdb = db.minmatches

  limit = int(args.get('limit', ['5'])[0])
  offset = int(args.get('offset', ['0'])[0])

  cursor = matchdb.find({'_id': args['id'][0]}).sort('t', -1)
  total = cursor.count()
  cursor = cursor.skip(offset).limit(limit)
  matches = read_matches(cursor)
  print wrap_result({'result': matches, 'offset': offset, 'limit': limit, 'total': total})
