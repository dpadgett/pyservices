#!/usr/bin/python -u
import time
import sys
import os
import traceback
import urlparse

from pymongo import MongoClient
 
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

print 'Content-type: text/plain'
print 'Status: 200 OK'
print ''

args = urlparse.parse_qs(urlparse.urlparse(os.environ['REQUEST_URI']).query)

if 'rpc' not in args:
  print 'Missing RPC'
  exit()

db = MongoClient("mongodb").demos

if args['rpc'] == ['teams']:
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
