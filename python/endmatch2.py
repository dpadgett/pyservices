#!/usr/bin/python
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

import StringIO
import time

def run(script, args):
  cwd = '/home/pyservices/'
  if script[0:1] != '/':
    script = cwd + script
  start = time.time()
  origargv = sys.argv
  sys.argv = [script]
  sys.argv.extend(args)
  print sys.argv
  d = dict(locals(), **globals())
  execfile(script, d, d)
  sys.argv = origargv
  print 'Elapsed:', time.time() - start

def longestmap(data):
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
  return map

class multifile(object):
  def __init__(self, files):
      self._files = files
  def __getattr__(self, attr, *args):
      return self._wrap(attr, *args)
  def _wrap(self, attr, *args):
      def g(*a, **kw):
          for f in self._files:
              res = getattr(f, attr, *args)(*a, **kw)
          return res
      return g

if __name__ == '__main__':
  request = json.loads(sys.stdin.read())
  response = {}
  logbuf = StringIO.StringIO()
  origout = sys.stdout
  sys.stdout = multifile([sys.stderr, logbuf])
  
  start = time.time()
  
  try:
    demometafd = open( request['demo'] + u'.dm_meta', u'r' )
  except:
    print traceback.format_exc()
    #print sys.exc_info()[0]
  demometa = None
  try:
    demometa = json.loads(demometafd.read().decode('utf-8'))
    map = longestmap(demometa)
    match_hash = ''
    if 'version' in demometa and demometa['version'] >= 2:
      match_id = ((map['serverId'] & 0xFFFFFFFF) << 32) + (map['checksumFeed'] & 0xFFFFFFFF)
      match_hash = struct.pack('!Q', match_id).encode('hex')
    
    response['matchid'] = match_hash

    #run('populate_db.py', [request['demo']])
    run('populate_db_lite.py', [request['demo']])
    run('create_users.py', [match_hash])
    run('calculate_ranks_v3.py', [])
    
    db = MongoClient('mongodb').demos
    playergamedb = db.playerGames
    cursor = playergamedb.find({'_id.match': match_hash})
    response['elos'] = []
    response['is_match'] = False
    for row in cursor:
      #print row
      if row['is_match'] == False:
        continue
      response['is_match'] = True
      if 'rating' in row:
        response['elos'].append({'client_num': row['client_num'], 'rating': row['rating'], 'name': row['names'][-1]['name'], 'team': row['score']['team']})
  except:
    print traceback.format_exc()
    #print sys.exc_info()[0]
  
  sys.stdout = origout
  try:
    response['log'] = logbuf.getvalue()
  except:
    response['log'] = 'Error returning server log'
  response['elapsed'] = time.time() - start
  logbuf.close()
  print json.dumps(response)
  #sys.exit()
