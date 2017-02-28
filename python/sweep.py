#!/usr/bin/python -u

import time
import sys
import os
import traceback
import urlparse
import StringIO
import json
from subprocess import Popen, PIPE

from pymongo import MongoClient
import pymongo

import struct

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

#args = urlparse.parse_qs(urlparse.urlparse(os.environ['REQUEST_URI']).query)

db = MongoClient("mongodb").demos


# run an empty parse to get current parser version
demoparser = u'/home/pyservices/jkdemometadata'

parse = Popen([demoparser, '-'], stdin=PIPE, stdout=PIPE)
parse.stdin.close()

parseout = parse.stdout.read()
result = parse.wait()

ver = json.loads(parseout)['version']

print 'Current parser version:', ver

# find all demos which don't have a current metadata
while True:
  demos = ['/cygdrive/U/demos/' + demo['_id'] for demo in db.mindemos.find({'m.v': {'$ne': ver}}, {}).sort('t', pymongo.ASCENDING).limit(10)]
  if len(demos) == 0:
    break
  for demo in demos:
    #demo = demo.encode('utf8')
    try:
      print demo.encode('utf8')
    except:
      print '<unprintable>'#, skip for now'
      #continue

    run('create_demo_metadata_single.py', [demo])

    metademo = demo + u'.dm_meta'
    print metademo.encode('utf8')
    with open( (demo + u'.dm_meta').encode('utf8'), u'r' ) as demometafd:
      demometa = json.loads(demometafd.read().decode('utf-8'))

    #run('populate_db.py', [request['demo']])
    run('populate_db_lite.py', [demo])

    for map in demometa['maps']:
      if 'version' in demometa and demometa['version'] >= 2:
        match_id = ((map['serverId'] & 0xFFFFFFFF) << 32) + (map['checksumFeed'] & 0xFFFFFFFF)
        match_hash = struct.pack('!Q', match_id).encode('hex')
        run('create_users.py', [match_hash])
