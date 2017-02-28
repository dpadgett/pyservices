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
print 'Missing demo files for:'

# find all demos which don't have a current metadata
for demo in db.mindemos.find({'m.v': {'$ne': ver}}, {'_id': 1}).sort('t', pymongo.ASCENDING).limit(5000):
  demo = '/cygdrive/U/demos/' + demo['_id']
  if os.path.isfile(demo.encode('utf8')):
    continue
  print demo.encode('utf8')
  parts = os.path.basename(demo).split(' ')
  files = os.listdir(os.path.dirname(demo))
  files = [(f, f.split(' ')) for f in files]
  matches = [os.path.dirname(demo) + u'/' + f[0].decode('utf8') for f in files if f[1][0] == parts[0] and f[1][-1] == parts[-1]]
  if len(matches) != 1:
    print 'no match'
    continue
  print matches[0].encode('utf8'), 'matched'
  os.rename(matches[0].encode('utf8'), demo.encode('utf8'))
  os.rename((matches[0] + '.dm_meta').encode('utf8'), (demo + '.dm_meta').encode('utf8'))
