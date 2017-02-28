#!/usr/bin/python -u
import time
import sys
import os
import traceback
import urlparse
import StringIO
import json

#from pymongo import MongoClient
 
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

print 'Content-type: text/plain'
print 'Status: 200 OK'
print ''

#db = MongoClient("mongodb").demos

if args['rpc'] == ['endmatch']:
  if 'demo' not in args or len(args['demo']) != 1:
    print 'Missing demo'
    exit()
  demo = args['demo'][0]
  origstdin = sys.stdin
  sys.stdin = StringIO.StringIO(json.dumps({'demo': demo}))
  run('endmatch2.py', [])
  sys.stdin = origstdin
