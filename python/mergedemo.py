#!/usr/bin/python -u
import sys
import os
import urlparse

from pymongo import MongoClient
from subprocess import Popen

args = urlparse.parse_qs(urlparse.urlparse(os.environ['REQUEST_URI']).query)

if 'matchid' not in args or len(args['matchid']) != 1:
  print 'No match id specified'
  exit()

matchid = args['matchid'][0]

if 'clientid' not in args or len(args['clientid']) != 1:
  print 'No client id specified'
  exit()

clientid = int(args['clientid'][0])

db = MongoClient("mongodb").demos

matches = db.minmatches.find({'_id': matchid})
match = None
for row in matches:
  match = row
  break
if match == None:
  print 'No match with matchid', matchid
  exit()

args = ['./demochanger', '%d' % clientid]
args.extend(['/cygdrive/U/demos/' + demo['id'] for demo in match['d']])
args.append('-')

print 'Content-type: application/octet-stream'
print 'Content-Disposition: attachment; filename="' + ('%d.%s.dm_26' % (clientid, matchid[:5])) + '"'
print 'Status: 200 OK'
print ''

proc = Popen(args)
proc.wait()
