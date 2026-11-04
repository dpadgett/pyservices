#!/usr/bin/python -u
import sys
import os
import urlparse

from pymongo import MongoClient
from subprocess import Popen, PIPE

args = urlparse.parse_qs(urlparse.urlparse(os.environ['REQUEST_URI']).query)

if 'matchid' not in args or len(args['matchid']) != 1:
  print 'No match id specified'
  exit()

matchid = args['matchid'][0]

if 'clientid' not in args or len(args['clientid']) != 1:
  print 'No client id specified'
  exit()

clientid = int(args['clientid'][0])

fmt = args.get('format', [''])[0]
if fmt in ('mme', 'mme_init', 'mme_meta'):
  import hashlib
  import mme_stream_lib
  cache_key = hashlib.sha1('merged:%s:%d:v1' % (matchid, clientid)).hexdigest()
  def _spawn_merged(mme_path, meta_path, lock_fd):
    db = MongoClient("mongodb").demos
    match = db.minmatches.find_one({'_id': matchid})
    if not match:
      return
    changer_args = ['/home/pyservices/demochanger', '%d' % clientid]
    changer_args.extend(['/cygdrive/U/demos/' + demo['id'] for demo in match['d']])
    changer_args.append('-')
    devnull = open('/dev/null', 'wb')
    p1 = Popen(changer_args, stdin=devnull, stdout=PIPE, stderr=devnull, close_fds=False, preexec_fn=mme_stream_lib.make_child_setup([]))
    p2 = Popen([mme_stream_lib.CONVERTER_BIN, '-', mme_path, meta_path], stdin=p1.stdout, stdout=devnull, stderr=devnull, close_fds=False, preexec_fn=mme_stream_lib.make_child_setup([lock_fd]))
    p1.stdout.close()
    devnull.close()
  mme_stream_lib.handle_mme_request(fmt, args, cache_key, _spawn_merged)

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

print >> sys.stderr, ' '.join(args)

proc = Popen(args, stdout=sys.stdout)
proc.wait()
#import shutil
#shutil.copyfileobj(proc.stdout, sys.stdout)

#print 'Done'
