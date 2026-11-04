#!/usr/bin/python -u
import sys
import os
import urlparse

from pymongo import MongoClient
from subprocess import Popen, PIPE
from os.path import basename

args = urlparse.parse_qs(urlparse.urlparse(os.environ['REQUEST_URI']).query)

demo = args['demo'][0]
if (demo[-6:-1] != '.dm_2' and demo[-8:] != '.dm_meta') or demo[0:18] != '/cygdrive/U/demos/' or '/../' in demo:
  print 'Not a demo'
  exit()

if 'time' not in args or len(args['time']) != 1:
  print 'No time specified'
  exit()

time = int(args['time'][0])

if 'before' not in args or len(args['before']) != 1:
  print 'No before specified'
  exit()

before = int(args['before'][0])

if 'after' not in args or len(args['after']) != 1:
  print 'No after specified'
  exit()

after = int(args['after'][0])

start = time - before
end = time + after

prefix = ""
if 'prefix' in args and len(args['prefix']) == 1:
  prefix = args['prefix'][0]

def format_time(time):
  return ("%02d" % (time / 1000 / 60 / 60)) + ':' + ("%02d" % (time / 1000 / 60 % 60)) + ':' + ("%02d" % (time / 1000 % 60)) + '.' + ("%03d" % (time % 1000))
def strip_non_ascii(string):
  # Returns the string without non ASCII characters
  stripped = (c for c in string if 0 < ord(c) < 127)
  return ''.join(stripped)

democutter = u'/home/pyservices/demotrimmer'

fmt = args.get('format', [''])[0]
if fmt in ('mme', 'mme_init', 'mme_meta'):
  import hashlib
  import mme_stream_lib
  st = os.stat(demo)
  cache_key = hashlib.sha1('trim:%s:%d:%d:%s:%s:v1' % (demo, int(st.st_mtime), st.st_size, format_time(start), format_time(end))).hexdigest()
  def _spawn_trimmed(mme_path, meta_path, lock_fd):
    demofd = open(demo, u'rb')
    devnull = open('/dev/null', 'wb')
    trim_args = [democutter, '-', '-', format_time(start), format_time(end)]
    p1 = Popen(trim_args, stdin=demofd, stdout=PIPE, stderr=devnull, close_fds=False, preexec_fn=mme_stream_lib.make_child_setup([]))
    p2 = Popen([mme_stream_lib.CONVERTER_BIN, '-', mme_path, meta_path], stdin=p1.stdout, stdout=devnull, stderr=devnull, close_fds=False, preexec_fn=mme_stream_lib.make_child_setup([lock_fd]))
    p1.stdout.close()
    demofd.close()
    devnull.close()
  mme_stream_lib.handle_mme_request(fmt, args, cache_key, _spawn_trimmed)

args = [democutter, '-', '-', format_time(start), format_time(end)]

time = "%02d-%02d_%04d" % (time / 1000 / 60, (time / 1000) % 60, time % 1000)

print 'Content-type: application/octet-stream'
print 'Content-Disposition: attachment; filename="' + (prefix + time + ' ' + strip_non_ascii(basename(demo))) + '"'
print 'Status: 200 OK'
print ''

print >> sys.stderr, ' '.join(args)

demofd = open( demo, u'rb' )
proc = Popen(args, stdout=sys.stdout, stdin=demofd)
proc.wait()
demofd.close()
