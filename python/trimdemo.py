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
