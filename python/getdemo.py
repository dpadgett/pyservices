#!/usr/bin/python -u
import sys
import os
import urlparse
import shutil

args = urlparse.parse_qs(urlparse.urlparse(os.environ['REQUEST_URI']).query)

if 'demo' not in args or len(args['demo']) != 1:
  print 'No demo specified'
  exit()

demo = args['demo'][0]
if demo[-6:-1] != '.dm_2' or demo[0:18] != '/cygdrive/U/demos/' or '/../' in demo:
  print 'Not a demo'
  exit()

if not os.path.isfile(demo):
  print 'Content-type: text/plain'
  print 'Status: 404 Not Found'
  print ''
  print 'File', demo, 'not found'

print 'Content-type: application/octet-stream'
print 'Content-Disposition: attachment; filename="' + os.path.basename(demo) + '"'
print 'Status: 200 OK'
print ''

with open(demo, 'r') as f:
  shutil.copyfileobj(f, sys.stdout)
