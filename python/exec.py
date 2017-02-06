#!/usr/bin/python -u
import time
import sys
import os
import traceback
import urlparse
 
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
  try:
    execfile(script, d, d)
  except:
    print traceback.format_exc()
  sys.argv = origargv
  print 'Elapsed:', time.time() - start

print 'Content-type: text/plain'
print 'Status: 200 OK'
print ''
#print os.environ
args = urlparse.parse_qs(urlparse.urlparse(os.environ['REQUEST_URI']).query)
#print args
if 'file' in args:
  script = args['file'][0]
  scriptargs = []
  if 'arg' in args:
    scriptargs = args['arg']
  #print 'Running', script, 'with args', scriptargs
  run(script, scriptargs)
#print 'eof'
