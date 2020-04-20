#!/usr/bin/python -u

from subprocess import Popen, PIPE
import sys
import json
import os

demoparser = u'./jkdemometadata'

if 'PATH_INFO' not in os.environ:
  exit()

demo = '/cygdrive/U/demos/demobot' + os.environ['PATH_INFO']

print >> sys.stderr, 'Writing', demo

if not os.path.isdir(os.path.dirname(demo)):
  os.makedirs(os.path.dirname(demo))

demometafd = open( demo + u'.dm_meta', u'wb' )

# pre-tee is a hack to count the num of bytes read without having to read thru the input manually in python
pretee = Popen(['bash', '-c', 'tee >(wc -c >&2)'], stdout=PIPE, stderr=PIPE)
tee = Popen(['tee', demo], stdin=pretee.stdout, stdout=PIPE)
parse = Popen([demoparser, '-', 'progress'], stdin=tee.stdout, stdout=demometafd)
pretee.stdout.close()
tee.stdout.close()

result = parse.wait()
tee.wait()
inputlen = int(pretee.stderr.read())
pretee.stderr.close()
pretee.wait()

demometafd.close()

print 'Content-type: text/plain'
print 'Status: 200 OK'
print ''

if result == 0:
  file = demo[len('/cygdrive/'):]
  file = file[0] + ':' + file[1:]
  print json.dumps({'bytes': inputlen, 'file': file})
