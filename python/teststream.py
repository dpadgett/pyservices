#!/usr/bin/python -u

from subprocess import Popen, PIPE
import sys
import json
import os

print 'Content-type: text/plain'
print 'Status: 200 OK'
print ''

print >> sys.stderr, os.environ

data = sys.stdin.read(1024)
while len(data) > 0:
  print >> sys.stderr, 'received', len(data), ' bytes' #: "' + data + '"' + ":".join("{:02x}".format(ord(c)) for c in data)
  data = sys.stdin.read(1024)
