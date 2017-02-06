#!/usr/bin/python

import sys
import time

input = 'dummy' #sys.stdin.read()

print 'Test'
print 'Did it work?'

print sys.argv

import os
print 'Env:', os.environ
print 'eof'

time.sleep(5)

print 'input:'
print 'Read', len(sys.stdin.read()), 'bytes'
print 'eof'
