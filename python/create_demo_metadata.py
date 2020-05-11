#!/usr/bin/python -u

from subprocess import Popen, PIPE
from os import listdir, stat, utime
from os.path import isfile, isdir, join, exists
import locale
import sys
import time
import find_demos
import traceback
import fileinput

#exit(0)

#demoparser = u'/cygdrive/C/Users/dan/Documents/Visual Studio 2010/Projects/JKDemoMetadata/Release/JKDemoMetadata.exe'
demoparser = u'/home/pyservices/jkdemometadata'

demos = [demo.rstrip() for demo in sys.stdin]
if len(demos) == 0:
  demos = find_demos.find_demos()

maxprocs = 1
if len(sys.argv) > 1:
  maxprocs = int(sys.argv[1])

if len(sys.argv) > 2:
  demoparser = sys.argv[2]

demolist = []

procs = []
for demo in demos:
  try:
    demo = demo.encode('utf8')
  except:
    pass
  print 'Processing demo: ' + demo
  test = (demo.decode('utf8') + u'.dm_meta').encode('utf8')
  print test
  #exit()
  if len(sys.argv) <= 2:
    try:
      demofd = open( demo, u'rb' )
      demometafd = open( test, u'wb' )
    except:
      print traceback.format_exc()
      print sys.exc_info()#[0]
      continue
    demolist.append(demo.decode('utf8'))
    timestamp = time.time()
    procs.append([Popen([demoparser, '-'], stdout=demometafd, stdin=demofd), demofd, demometafd, test, timestamp])
  else:
    try:
      demofd = open( '/dev/null', u'rb' )
      demometafd = open( '/dev/null', u'wb' )
    except:
      print traceback.format_exc()
      print sys.exc_info()#[0]
      continue
    demolist.append(demo.decode('utf8'))
    timestamp = time.time()
    procs.append([Popen([demoparser, demo]), demofd, demometafd, test, timestamp])
  if len( procs ) > maxprocs:
    procs[0][0].wait()
    procs[0][1].close()
    procs[0][2].close()
    utime(procs[0][3], (procs[0][4], procs[0][4]))
    del procs[0]

for proc in procs:
  proc[0].wait()
  proc[1].close()
  proc[2].close()
  utime(proc[3], (proc[4], proc[4]))

print 'Finished creating metadata, populating DB'

if len(demolist) > 0:
  sys.argv[1:] = demolist
  
  #execfile('/home/pyservices/populate_db.py')
  execfile('/home/pyservices/populate_db_lite.py')
