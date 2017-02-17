#!/usr/bin/python

from subprocess import Popen, PIPE
from os import listdir, stat, utime
from os.path import isfile, isdir, join, exists
import locale
import sys
import time
import find_demos
import traceback

#exit(0)

#demoparser = u'/cygdrive/C/Users/dan/Documents/Visual Studio 2010/Projects/JKDemoMetadata/Release/JKDemoMetadata.exe'
demoparser = u'/home/pyservices/jkdemometadata'

demos = sys.argv[1:] #['/cygdrive/U/demos/whoracle3/autorecord/mpctf_kejim 2014-07-06_19-20-04/1 staso mpctf_kejim 2014-07-06_19-20-04.dm_26']

procs = []
for demo in demos:
  print 'Processing demo: ' + demo.encode('utf8')
  try:
    demofd = open( demo.encode('utf8'), u'rb' )
    demometafd = open( (demo + u'.dm_meta').encode('utf8'), u'wb' )
  except:
    print traceback.format_exc()
    continue
  timestamp = time.time()
  procs.append([Popen([demoparser, '-'], stdout=demometafd, stdin=demofd), demofd, demometafd, (demo + u'.dm_meta').encode('utf8'), timestamp])
  if len( procs ) > 0:
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

#execfile('/home/dan/populate_db.py')
