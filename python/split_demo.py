#!/usr/bin/python

import sys
import json
import os
import time

from subprocess import Popen, PIPE, call

def format_time(time):
  return ("%02d" % (time / 1000 / 60 / 60)) + ':' + ("%02d" % (time / 1000 / 60 % 60)) + ':' + ("%02d" % (time / 1000 % 60)) + '.' + ("%03d" % (time % 1000))

# script should be a list of [demofile, startmillis] pairs
def mergedemo(script, filename):
  democutter = u'/cygdrive/C/Users/dan/Documents/Visual Studio 2010/Projects/JKDemoMetadata/Release/DemoMerger.exe'
  #for nice_frag in [i for i in list if i[0] > 2.1]:
  #print nice_frag[0], basename(nice_frag[1][0]), nice_frag[1][1]['human_time']
  #shutil.copy2(nice_frag[3], u'/cygdrive/C/Program Files (x86)/jka/base/demos/acur/' + str(nice_frag[0]) + ' ' + nice_frag[2].replace(':', '-').replace('.', '_') + ' ' + basename(nice_frag[3]))
  
  scriptfile = 'C:\\cygwin\\tmp\\script'
  cygscriptfile = '/tmp/script'
  scriptfd = open( cygscriptfile, u'wb' )
  for pair in script:
    demofile, startmillis = pair
    scriptfd.write( demofile[1] + ' ' + str(demofile[0]) + ' ' + format_time(startmillis) + "\n" )
  scriptfd.close()
  
  proc = Popen([democutter, scriptfile, filename])
  proc.wait()

def split_demo(demo):
  metafile = demo[0:-1].replace('.dm_2', '.dm_meta')
  f = open(metafile, 'r')
  demometa = json.loads(f.read())
  f.close()
  mapnum = 0
  timemillis = os.stat(demo).st_mtime * 1000
  for map in demometa['maps']:
    outfile = demo[0:-1].replace('.dm_2', '.map' + str(mapnum + 1) + '.dm_26')
    osoutfile = outfile.replace('/cygdrive/U/', 'U:/').replace('/cygdrive/C/', 'C:/')
    osdemo = demo.replace('/cygdrive/U/', 'U:/').replace('/cygdrive/C/', 'C:/')
    print 'Splitting out map', (mapnum + 1)
    mergedemo([[(mapnum, osdemo), 0]], osoutfile)
    # touch the outfile so it has the right timestamp
    os.utime(outfile, (int(time.time()), int(timemillis / 1000)))
    timemillis += map['map_end_time'] - map['map_start_time']
    mapnum += 1

if __name__ == '__main__':
  if len(sys.argv) <= 1:
    print 'Usage:', sys.argv[0], 'demo.dm_26'
    print 'Note: .dm_meta must be in the same directory'
    exit(1)
  demo = sys.argv[1]
  split_demo(demo)
