# requires first train.py to be run

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import re
from sklearn import preprocessing, svm

import sys
import traceback
import datetime
import json

from bson.objectid import ObjectId
import pymongo

from make_example import make_example

from joblib import dump, load
clf = load('niceshot.joblib')
scaler = load('niceshotscaler.joblib')

db = MongoClient("mongodb").demos
demodb = db.mindemos
matchdb = db.minmatches

playerid = ObjectId('55dc3d0acb15c73790d832d7')

#alldemos = [demo for match in matches for demo in match['demos'] if not ('/psychodelic' in demo['id'])]
#alldemos = [demo for match in matchdb.find({'t': {'$gt': datetime.datetime.now() - datetime.timedelta(days=3)}, 'ma': True}, {'d.id'}) for demo in match['d']]
sessiondb = db.sessions
sessiongamedb = db.sessionGames
sessionids = sessiondb.distinct('_id', {'playerid': playerid})
games = [game for game in sessiongamedb.find({'_id.session': {'$in': sessionids}, 'time': {'$gt': datetime.datetime.now() - datetime.timedelta(days=30)}, 'is_match': True}).sort('time', pymongo.DESCENDING)]
#alldemos = [demo for match in matchdb.find({'t': {'$gt': datetime.datetime.now() - datetime.timedelta(days=30)}, 'ma': True}, {'d.id'}) for demo in match['d']]
alldemos = [{'id': demo} for game in games for onegame in game['games'] for demo in onegame['demos']]
examples = []
exampleids = []

for demo in alldemos:
  try:
    print 'Processing', demo['id']
    with open('/cygdrive/U/demos/' + demo['id'] + '.dm_meta', 'r') as f:
      demodata = {'metadata': json.loads(f.read())}
    map = demodata['metadata']['maps'][0]
    for amap in demodata['metadata']['maps']:
      #print 'map duration: %d' % (amap['map_end_time'] - amap['map_start_time'])
      if amap['map_end_time'] - amap['map_start_time'] > map['map_end_time'] - map['map_start_time']:
        map = amap
    frags = map['ownfrags']
    for frag in frags:
      exampleids.append([demo['id'], frag])
      examples.append(make_example(frag, map))
    print 'found', len(examples), 'frags'
  except KeyboardInterrupt:
    raise
  except:
    print traceback.format_exc()
    print 'had an error, skipping'

scaledexamples = scaler.transform(examples)

results = clf.predict(scaledexamples)
print len([[results[i], i] for i in range(0,len(results)) if results[i] > 1.5])

from subprocess import Popen, PIPE, call
from os import listdir, stat, remove
from os.path import isfile, isdir, join, exists, basename

#democutter = u'/cygdrive/C/Users/dan/Documents/Visual Studio 2010/Projects/JKDemoMetadata/Release/DemoTrimmer.exe'
democutter = u'/home/pyservices/demotrimmer'
def format_time(time):
  return ("%02d" % (time / 1000 / 60 / 60)) + ':' + ("%02d" % (time / 1000 / 60 % 60)) + ':' + ("%02d" % (time / 1000 % 60)) + '.' + ("%03d" % (time % 1000))
def strip_non_ascii(string):
  ''' Returns the string without non ASCII characters'''
  stripped = (c for c in string if 0 < ord(c) < 127)
  return ''.join(stripped)
print 'Best overall frags:'
for nice_frag in [[results[i], exampleids[i]] for i in range(0,len(results)) if results[i] > 1.5]:
  #for nice_frag in [i for i in list if i[0] > 2.1]:
  print nice_frag[0], basename(nice_frag[1][0]), nice_frag[1][1]['human_time']
  #shutil.copy2(nice_frag[3], u'/cygdrive/C/Program Files (x86)/jka/base/demos/acur/' + str(nice_frag[0]) + ' ' + nice_frag[2].replace(':', '-').replace('.', '_') + ' ' + basename(nice_frag[3]))
  
  demofd = open( '/cygdrive/U/demos/' + nice_frag[1][0], u'rb' )
  #demometafd = open( u'/cygdrive/C/Program Files (x86)/jka/base/demos/acur/' + str(nice_frag[0]) + ' ' + nice_frag[1][1]['human_time'].replace(':', '-').replace('.', '_') + ' ' + strip_non_ascii(basename(nice_frag[1][0])), u'wb' )
  demometafd = open( u'/home/pyservices/nicedemos/' + str(nice_frag[0]) + ' ' + nice_frag[1][1]['human_time'].replace(':', '-').replace('.', '_') + ' ' + strip_non_ascii(basename(nice_frag[1][0])), u'wb' )
  proc = Popen([democutter, '-', '-', format_time(nice_frag[1][1]['time'] - 9000), format_time(nice_frag[1][1]['time'] + 7000)], stdout=demometafd, stdin=demofd)
  proc.wait()
  demofd.close()
  demometafd.close()
