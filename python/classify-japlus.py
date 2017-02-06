# requires first train.py to be run

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import re
from sklearn import preprocessing, svm

from make_example import make_example

db = MongoClient().demos
demodb = db.demos
matchdb = db.matches
#matches = list(matchdb.find({'is_match': False}))
alldemos = [demo for match in matchdb.find({'is_match': False}) for demo in match['demos'] if not ('/psychodelic' in demo['id'])]
examples = []
scaledexamples = []
exampleids = []
results = []

from subprocess import Popen, PIPE, call
from os import listdir, stat, remove
from os.path import isfile, isdir, join, exists, basename

def cutdemo(result, exampleid):
  democutter = u'/cygdrive/C/Users/dan/Documents/Visual Studio 2010/Projects/JKDemoMetadata/Release/DemoTrimmer.exe'
  def format_time(time):
    return ("%02d" % (time / 1000 / 60 / 60)) + ':' + ("%02d" % (time / 1000 / 60 % 60)) + ':' + ("%02d" % (time / 1000 % 60)) + '.' + ("%03d" % (time % 1000))
  def strip_non_ascii(string):
    ''' Returns the string without non ASCII characters'''
    stripped = (c for c in string if 0 < ord(c) < 127)
    return ''.join(stripped)
  nice_frag = [result, exampleid]
  #for nice_frag in [i for i in list if i[0] > 2.1]:
  print nice_frag[0], basename(nice_frag[1][0]), nice_frag[1][1]['human_time']
  #shutil.copy2(nice_frag[3], u'/cygdrive/C/Program Files (x86)/jka/base/demos/acur/' + str(nice_frag[0]) + ' ' + nice_frag[2].replace(':', '-').replace('.', '_') + ' ' + basename(nice_frag[3]))
  
  demofd = open( nice_frag[1][0], u'rb' )
  demometafd = open( u'/cygdrive/C/Program Files (x86)/jka/base/demos/acur/' + str(nice_frag[0]) + nice_frag[1][1]['human_time'].replace(':', '-').replace('.', '_') + ' ' + strip_non_ascii(basename(nice_frag[1][0])), u'wb' )
  proc = Popen([democutter, '-', '-', format_time(nice_frag[1][1]['time'] - 9000), format_time(nice_frag[1][1]['time'] + 7000)], stdout=demometafd, stdin=demofd)
  proc.wait()
  demofd.close()
  demometafd.close()


for demo in alldemos:
  print 'Processing', demo['id']
  demodata = demodb.find({'_id':demo['id']})[0]
  map = demodata['metadata']['maps'][0]
  frags = map['ownfrags']
  for frag in frags:
    if frag['target_is_bot'] or frag['attacker_is_bot']:
      # skip bot frags
      continue
    example = make_example(frag, map)
    scaledexample = scaler.transform([example])[0]
    result = clf.predict(scaledexample)[0]
    if result > 1.5:
      exampleid = [demo['id'], frag]
      exampleids.append(exampleid)
      examples.append(example)
      scaledexamples.append(scaledexample)
      results.append(result)
      cutdemo(result, exampleid)
  print 'found', len(examples), 'nice frags'

#results = [clf.predict(example)[0] for example in scaledexamples]
#print len([[results[i], i] for i in range(0,len(results)) if results[i] > 2.0])

print 'Best overall frags:'
for nice_frag in [[results[i], exampleids[i]] for i in range(0,len(results)) if results[i] > 1.9]:
  #for nice_frag in [i for i in list if i[0] > 2.1]:
  print nice_frag[0], basename(nice_frag[1][0]), nice_frag[1][1]['human_time']
