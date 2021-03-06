#!/usr/bin/python -u

from subprocess import Popen, PIPE
from os import listdir, stat
from os.path import isfile, isdir, join, exists
import locale
import sys
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
import json
from dateutil.parser import *
from dateutil.tz import *
from datetime import *
from dateutil.relativedelta import *
import pytz
from pytz import timezone
import math
from bson.code import Code
from collections import deque
import hashlib
import struct
import traceback

import find_demos

import shrinker
import copy
import demodb_lib
import demometa_lib

db = MongoClient("mongodb").demos
basedir = u'/cygdrive/U/demos/'

demos = sys.argv[1:]
if len(demos) == 0:
  demos = find_demos.find_demos_all()

basedir = u'/cygdrive/U/demos/'
#db = MongoClient('mongodb://stuffer:plz2gold@sjc.jactf.com,pub.jactf.com,whoracle.jactf.com/demos?replicaSet=ctfpug', tz_aware = True).demos
#db = MongoClient('mongodb://localhost:27018/demos', tz_aware = True).demos
demodb = db.mindemos

updates = demodb_lib.DemoDbLib()

for demo in demos:
  mtime = stat( (demo + u'.dm_meta').encode('utf8') ).st_mtime
  existing_mtime = -1
  for doc in demodb.find({'_id': demo[len(basedir):]}, { 'mt': 1 }):
    if 'mt' in doc:
      existing_mtime = doc['mt'] = doc['mt']
  #if demo in existing_demos and existing_demos[demo] >= mtime:
  if existing_mtime >= mtime:
    print 'Skipping already-present demo', demo.encode('utf8')
    continue
    #pass
  print 'Processing demo: ' + demo.encode('utf8')
  try:
    demometafd = open( (demo + u'.dm_meta').encode('utf8'), u'r' )
  except:
    print sys.exc_info()[0]
    continue
  try:
    demometa = json.loads(demometafd.read().decode('utf-8'))
  except:
    print sys.exc_info()[0]
    continue
  if (demo.find(' ') != -1):
    tmstr = demo.rsplit(' ', 1)[1][0:-6]
  else:
    tmstr = ''
  if (any(c.isalpha() for c in tmstr)):
    tmstr = ''
  if (tmstr.find('-') == -1 and tmstr != ''):
    tmstr = ' '.join(demo.rsplit(' ', 2)[-2:])[0:-6]
  if (tmstr.find('-') == -1 or (len(tmstr) != 17 and len(tmstr) != 19)):
    print 'Warning: creation timestamp couldn\'t be found in filename, calculating from mtime'
    timemillis = stat(demo.encode('utf8')).st_mtime * 1000
    for map in demometa['maps']:
      timemillis -= map['map_end_time'] - map['map_start_time']
    tm = datetime.fromtimestamp(round(timemillis / 1000))
  else:
    if (tmstr.find('_') == -1):
      (date, time) = tmstr.split(' ')
    else:
      (date, time) = tmstr.split('_')
    tm = parse(date + ' ' + time.replace('-', ':'))
  tzone = demometa_lib.timezone_for_demo(demo)
  tm = tzone.localize(tm, is_dst=True)
  print 'Time:', tm
  wrappeddemometa = shrinker.minimize({'_id': demo, 'time_created': tm, 'metadata_mtime': mtime, 'metadata': copy.deepcopy(demometa)})
  # write client name separately since mongodb can't query it properly
  if 'maps' in demometa and len(demometa['maps']) > 0:
    for idx, map in enumerate(demometa['maps']):
      (match, match_hash) = demometa_lib.map_is_match(demometa, map)
      wrappeddemometa['m']['m'][idx]['ma'] = match
      wrappeddemometa['m']['m'][idx]['h'] = match_hash
    namefreq = {}
    allnames = [map['names'][str(demometa['client']['id'])] for map in demometa['maps'] if 'names' in map and str(demometa['client']['id']) in map['names']]
    allnames = [name for names in allnames for name in names]
    for name in allnames:
      curval = namefreq.get(name['name'], 0)
      namefreq[name['name']] = curval + name['name_end_time'] - name['name_start_time']
    maxnametime = 0
    maxname = ''
    for name in namefreq.keys():
      if namefreq[name] > maxnametime:
        maxname = name
        maxnametime = namefreq[name]
    wrappeddemometa['p'] = maxname
    (match, match_hash) = demometa_lib.is_match(demometa)
    wrappeddemometa['ma'] = match
    wrappeddemometa['h'] = match_hash
  updates.update_demo(wrappeddemometa)

updates.flush()
