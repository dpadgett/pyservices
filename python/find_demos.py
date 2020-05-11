#!/usr/bin/python

from os import listdir, stat, utime
from os.path import isfile, isdir, join, exists
from collections import deque
import re
import os
import stat as statlib

from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *
from pytz import timezone
import commands
import datetime
import sys
import calendar

import json
import demometa_lib

# only process last 12 hours folders, since listing all directories eats a lot of cpu
end_time = parse(commands.getoutput("/bin/date") + ' -0800') + relativedelta(days=-2)#days=-7)#hours=-12)#years=-20)

def find_demos_dirs():
  global end_time
  basedir = u'/cygdrive/U/demos'
  #basepaths = [basedir]# + u'/cyd']#, basedir + u'/delta', basedir + u'/eh', basedir + u'/ent', basedir + u'/ovelha', basedir + u'/stubert', basedir + u'/teh', basedir + u'/xen_crypt']
  basepaths = [basedir + dir for dir in demometa_lib.tz_mapping.keys() if dir != '/whoracle' and dir != '/whoracle2']
  #basepaths = [basedir + u'/cyd', basedir + u'/teh']
  #basepaths = [basedir + u'/teh']
  #basepaths = [basedir + u'/teh']
  #basepaths = [basedir + u'/tiin']
  #basepaths = [basedir + u'/onasi']
  #basepaths = [basedir + u'/whoracle3']
  #basepaths.append(basedir + u'/demobot')
  #basepaths = [basedir + u'/fim/autorecord/2020/04']

  def shouldCheck(dir):
    dated_folders = demometa_lib.tz_mapping.keys()
    dated = False
    match = re.match(r'.*/.* ([0-9]{4}-[0-9]{2}-[0-9]{2})_([0-9]{2}-[0-9]{2}-[0-9]{2})', dir)
    if match != None:
      (date, time) = match.groups()
      extra = relativedelta(hours=0)
      dated = True
    else:
      if any([dir.startswith(folder, len(basedir)) for folder in dated_folders]):
        folders = dir[len(basedir + '/'):].split('/')
        dateparts = None
        for idx, folder in enumerate(folders):
          if re.match(r'[0-9]{4}', folder) != None:
            dateparts = folders[idx:]
        if dateparts != None:
          year = int(dateparts[0])
          month = 1
          day = 1
          extra = relativedelta(years=1)
          if len(dateparts) >= 2:
            month = int(dateparts[1])
            extra = relativedelta(months=1)
          if len(dateparts) >= 3:
            day = int(dateparts[2])
            extra = relativedelta(days=1)
          date = '%04d-%02d-%02d' % (year, month, day)
          time = ''
          dated = True
    if dated:
      tm = parse(date + ' ' + time.replace('-', ':'))
      tzone = demometa_lib.timezone_for_demo(dir)
      tm = tzone.localize(tm, is_dst=True)
      tm += extra
      if tm < end_time:
        #print 'Skipping too old folder', dir
        #return True
        return False
        #pass
      print 'Checking dir', dir
      return True
    print 'Checking dir', dir
    return True

  for f in basepaths:
    if not f.endswith('demobot'):
      yield f
  #directories = [f for f in basepaths]
  tovisit = deque(basepaths)
  while len(tovisit) > 0:
    dir = tovisit.popleft()
    for f in listdir(dir):
      if (not f.endswith('.dm_26')) and (not f.endswith('.dm_meta')) and isdir(join(dir,f)) and join(dir,f) != dir and shouldCheck(join(dir,f)):
        tovisit.append(join(dir,f))
        yield join(dir,f)
    #childdirs = [ join(dir,f) for f in listdir(dir) if isdir(join(dir,f)) and join(dir,f) != dir and shouldCheck(join(dir,f)) ]
    #tovisit.extend(childdirs)
    #for f in childdirs:
    #  yield f
    #directories.extend(childdirs)
  #return directories

def check_metadata(demo):
  global end_time
  #print 'check_metadata for', demo
  demo_stat = stat(demo)
  metadata_stat = stat(demo + ".dm_meta")
  if demo_stat.st_mtime > metadata_stat.st_mtime:
    return False
  return True

# these checks are too inefficient... since filesystem is now remote
#TODO: replace with a lookup from the mongodb copy of the data
'''
  try:
    demometafd = open( demo + u'.dm_meta', u'r' )
    demometa = json.loads(demometafd.read().decode('utf-8'))
  except:
    print sys.exc_info()[0]
    return False
  if 'version' in demometa:
    if demometa['version'] < 4:
      return False
  else:
    return False
  #return demo_stat.st_mtime < calendar.timegm(end_time.timetuple())
  if 'filesize' in demometa:
    return demo_stat.st_size == demometa['filesize']
  return False
'''

def find_demos_fixed():
  with open("rerun_files.txt", "r") as f:
    for demo in f:
      yield demo.rstrip('\n').rstrip('\r')

def joinutf8(d,f):
  return join(d.decode('utf-8'),f.decode('utf-8')).encode('utf-8')

def find_demos_real():
  #for d in find_demos_fixed():
  #  yield d
  #return
  directories = find_demos_dirs()
  for d in directories:
    print 'Processing', d
    for file in listdir(d):
      print 'Processing', file
      if (file.endswith(".dm_26") or file.endswith(".dm_25")) and not ".dm_meta" in file and (not exists(joinutf8(d,file + ".dm_meta"))
          or not check_metadata(joinutf8(d,file))):
        yield joinutf8(d,file).decode('utf-8')
  #demos = [ join(d,file) for d in directories for file in listdir(d)
  #  if (file.endswith(".dm_26") or file.endswith(".dm_25")) and not ".dm_meta" in file and (not exists(join(d,file + ".dm_meta"))
  #    or not check_metadata(join(d,file))) ]
  #return demos

import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
import traceback
def find_demos_all():
  #for d in find_demos_all_real():
  #  yield d
  #return
  basedir = u'/cygdrive/U/demos'
  db = MongoClient("mongodb").demos
  mindemodb = db.mindemos
  #db.mindemos.find({'m.m.n': {'$exists': true},'m.m.te': {'$exists': false},'t':{'$gt': new Date('2015-08-03')}}).count()
  #demos = mindemodb.find({'m.m.n': {'$exists': True},'m.m.te': {'$exists': False},'t':{'$gt': datetime.datetime(2015, 8, 3)}}, {'_id':1}).limit(100).batch_size(30)
  demos = mindemodb.find({'m.v': 5}, {'_id':1}).limit(100).batch_size(30)
  for demo in demos:
    print demo['_id'].encode('utf-8')
    yield (basedir + '/' + demo['_id'])

def find_demos():
  #exit(1)
  basedir = u'/cygdrive/U/demos'
  db = MongoClient("mongodb").demos
  mindemodb = db.mindemos
  minmatchdb = db.minmatches
  count = 0
  #db.mindemos.find({'m.v':{'$ne':5},'t':{'$gt': new Date(2016, 8, 15)},'ma':true}).count()
  #db.mindemos.find({'m.v':{'$ne':5},'t':{'$gt': new Date(2016, 8, 15)}}).count()
  #demos = mindemodb.find({'m.v':{'$ne':5},'ma':True,'t':{'$gt': datetime.datetime(2016, 8, 15)}}, {'_id':1}).limit(400).batch_size(10)
  demos = mindemodb.find({'m.v':{'$ne':5},'t':{'$gt': datetime.datetime(2016, 8, 15)}}, {'_id':1}).limit(400).batch_size(10)
  '''
  matches = minmatchdb.find({'ma':True,'t':{'$gt': datetime.datetime(2016, 8, 15)}},{'d':1}).sort('t', pymongo.ASCENDING).batch_size(2)
  tzone = timezone('US/Pacific')
  for match in matches:
    print match['_id']
    demos = [{'_id': d['id']} for d in match['d']]
    for demo in demos:
      file = (basedir + '/' + demo['_id'])
      d = ''
      if (exists(joinutf8(d,file + ".dm_meta"))):
        metastat = stat(joinutf8(d,file + ".dm_meta"))
        if tzone.localize(datetime.datetime.fromtimestamp(metastat.st_mtime), is_dst=True) >= tzone.localize(datetime.datetime(2020, 5, 4, 4, 47), is_dst=True):
          continue
      print demo['_id'].encode('utf-8')
      count += 1
      yield file
      if count >= 400:
        return
  '''
  for demo in demos:
    file = (basedir + '/' + demo['_id'])
    print demo['_id'].encode('utf-8')
    count += 1
    yield file
  if count == 0:
    print 'All demos verified'
    exit(1) # to break bash loop

def find_demos_all_real():
  #for d in find_demos_fixed():
  #  yield d
  #return
  global end_time
  directories = find_demos_dirs()
  tzone = timezone('US/Pacific')
  for d in directories:
    for file in listdir(d):
      if ".dm_meta" in file:
        continue
      try:
        demostat = stat(joinutf8(d,file))
        metastat = stat(joinutf8(d,file + ".dm_meta"))
      except os.error:
        # file did not exist
        continue
      if not statlib.S_ISREG(metastat.st_mode):
        # directory instead of file
        continue
      if (tzone.localize(datetime.datetime.fromtimestamp(metastat.st_mtime), is_dst=True) >= end_time
          and demostat.st_mtime <= metastat.st_mtime):
        yield joinutf8(d,file).decode('utf-8')
  #demos = [ join(d,file) for d in directories for file in listdir(d)
  #  if not ".dm_meta" in file and (exists(join(d,file + ".dm_meta"))
  #    and tzone.localize(datetime.datetime.fromtimestamp(stat(join(d,file + ".dm_meta")).st_mtime), is_dst=True) >= end_time
  #    and stat(join(d,file)).st_mtime <= stat(join(d,file + ".dm_meta")).st_mtime) ]
  #return demos
