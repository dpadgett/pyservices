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
end_time = parse(commands.getoutput("/bin/date") + ' -0800') + relativedelta(days=-7)#hours=-12)#years=-20)

def find_demos_dirs():
  global end_time
  global demometa_lib.tz_mapping
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

  def shouldCheck(dir):
    global demometa_lib.tz_mapping
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
    yield f
  #directories = [f for f in basepaths]
  tovisit = deque(basepaths)
  while len(tovisit) > 0:
    dir = tovisit.popleft()
    for f in listdir(dir):
      if isdir(join(dir,f)) and join(dir,f) != dir and shouldCheck(join(dir,f)):
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

def find_demos():
  #for d in find_demos_fixed():
  #  yield d
  #return
  directories = find_demos_dirs()
  for d in directories:
    for file in listdir(d):
      if (file.endswith(".dm_26") or file.endswith(".dm_25")) and not ".dm_meta" in file and (not exists(join(d,file + ".dm_meta"))
          or not check_metadata(join(d,file))):
        yield join(d,file)
  #demos = [ join(d,file) for d in directories for file in listdir(d)
  #  if (file.endswith(".dm_26") or file.endswith(".dm_25")) and not ".dm_meta" in file and (not exists(join(d,file + ".dm_meta"))
  #    or not check_metadata(join(d,file))) ]
  #return demos

def find_demos_all():
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
        demostat = stat(join(d,file))
        metastat = stat(join(d,file + ".dm_meta"))
      except os.error:
        # file did not exist
        continue
      if not statlib.S_ISREG(metastat.st_mode):
        # directory instead of file
        continue
      if (tzone.localize(datetime.datetime.fromtimestamp(metastat.st_mtime), is_dst=True) >= end_time
          and demostat.st_mtime <= metastat.st_mtime):
        yield join(d,file)
  #demos = [ join(d,file) for d in directories for file in listdir(d)
  #  if not ".dm_meta" in file and (exists(join(d,file + ".dm_meta"))
  #    and tzone.localize(datetime.datetime.fromtimestamp(stat(join(d,file + ".dm_meta")).st_mtime), is_dst=True) >= end_time
  #    and stat(join(d,file)).st_mtime <= stat(join(d,file + ".dm_meta")).st_mtime) ]
  #return demos
