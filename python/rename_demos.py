#!/usr/bin/python

import find_demos
from os import listdir, stat, utime, makedirs
from os.path import isfile, isdir, join, exists
import re
import shutil
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
import traceback

db = MongoClient().demos
demodb = db.mindemos
fulldb = db.demos
basedir = u'/cygdrive/U/demos/'

'''
if __name__ == '__main__':
  rootdir = u'/cygdrive/Q/dumbledore3@gmail.com/demos/nyc/japlus'
  for dir in listdir(rootdir):
    dir = join(rootdir, dir)
    match = re.match(r'.*/([0-9]{4})/([0-9]{2})/([0-9]{2})/', dir)
    if match != None:
      continue
    match = re.match(r'(.*)/.* ([0-9]{4})-([0-9]{2})-([0-9]{2})_[0-9]{2}-[0-9]{2}-[0-9]{2}', dir)
    if match == None:
      continue
    (subdir, year, month, day) = match.groups()
    match = re.match(r'.*/(.*)', dir)
    if match == None:
      continue
    leaf = match.groups()[0]
    newdir = "%s/%s/%s/%s/%s" % (subdir, year, month, day, leaf)
    print 'Moving %s to %s' % (dir, newdir)
    if exists(newdir):
      print 'New dir %s already exists, removing' % (newdir)
      #shutil.rmtree(newdir)
    newdir = newdir[:-len(leaf)-1]
    if not exists(newdir):
      makedirs(newdir)
    shutil.move(dir, newdir)
'''

if __name__ == '__main__':
  print 'Renaming demos'
  rootdir = u'/cygdrive/U/demos/west_coast_pug'
  for dir in listdir(rootdir):
    dir = join(rootdir, dir)
    print dir
    match = re.match(r'.*/([0-9]{4})/([0-9]{2})/([0-9]{2})/', dir)
    if match != None:
      continue
    match = re.match(r'(.*)/(.*) ([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2})-([0-9]{2})-([0-9]{2})', dir)
    if match == None:
      match = re.match(r'(.*)/(.*) ([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2})([0-9]{2})([0-9]{2})', dir)
      if match == None:
        continue
    (subdir, map, year, month, day, hour, minute, second) = match.groups()
    #match = re.match(r'.*/(.*)', dir)
    #if match == None:
    #  continue
    #leaf = match.groups()[0]
    leaf = "%s %s-%s-%s_%s-%s-%s" % (map, year, month, day, hour, minute, second)
    newdir = "%s/%s/%s/%s/%s" % (subdir, year, month, day, leaf)
    print 'Moving %s to %s' % (dir, newdir)
    for file in listdir(dir):
      if (file.endswith(".dm_26")):
        fullfile = "%s/%s" % (dir, file)
        newfullfile = "%s/%s" % (newdir, file)
        for cur in demodb.find({'_id': fullfile[len(basedir):]}):
          print cur['_id']
          cur['_id'] = newfullfile[len(basedir):]
          try:
            demodb.insert(cur)
          except DuplicateKeyError:
            print 'New doc %s already exists' % (cur['_id'])
            pass
          demodb.remove({'_id': fullfile[len(basedir):]})
        for cur in fulldb.find({'_id': fullfile}):
          print cur['_id']
          cur['_id'] = newfullfile
          try:
            fulldb.insert(cur)
          except DuplicateKeyError:
            print 'New doc %s already exists' % (cur['_id'])
            pass
          fulldb.remove({'_id': fullfile})
    if exists(newdir):
      print 'New dir %s already exists, removing' % (newdir)
      shutil.rmtree(newdir)
    newdir = newdir[:-len(leaf)-1]
    if not exists(newdir):
      makedirs(newdir)
    shutil.move(dir, newdir)
      
