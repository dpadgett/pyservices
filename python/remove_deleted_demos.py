#!/usr/bin/python

from subprocess import Popen, PIPE
from os import listdir, stat
from os.path import isfile, isdir, join, exists
import locale
import sys
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import json
from dateutil.parser import *
from dateutil.tz import *
from datetime import *
from dateutil.relativedelta import *
from pytz import timezone
import math
from bson.code import Code
from collections import deque
import hashlib

#exit(0)

basedir = u'/cygdrive/U/demos'
basepaths = [basedir]#[basedir + u'/japlus', basedir + u'/pug']

directories = []
tovisit = deque(basepaths)
while len(tovisit) > 0:
  dir = tovisit.popleft()
  childdirs = [ join(dir,f) for f in listdir(dir) if isdir(join(dir,f)) ]
  tovisit.extend(childdirs)
  directories.append(dir)
  directories.extend(childdirs)

demos = [ join(d,file) for d in directories for file in listdir(d)
  if not ".dm_meta" in file and (exists(join(d,file + ".dm_meta"))
    and stat(join(d,file)).st_mtime <= stat(join(d,file + ".dm_meta")).st_mtime) ]

db = MongoClient().demos
demodb = db.demos
existing_demos = {}
for doc in demodb.find({}, { 'metadata_mtime': 1 }):
  if 'metadata_mtime' in doc:
    existing_demos[doc['_id']] = doc['metadata_mtime']
current_demos = {}
for demo in demos:
  mtime = stat( demo + u'.dm_meta' ).st_mtime
  current_demos[demo] = mtime
  if demo in existing_demos and existing_demos[demo] >= mtime:
    #print 'Skipping already-present demo'
    continue
for demo in existing_demos.keys():
  if demo not in current_demos:
    print 'Removing demo', demo, 'which no longer exists'
    print demodb.remove({'_id': demo})
