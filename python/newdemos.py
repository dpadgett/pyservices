#!/usr/bin/python -u
from subprocess import Popen, PIPE
import time
import sys
import os
import traceback
import urlparse
import zipfile

from pymongo import MongoClient

since = int(sys.argv[1])

from datetime import datetime
import pytz

#since = datetime.fromtimestamp(since, pytz.utc)

db = MongoClient("mongodb").demos

#print 'Looking from', start, 'to', end

basedir = u'/cygdrive/U/demos/'
demos = [demo['_id'].encode('utf8') for demo in db.mindemos.find({'mt':{'$gte':since}},{'_id':1})]

for demo in demos:
  print demo

