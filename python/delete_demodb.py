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

matchids = sys.argv[1:]

demodb = db.mindemos

updates = demodb_lib.DemoDbLib()

for matchid in matchids:
  print 'Processing match: ' + matchid.encode('utf8')
  matches = db.minmatches.find({'_id': matchid})
  match = None
  for match in matches:
    break
  demos = [d['id'] for d in match['d']]
  print demos
  updates.base_matches[matchid] = match
  for demo in demos:
    demo = db.mindemos.find_one_and_delete({'_id': demo})
    updates.update_maps(demo, None)
    updates.update_servers(demo, None)
  db.playerGames.delete_many({'_id.match': matchid})
  db.minmatches.delete_one({'_id': matchid})

updates.flush()
