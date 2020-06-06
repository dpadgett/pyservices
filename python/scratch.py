#!/usr/bin/python

from os import listdir, stat, utime
from os.path import isfile, isdir, join, exists, dirname, basename
import hashlib
import struct
import re
import sys

from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *
import commands

import datetime

import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
import traceback

import merge_metadata
from bson.objectid import ObjectId

import shrinker
import user_lib

if __name__ == '__main__':
  db = MongoClient("mongodb").demos
  #demodb = db.demos
  mindemodb = db.mindemos
  matchdb = db.minmatches
  ipdb = db.minips
  playerdb = db.players2
  playergamedb = db.playerGames
  basedir = u'/cygdrive/U/demos/'
  
  #match = shrinker.inflate_match(matchdb.find({'_id': '13fe277520d1f31e'}).next())
  #print len(match['scores'])
  #demos = mindemodb.find({'_id': {'$regex': 'fim/autorecord/2020/04/16/mpctf4 2020-04-16_19-23-06/.*'}}, {'mt':1})
  #mintime = int((datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%s"))
  #print mintime
  #demos = mindemodb.find({'mt': {'$gt': mintime}})
  demos = mindemodb.find({'_id': {'$regex': 'pug/2020/04/26/mpctf_kothis 2020-04-26_19-27-46/.*'}}, {'mt':1})
  for demo in demos:
    print demo
    #print demo['_id']
    #print demo['_id'] + '.dm_meta'
