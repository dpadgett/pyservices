#!/usr/bin/python
# script to generate user match histories based on ip / guid logs

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
  playerdb = db.players3
  playergamedb = db.playerGames
  basedir = u'/cygdrive/U/demos/'

  res = playergamedb.aggregate([
    {'$limit': 100},
    {'$group': {
      '_id': '$_id.player',
      'num_games': {'$sum': 1},
      'names': {'$push': '$names'}
    }}
  ])
  for row in res:
    print row
  '''
  playerids = [id for id in playergamedb.distinct('_id.player')]
  curplayerids = [id for id in playerdb.distinct('_id')]
  remaining = [p for p in playerids if p not in curplayerids]
  for playerid in remaining:
    matches = [m for m in playergamedb.find({'_id.player': playerid}).sort('time', -1)]
    player = user_lib.recreate_player(playerid, matches[:25])
    player['num_games'] = len(matches)
    player['num_matches'] = playergamedb.find({'_id.player': playerid, 'is_match': True}).count()
    for ratedmatch in playergamedb.find({'_id.player': playerid, 'rating': {'$exists': True}}).sort('time', -1).limit(1):
      player['rating'] = ratedmatch['rating']['updated']
    player['time'] = 0
    for match in matches:
      for name in match.get('names', []):
        player['time'] += name['name_end_time'] - name['name_start_time']
    print player
    playerdb.save(player)
  '''
