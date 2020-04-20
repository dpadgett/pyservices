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
  playerdb = db.players2
  playergamedb = db.playerGames
  basedir = u'/cygdrive/U/demos/'

  playerids = [id for id in playergamedb.distinct('_id.player')]
  curplayerids = set([id for id in playerdb.distinct('_id')])
  remaining = [p for p in playerids if p not in curplayerids]
  chunksize = 50
  remaining = [remaining[idx:idx+chunksize] for idx in range(0, len(remaining), chunksize)]
  for chunk in remaining:
    bulk = playerdb.initialize_unordered_bulk_op()
    gamecur = playergamedb.find({'_id.player': {'$in': chunk}})
    games = {}
    for game in gamecur:
      games[game['_id']['player']] = games.get(game['_id']['player'], []) + [game]
    for playerid in chunk:
      matches = games[playerid]
      player = user_lib.recreate_player(playerid, matches)
      print player
      bulk.find({'_id': player['_id']}).upsert().replace_one(player)
    result = bulk.execute()
    print result
