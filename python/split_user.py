#!/usr/bin/python
# script to split a user given a list of games to split off

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

from bson.objectid import ObjectId

import user_lib

import json

if __name__ == '__main__':
  db = MongoClient("mongodb").demos
  #demodb = db.demos
  mindemodb = db.mindemos
  matchdb = db.minmatches
  ipdb = db.minips
  playerdb = db.players
  playergamedb = db.playerGames
  basedir = u'/cygdrive/U/demos/'

  games_to_split = json.loads(sys.stdin.read())

  print 'Splitting games into a new player'

  playergames = []
  oldplayerids = []
  for matchid in games_to_split:
    #matchid['player']['$id'] = '5c57743f3f926d1e1b2652e1'
    matches = [m for m in playergamedb.find({'_id.player': ObjectId(matchid['player']['$id']), '_id.match': matchid['match']})]
    if len(matches) != 1:
      print 'Couldnt find match', matchid
      break
    playergames.extend(matches)
    if ObjectId(matchid['player']['$id']) not in oldplayerids:
      oldplayerids.append(ObjectId(matchid['player']['$id']))
  if len(playergames) != len(games_to_split):
    print 'Couldnt find all games, bailing'
    sys.exit(1)

  player = {}
  playerid = playerdb.save(player)
  #playerid = ObjectId('55dc48b3cb15c73790d86482')
  print 'Split player id:', playerid

  for playergame in playergames:
    print 'Moving', playergame['_id']
    oldid = {'player': playergame['_id']['player'], 'match': playergame['_id']['match']}
    playergame['_id']['player'] = playerid
    playergamedb.save(playergame)
    playergamedb.remove({'_id': oldid})

  #player = user_lib.recreate_player(playerid, playergames)
  #playerdb.save(player)
  oldplayerids.append(playerid)
  for playerid in oldplayerids:
    print 'Recreating player', playerid
    playergames = [g for g in playergamedb.find({'_id.player': playerid})]
    player = user_lib.recreate_player(playerid, playergames)
    playerdb.save(player)
  print 'Player created'
  print player
