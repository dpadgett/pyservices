#!/usr/bin/python
# script to generate user match histories based on ip / guid logs

from os import listdir, stat, utime
from os.path import isfile, isdir, join, exists, dirname, basename
import hashlib
import struct
import re

from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *

import datetime

import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
import traceback

def getScore(match, client_num):
  scores = None
  for sc in match['sc']:
    scores = sc
    if sc['fi'] != 0:
      break
  team = None
  for teamname, key in [['red', 'r'], ['blue', 'b'], ['spec', 's'], ['free', 'f']]:
    if scores.get(key) == None:
      continue
    for player in scores[key]:
      if player['c'] == client_num:
        team = teamname
        break
  return {'blue': scores.get('bs'), 'red': scores.get('rs'), 'team': team}

if __name__ == "__main__":
  db = MongoClient().demos
  demodb = db.demos
  matchdb = db.minmatches
  ipdb = db.minips
  playerdb = db.players
  playergamedb = db.playerGames
  for player in playerdb.find():
    games = playergamedb.find({'_id.player': player['_id']})
    has_games = False
    for game in games:
      has_games = True
      break
    if has_games:
      print 'Player %s already has games' % (player['_id'])
      continue
    playerid = player['_id']
    for summary in player['matches']:
      matches = matchdb.find({'_id': summary['id']})
      for match in matches:
        break
      playergame = {'_id': {'player': playerid, 'match': summary['id']},
          'client_num': summary['client_num'],
          'names': summary['names'],
          'time': summary['time'],
          'is_match': summary['is_match'],
          'map': match['n'],
          'score': getScore(match, summary['client_num'])}
      print playergame['_id']
      playergamedb.save(playergame)
    print player['_id']
