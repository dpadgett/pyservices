#!/usr/bin/python
# -*- coding: utf-8 -*-
# version of merge_matches which uses prediction to generate any missing povs
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import pymongo
import re
import random
import json
import shlex
from os.path import isfile, isdir, join, exists
from os import listdir
import zipfile
import sys
import hashlib
import struct

from dateutil.parser import *
from dateutil.tz import *
from datetime import *
from dateutil.relativedelta import *
from pytz import timezone

from subprocess import Popen, PIPE, call
from os import listdir, stat, remove
from os.path import isfile, isdir, join, exists, basename

import trueskill

from bson.objectid import ObjectId

import merge_metadata
import shrinker
import traceback

import shrinker
import copy

def strip_colors(string):
  ''' Returns the string with color codes stripped'''
  idx = 0
  result = ''
  while idx < len(string):
    if string[idx] == '^' and idx < len(string) - 1 and ord(string[idx + 1]) >= ord('0') and ord(string[idx + 1]) <= ord('9'):
      idx += 2
      continue
    result += string[idx]
    idx += 1
  return result

if __name__ == '__main__':
  db = MongoClient("mongodb").demos
  #demodb = db.demos
  matchdb = db.minmatches
  playerdb = db.players
  playergamedb = db.playerGames
  ratingdb = db.playerGameRatings
  sessiongamedb = db.sessionGames
  sessionplayerdb = db.sessionPlayers
  sessiondb = db.sessions
  
  '''
  sessions = [s['_id'] for s in sessiondb.find({'playerid': ObjectId('55dc3d0acb15c73790d832d7')}, {})]
  matches = set([sg['_id']['match'] for sg in sessiongamedb.find({'_id.session': {'$in': sessions}, 'is_match': True}, {})])
  print len(matches)
  ratedmatches = set([r['_id']['match'] for r in ratingdb.find({'_id.player': ObjectId('55dc3d0acb15c73790d832d7')}, {})])
  print len(ratedmatches)
  missing = matches - ratedmatches
  print len(missing)
  print missing
  #matches = matchdb.find({'_id': '038ac62eeb06d6a1'}).sort('t', 1)
  '''
  
  '''
  ducksessions = [s['_id'] for s in sessiondb.find({'playerid':ObjectId('58665935cb15c72b1c0acf25')}, {})]
  skznames = [u'^1skz', u'^1SkyZ', u'^1SkZ']
  ducknames = [u'^5^^0\xd0u^7\xa2k', u'^5^^0S^7an\xa0^5^^0H^7ol\xf8']
  for s in ducksessions:
    names = sessiongamedb.distinct('games.names.name',{'_id.session':s})
    skz = [n for n in names if n in skznames]
    duck = [n for n in names if n in ducknames]
    if len(skz) > 0 and len(duck) > 0:
      print s, names
  '''
  
  '''
  ids = [m['_id']['session'] for m in sessiongamedb.find({'_id.match': '22f6e4eb467f9fc0'}, {})]
  ids.sort()
  for id in ids:
    print id
  games = [m for m in sessiongamedb.find({'_id.match': '22f6e4eb467f9fc0', '_id.session.guid': 31740})]
  for game in games:
    print game['_id'], [n['name'] for g in game['games'] for n in g['names']]
  '''
  
  # resets player mapping for 1 player id
  resetplayerid = ObjectId('55dc4895cb15c73790d86427')
  sessionplayerdb.delete_one({'_id': resetplayerid})
  sessiondb.update_many({'playerid': resetplayerid}, {'$unset': {'playerid':1}})
  ratingdb.delete_many({'_id.player': resetplayerid})
