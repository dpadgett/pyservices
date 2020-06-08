#!/usr/bin/python -u
import time
import sys
import os
import traceback
import urlparse

from pymongo import MongoClient
import pymongo
from pymongo.errors import BulkWriteError

from bson.objectid import ObjectId
import json
import bson.json_util

def merge_players(ids):
  db = MongoClient("mongodb").demos
  playerdb = db.sessionPlayers
  sessiondb = db.sessions
  ratingdb = db.playerGameRatings
  for id in ids[1:]:
    sessiondb.update_many({'playerid': id}, {'$set': {'playerid': ids[0]}})
    oldplayer = None
    for oldplayer in playerdb.find({'_id': id}):
      break
    playerdb.update_one({'_id': ids[0]}, {'$inc': {'num_sessions': oldplayer['num_sessions'], 'num_games': oldplayer['num_games'], 'num_matches': oldplayer['num_matches'], 'time': oldplayer['time']}})
    playerdb.delete_one({'_id': id})
    ratings = [r for r in ratingdb.find({'_id.player': id})]
    for rating in ratings:
      rating['_id']['player'] = ids[0]
    ratingdb.delete_many({'_id.player': id})
    if len(ratings) > 0:
      try:
        ratingdb.insert_many(ratings)
      except BulkWriteError as bwe:
        print bwe.details
  # fix last_name field
  session = None
  for session in sessiondb.find({'playerid': ids[0]}).sort('last_game', pymongo.DESCENDING).limit(1):
    break
  updated = playerdb.find_one_and_update({'_id': ids[0]}, {'$set': {'last_name': session['last_name']}})
  return updated
