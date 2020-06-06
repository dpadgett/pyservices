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

def hashFor(data):
  hasher = hashlib.sha1()
  hasher.update(data)
  return struct.unpack('!i', hasher.digest()[:4])[0]

def hashForIp(ip):
  pieces = [int(num) for num in ip.split('.')]
  packedip = struct.pack('BBBB', pieces[3], pieces[2], pieces[1], pieces[0])
  return hashFor(packedip)

def ipIsBlacklisted(ip):
  return ip == hashForIp('0.0.0.0') or ip == hashFor('') or ip == 0 or ip == -1 or ip == hashForIp('104.239.162.135') or ip == hashForIp('37.187.199.239')

if __name__ == '__main__':
  db = MongoClient("mongodb").demos
  #demodb = db.demos
  mindemodb = db.mindemos
  matchdb = db.minmatches
  ipdb = db.minips
  playerdb = db.players
  playergamedb = db.playerGames
  sessiongamedb = db.sessionGames
  sessiondb = db.sessions
  sessionplayerdb = db.sessionPlayers
  basedir = u'/cygdrive/U/demos/'

  #start = parse(commands.getoutput("/bin/date") + ' -0800') + relativedelta(hours=-12)
  #start = datetime.datetime(2020, 4, 23, 12, 0, 0)
  start = datetime.datetime(2016, 8, 15)

  if len(sys.argv) > 1:
    sessions = sessiondb.find({'_id': {'$in': sys.argv[1:]}}).sort('t', pymongo.ASCENDING).skip(0).batch_size(30)
  else:
    #db.sessions.find({'playerid':ObjectId('55dc3ed9cb15c73790d83e03')},{'last_name':1}).sort({'first_game':1})
    #sessions = sessiondb.find({'playerid': {'$exists': False}}).sort('first_game', pymongo.ASCENDING).skip(0).batch_size(30)
    #sessions = sessiondb.find({}).sort('first_game', pymongo.ASCENDING).skip(0).batch_size(30)
    '''
    sessionplayerdb.drop()
    sessiondb.update_many({'playerid': {'$exists': True}}, {'$unset': {'playerid':1}})
    '''
    sessions = sessiondb.find({'playerid': {'$exists': False}}).sort('first_game', pymongo.ASCENDING).skip(0).batch_size(30)

  num_sessions = 0
  chunksize = 50
  bulk = sessionplayerdb.initialize_unordered_bulk_op()
  sessbulk = sessiondb.initialize_unordered_bulk_op()
  playerids_by_key = {}
  for session in sessions:
    print num_sessions, session['_id'], session['first_game'], session['last_game'], session['num_games']
    num_sessions += 1
    
    sessionid = session['_id']
    def search(key):
      playerid = playerids_by_key.get('%s=%s' % (key, str(sessionid[key])), None)
      if playerid != None:
        return playerid
      matches = sessiondb.find({'_id.' + key: sessionid[key], 'first_game': {'$lt': session['first_game']}}).sort('first_game', pymongo.DESCENDING)
      for match in matches:
        if 'playerid' in match:
          return match['playerid']
      return None
    playerid = None
    if 'newmod_id' in sessionid and len(sessionid['newmod_id']) > 8:
      playerid = search('newmod_id')
      if playerid == None:
        print 'No Match by newmod id'
      else:
        print 'MATCH by newmod id!'
    if playerid == None and 'guid' in sessionid and sessionid['guid'] > 1000:
      playerid = search('guid')
      if playerid == None:
        print 'No Match by guid'
      else:
        print 'MATCH by guid!'
    '''if playerid == None:
      # don't try IP, too risky
      playerid = search('ip')
      if playerid == None:
        print 'No Match by ip'
      else:
        print 'MATCH by ip!'
      pass
    '''
    # last fallback, is to use old player db.  since many were already manually merged there.
    if playerid is None and 'guid' in sessionid and 'ip' in sessionid:
      # db.playerGames.find({'names': {'$elemMatch': {'guid_hash': 10, 'ip_hash': 10}}}, {'_id.player':1}).hint("names.guid_hash_1_names.ip_hash_1").explain()
      #oldmatches = playergamedb.distinct('_id.player', {'names': {'$elemMatch': {'guid_hash': sessionid['guid'], 'ip_hash': sessionid['ip']}}})
      # it was using ip_hash only index.  so do it via find() instead which lets us give a hint.
      oldmatches = set([p['_id']['player'] for p in playergamedb.find({'names': {'$elemMatch': {'guid_hash': sessionid['guid'], 'ip_hash': sessionid['ip']}}}, {'_id.player'}).hint("names.guid_hash_1_names.ip_hash_1")])
      if len(oldmatches) == 1:
        playerid = next(iter(oldmatches))
        print 'MATCH by old player:', playerid
        # check if it already exists
        exists = len([p for p in sessionplayerdb.find({'_id': playerid})]) > 0
        if not exists:
          sessionplayerdb.save({'_id': playerid})
      elif len(oldmatches) > 1:
        print 'Found multiple old matches:', oldmatches

    # special cases caused by config sharing
    if playerid == ObjectId('58665935cb15c72b1c0acf25') and sessionid.get('guid', None) != 15636:
      # skyz and duck
      playerid = None
      print 'Ignoring match due to config sharing!'
    if playerid == ObjectId('589fe62e87b4d01111f912c8') and sessionid.get('guid', None) != 31740:
      # aP.Corvo and aP.Aguia
      playerid = None
      print 'Ignoring match due to config sharing!'

    if playerid == None:
      playerid = sessionplayerdb.save({})
    sessbulk.find({'_id': sessionid}).update_one({'$set': {'playerid': playerid}})
    bulk.find({'_id': playerid}).update_one({'$inc': {'num_sessions': 1, 'num_games': session['num_games'], 'num_matches': session['num_matches'], 'time': session['time']}, '$set': {'last_name': session['last_name']}})
    playerids_by_key['guid=%d' % (sessionid['guid'])] = playerid
    if 'newmod_id' in sessionid:
      playerids_by_key['newmod_id=%s' % (sessionid['newmod_id'])] = playerid
    if (num_sessions % chunksize) == 0:
      #exit()
      try:
        print bulk.execute()
      except BulkWriteError as bwe:
        print bwe.details
      try:
        print sessbulk.execute()
      except BulkWriteError as bwe:
        print bwe.details
      bulk = sessionplayerdb.initialize_unordered_bulk_op()
      sessbulk = sessiondb.initialize_unordered_bulk_op()
    #break
  # clean any orphaned player entries.  technically shouldn't matter much other than cleaning garbage.
  playerids = set(sessiondb.distinct('playerid'))
  allplayerids = set(sessionplayerdb.distinct('_id'))
  for orphaned_id in allplayerids - playerids:
    bulk.find({'_id': orphaned_id}).remove_one()
  try:
    print bulk.execute()
  except BulkWriteError as bwe:
    print bwe.details
  try:
    print sessbulk.execute()
  except BulkWriteError as bwe:
    print bwe.details
