#!/usr/bin/python -u

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
import pytz
from pytz import timezone
import traceback

import shrinker
import copy

class DemoDbLib(object):
  def update_maps(self, old, new):
    if old != None:
      for map in old['m']['m']:
        cur_value = 0
        if map['n'] in self.map_updates:
          cur_value = self.map_updates[map['n']]
        self.map_updates[map['n']] = cur_value - (map['e'] - map['s'])
    if new != None:
      for map in new['m']['m']:
        cur_value = 0
        if map['n'] in self.map_updates:
          cur_value = self.map_updates[map['n']]
        self.map_updates[map['n']] = cur_value + (map['e'] - map['s'])
    #print 'map updates', self.map_updates
    return self.map_updates

  def update_servers(self, old, new):
    if old != None and 'h' in old['m']:
      server = old['m']['h']
      for map in old['m']['m']:
        cur_value = 0
        if server in self.server_updates:
          cur_value = self.server_updates[server]
        self.server_updates[server] = cur_value - (map['e'] - map['s'])
    if new == None or 'h' not in new['m']:
      print 'Error: Couldn\'t find server in', new
      return self.server_updates
    server = new['m']['h']
    for map in new['m']['m']:
      cur_value = 0
      if server in self.server_updates:
        cur_value = self.server_updates[server]
      self.server_updates[server] = cur_value + (map['e'] - map['s'])
    #print 'server updates', self.server_updates
    return self.server_updates

  def get_match(self, id):
    if id in self.match_updates:
      return self.match_updates[id]
    cursor = self.db.minmatches.find({'_id': id})
    for match in cursor:
      self.match_updates[id] = match
      self.base_matches[id] = match
      return match
    self.match_updates[id] = None
    return None

  def update_matches(self, old, new):
    # can skip the initial projection, which is done just to save space in unwind
    if old != None:
      for map in old['m']['m']:
        id = map['h']
        print id
        match = self.get_match(id)
        if match != None:
          # find the index of this demo in the demo list
          indexes = []
          for index, val in enumerate(match['d']):
            if val['id'] == old['_id']:
              indexes.append(index)
          # remove it from the demo list and its scores from the scores list
          match['d'][:] = [d for index, d in enumerate(match['d']) if index not in indexes]
          match['sc'][:] = [sc for index, sc in enumerate(match['sc']) if index not in indexes]
          if len(match['d']) == 0:
            self.match_updates[id] = None
          # the rest can't be properly resolved without re-fetching everything so don't bother
    if new != None:
      for map in new['m']['m']:
        id = map['h']
        print id
        match = self.get_match(id)
        if match == None:
          match = {'_id': id, 'd': [], 's': None, 'e': None, 'n': None, 'sc': [], 'h': None, 't': None, 'ma': None}
          self.match_updates[id] = match
        match['d'].append({'id': new['_id'], 'c': new['m']['c']['id'], 'n': new['p']})
        match['sc'].append(map['sc'])
        if match['h'] == None:
          match['h'] = new['m']['h']
        if match['s'] == None:
          match['s'] = map['s']
        else:
          match['s'] = min(match['s'], map['s'])
        if match['e'] == None:
          match['e'] = map['e']
        else:
          match['e'] = max(match['e'], map['e'])
        if match['n'] == None:
          match['n'] = map['n']
        if match['t'] == None:
          match['t'] = new['t']
        else:
          #print match['t'], new['t']
          matchtime = match['t']
          if matchtime.tzinfo is None:
            matchtime = matchtime.replace(tzinfo=pytz.UTC)
          match['t'] = min(matchtime, new['t'])
        if match['ma'] == None:
          match['ma'] = map['ma']
        else:
          match['ma'] = match['ma'] or map['ma']
        #print 'match updates', self.match_updates
    return self.match_updates

  def update_names(self):
    self.name_updates = {}
    for id, match in self.base_matches.iteritems():
      if match == None:
        continue
      for scores in match['sc']:
        for team in ['b', 'r', 's', 'f']:
          if team not in scores or scores[team] == None:
            continue
          for player in scores[team]:
            cur_value = 0
            if player['n'] in self.name_updates:
              cur_value = self.name_updates[player['n']]
            self.name_updates[player['n']] = cur_value - player['t']
    for id, match in self.match_updates.iteritems():
      if match == None:
        continue
      for scores in match['sc']:
        for team in ['b', 'r', 's', 'f']:
          if team not in scores or scores[team] == None:
            continue
          for player in scores[team]:
            cur_value = 0
            if player['n'] in self.name_updates:
              cur_value = self.name_updates[player['n']]
            self.name_updates[player['n']] = cur_value + player['t']
    final_name_updates = {}
    for name, value in self.name_updates.iteritems():
      if value != 0:
        final_name_updates[name] = value
    #print 'name updates', name_updates
    self.name_updates = final_name_updates

  def __init__(self):
    self.db = MongoClient("mongodb").demos
    self.basedir = u'/cygdrive/U/demos/'
    self.demodb = self.db.mindemos
    self.base_matches = {}
    self.map_updates = {}
    self.server_updates = {}
    self.match_updates = {}

  def update_demo(self, wrappeddemometa):
    try:
      oldwrappeddemometa = self.demodb.find_one_and_replace(filter = {'_id': wrappeddemometa['_id']}, replacement = wrappeddemometa, upsert = True)
      self.update_maps(oldwrappeddemometa, wrappeddemometa)
      self.update_servers(oldwrappeddemometa, wrappeddemometa)
      self.update_matches(oldwrappeddemometa, wrappeddemometa)
      demoid = wrappeddemometa['_id']
      print 'Upserted', demoid.encode('utf8')
    except DuplicateKeyError:
      print 'Skipped duplicate, already in db'
      return
    except:
      print 'Error on', wrappeddemometa
      print traceback.format_exc()
      exit()

  def rename_demo(self, oldid, wrappeddemometa):
    if oldid == wrappeddemometa['_id']:
      print 'Renaming to same id:', oldid
      return
    try:
      self.demodb.insert(wrappeddemometa)
      oldwrappeddemometa = self.demodb.find_one_and_delete(filter = {'_id': oldid})
      if oldwrappeddemometa == None:
        print "Didn't find ", oldid
        return
      self.update_maps(oldwrappeddemometa, wrappeddemometa)
      self.update_servers(oldwrappeddemometa, wrappeddemometa)
      self.update_matches(oldwrappeddemometa, wrappeddemometa)
      demoid = wrappeddemometa['_id']
      print 'Upserted', demoid.encode('utf8')
    except DuplicateKeyError:
      print 'Skipped duplicate, already in db'
      return
    except:
      print 'Error on', wrappeddemometa
      print traceback.format_exc()
      exit()

  def flush(self):
    self.update_names()

    # apply the various updates
    if len(self.map_updates) > 0:
      bulk = self.db.minmaps.initialize_unordered_bulk_op()
      for map, delta in self.map_updates.iteritems():
        bulk.find({'_id': map}).upsert().update({'$inc': {'t': delta}})
      map_result = bulk.execute()
      print 'executed map writes:', map_result

    if len(self.server_updates) > 0:
      bulk = self.db.minservers.initialize_unordered_bulk_op()
      for server, delta in self.server_updates.iteritems():
        bulk.find({'_id': server}).upsert().update({'$inc': {'t': delta}})
      server_result = bulk.execute()
      print 'executed server writes:', server_result

    if len(self.name_updates) > 0:
      bulk = self.db.minnames.initialize_unordered_bulk_op()
      for name, delta in self.name_updates.iteritems():
        bulk.find({'_id': name}).upsert().update({'$inc': {'t': delta}})
      name_result = bulk.execute()
      print 'executed name writes:', name_result

    if len(self.match_updates) > 0:
      bulk = self.db.minmatches.initialize_unordered_bulk_op()
      for hash, match in self.match_updates.iteritems():
        if match == None:
          bulk.find({'_id': hash}).remove_one()
        else:
          bulk.find({'_id': hash}).upsert().replace_one(match)
      try:
        match_result = bulk.execute()
      except BulkWriteError as bwe:
        match_result = bwe.details
      print 'executed match writes:', match_result
