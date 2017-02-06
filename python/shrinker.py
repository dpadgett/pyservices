#!/usr/bin/python

# library for deflating/inflating demo metadata, for storage in mongodb

def rekey(data, keymap):
  for key, value in data.copy().iteritems():
    if key in keymap:
      if keymap[key] == key:
        continue
      data[keymap[key]] = data[key]
    del data[key]

def invert(keymap):
  return {v: k for k, v in keymap.iteritems()}

def namemapping():
  return {
    'n': 'name',
    'g': 'guid_hash',
    'i': 'ip_hash',
    'b': 'is_bot',
    's': 'name_start_time',
    'e': 'name_end_time',
    'sr': 'name_start_time_raw',
    'er': 'name_end_time_raw'}

def minimize_name(name):
  rekey(name, invert(namemapping()))

def inflate_name(name):
  rekey(name, namemapping())

def playermapping():
  return {
    'c': 'client',
    'n': 'client_name',
    's': 'score',
    'p': 'ping',
    't': 'time',
    'te': 'team'}

def scoremapping():
  return {
    'fi': 'is_final',
    'rs': 'red_score',
    'bs': 'blue_score',
    'f': 'freeplayers',
    'r': 'redplayers',
    'b': 'blueplayers',
    's': 'specplayers'}

def minimize_scores(scores):
  for key in ['freeplayers', 'redplayers', 'blueplayers', 'specplayers']:
    if key in scores:
      for player in scores[key]:
        rekey(player, invert(playermapping()))
  rekey(scores, invert(scoremapping()))

def inflate_scores(min_scores):
  rekey(min_scores, scoremapping())
  teams = {'freeplayers': 'FREE', 'redplayers': 'RED', 'blueplayers': 'BLUE', 'specplayers': 'SPECTATOR'}
  for team, teamval in teams.iteritems():
    if team in min_scores and min_scores[team] != None:
      for player in min_scores[team]:
        rekey(player, playermapping())
        if 'team' not in player:
          player['team'] = teamval

def mapmapping():
  return {
    'n': 'mapname',
    's': 'map_start_time',
    'e': 'map_end_time',
    'sc': 'scores',
    'na': 'names',
    'ma': 'is_match',
    'h': 'match_hash'}

def minimize_map(map):
  if 'names' in map:
    for clientid, names in map['names'].iteritems():
      for name in names:
        minimize_name(name)
  if 'scores' in map:
    minimize_scores(map['scores'])
  rekey(map, invert(mapmapping()))

def inflate_map(map):
  rekey(map, mapmapping())
  if 'scores' in map:
    inflate_scores(map['scores'])
  if 'names' in map:
    for clientid, names in map['names'].iteritems():
      for name in names:
        inflate_name(name)

def metadatamapping():
  return {
    'c': 'client',
    'h': 'sv_hostname',
    'm': 'maps',
    'v': 'version'}

# returns a minimized copy of the demo metadata in data
def minimize_metadata(metadata):
  for map in metadata['maps']:
    minimize_map(map)
  rekey(metadata, invert(metadatamapping()))
  return metadata

def inflate_metadata(metadata):
  rekey(metadata, metadatamapping())
  # default metadata to version 3
  if 'version' not in metadata:
    metadata['version'] = 3
  if 'maps' in metadata:
    for map in metadata['maps']:
      inflate_map(map)
  return metadata

def toplevelmapping():
  return {
    'p': 'player',
    'ma': 'is_match',
    'h': 'match_hash',
    't': 'time_created',
    'mt': 'metadata_mtime',
    'm': 'metadata',
    '_id': '_id'}

def minimize(data):
  data['_id'] = data['_id'][len('/cygdrive/U/demos/'):]
  if 'metadata' in data:
    minimize_metadata(data['metadata'])
  rekey(data, invert(toplevelmapping()))
  return data

# returns an inflated copy of the minimized demo data in min_data
# should be generic enough to support inflation of data with some filtered out keys
def inflate(data):
  data['_id'] = '/cygdrive/U/demos/' + data['_id']
  rekey(data, toplevelmapping())
  if 'metadata' in data:
    inflate_metadata(data['metadata'])
  return data
