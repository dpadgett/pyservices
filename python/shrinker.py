#!/usr/bin/python

# library for deflating/inflating demo metadata, for storage in mongodb

def rekey(data, keymap, keep_unknown=False):
  for key, value in data.copy().iteritems():
    if key in keymap:
      if keymap[key] == key:
        continue
      data[keymap[key]] = data[key]
      del data[key]
      continue
    if not keep_unknown:
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

def teammapping():
  return {
    't': 'team',
    's': 'team_start_time',
    'e': 'team_end_time',
    'sr': 'team_start_time_raw',
    'er': 'team_end_time_raw'}

def minimize_team(team):
  rekey(team, invert(teammapping()))

def inflate_team(team):
  rekey(team, teammapping())

def newmodmapping():
  return {
    'i': 'newmod_id',
    's': 'newmod_start_time',
    'e': 'newmod_end_time',
    'sr': 'newmod_start_time_raw',
    'er': 'newmod_end_time_raw'}

def minimize_newmod(newmod):
  rekey(newmod, invert(newmodmapping()))

def inflate_newmod(newmod):
  # until 2020-09-14, newmod was accidentally inserted unshrunk,
  # so need to keep the unshrunk keys for these
  rekey(newmod, newmodmapping(), keep_unknown=True)

def bookmarkmapping():
  return {
    't': 'time',
    'tr': 'time_raw',
    'm': 'mark'}

def minimize_bookmarks(bookmarks):
  for bookmark in bookmarks:
    rekey(bookmark, invert(bookmarkmapping()))

def inflate_bookmarks(bookmarks):
  for bookmark in bookmarks:
    rekey(bookmark, bookmarkmapping())

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
    'te': 'teams',
    'nm': 'newmod',
    'ma': 'is_match',
    'h': 'match_hash',
    'bm': 'bookmarks'}

def minimize_map(map):
  if 'names' in map:
    for clientid, names in map['names'].iteritems():
      for name in names:
        minimize_name(name)
  if 'teams' in map:
    for clientid, teams in map['teams'].iteritems():
      for team in teams:
        minimize_team(team)
  if 'newmod' in map:
    for clientid, newmods in map['newmod'].iteritems():
      for newmod in newmods:
        minimize_newmod(newmod)
  if 'scores' in map:
    minimize_scores(map['scores'])
  if 'bookmarks' in map:
    minimize_bookmarks(map['bookmarks'])
  rekey(map, invert(mapmapping()))

def inflate_map(map):
  rekey(map, mapmapping())
  if 'scores' in map:
    inflate_scores(map['scores'])
  if 'names' in map:
    for clientid, names in map['names'].iteritems():
      for name in names:
        inflate_name(name)
  if 'teams' in map:
    for clientid, teams in map['teams'].iteritems():
      for team in teams:
        inflate_team(team)
  if 'newmod' in map:
    for clientid, newmods in map['newmod'].iteritems():
      for newmod in newmods:
        inflate_newmod(newmod)
  if 'bookmarks' in map:
    inflate_bookmarks(map['bookmarks'])

def minimize_map_path(path):
  # TODO: scores, names, teams
  path[0] = invert(mapmapping())[path[0]]

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

def minimize_metadata_path(path):
  if path[0] == 'maps':
    elem = path.pop(0)
    minimize_map_path(path)
    path.insert(0, elem)
  path[0] = invert(metadatamapping())[path[0]]

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

def minimize_path(path):
  if path[0] == 'metadata':
    elem = path.pop(0)
    minimize_metadata_path(path)
    path.insert(0, elem)
  path[0] = invert(toplevelmapping())[path[0]]

# minimize field names for projection
def minimize_proj(proj):
  for key, value in proj.items():
    del proj[key]
    path = key.split('.')
    minimize_path(path)
    key = '.'.join(path)
    proj[key] = value
  return proj

def matchmapping():
  return {
    'd': 'demos',
    's': 'map_start_time',
    'e': 'map_end_time',
    'n': 'mapname',
    'sc': 'scores',
    'h': 'sv_hostname',
    't': 'time_created',
    'ma': 'is_match',
    '_id': '_id'}

def demomapping():
  return {
    'c': 'client_id',
    'n': 'name',
    'id': 'id'}

def minimize_match(data):
  #data['_id'] = data['_id'][len('/cygdrive/U/demos/'):]
  if 'scores' in data:
    for scores in data['scores']:
      minimize_scores(scores)
  if 'demos' in data:
    for demo in data['demos']:
      rekey(demo, invert(demomapping()))
      demo['id'] = demo['id'][len('/cygdrive/U/demos/'):]
  rekey(data, invert(matchmapping()))
  return data

# TODO: this isn't done yet.  only minimize works.  to properly inflate, additional data is needed.
def inflate_match(data):
  #data['_id'] = '/cygdrive/U/demos/' + data['_id']
  rekey(data, matchmapping())
  for demo in data['demos']:
    demo['id'] = '/cygdrive/U/demos/' + demo['id']
    rekey(demo, demomapping())
  if 'scores' in data:
    for scores in data['scores']:
      inflate_scores(scores)
  if 'metadata' in data:
    inflate_metadata(data['metadata'])
  return data

def minimize_match_path(path):
  # TODO: scores, demos
  path[0] = invert(matchmapping())[path[0]]

# minimize field names for projection
def minimize_match_proj(proj):
  for key, value in proj.items():
    del proj[key]
    path = key.split('.')
    minimize_match_path(path)
    key = '.'.join(path)
    proj[key] = value
