from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import re
import random
import json
import shlex
from os.path import isfile, isdir, join, exists
from os import listdir
import zipfile

from dateutil.parser import *
from dateutil.tz import *
from datetime import *
from dateutil.relativedelta import *
from pytz import timezone

def load_arenas():
  basedir = '/cygdrive/C/Program Files (x86)/jka/base'
  maps = dict()
  for pak in listdir(basedir):
    if not pak.endswith('.pk3'):
      continue
    file = join(basedir, pak)
    if isdir(file):
      continue
    pak = zipfile.ZipFile(file)
    arenas = [name for name in pak.namelist() if name.endswith('.arena')]
    for arena in arenas:
      fd = pak.open(arena)
      contents = fd.read()
      fd.close()
      map = None
      key = None
      for token in shlex.split(contents):
        if token == '{':
          map = dict()
        elif token == '}':
          if 'map' in map:
            maps[map['map']] = map
          map = dict()
        elif key != None:
          map[key] = token
          key = None
        else:
          key = token
  return maps

maps = load_arenas()

from subprocess import Popen, PIPE, call
from os import listdir, stat, remove
from os.path import isfile, isdir, join, exists, basename

def strip_non_ascii(string):
  ''' Returns the string without non ASCII characters'''
  stripped = (c for c in string if 0 < ord(c) < 127)
  return ''.join(stripped)

def strip_fs(string):
  ''' Returns the string without any special fs characters'''
  stripped = (c for c in strip_non_ascii(string) if c != '.' and c != '/' and c != "\\")
  return ''.join(stripped)

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

def strip_html(string):
  ''' Returns the string without any special html characters'''
  stripped = (c for c in string if c != '<' and c != '>')
  return ''.join(stripped)

# script should be a list of [demofile, startmillis] pairs
def mergedemo(script, filename):
  democutter = u'/cygdrive/C/Users/dan/Documents/Visual Studio 2010/Projects/JKDemoMetadata/Release/DemoMerger.exe'
  def format_time(time):
    return ("%02d" % (time / 1000 / 60 / 60)) + ':' + ("%02d" % (time / 1000 / 60 % 60)) + ':' + ("%02d" % (time / 1000 % 60)) + '.' + ("%03d" % (time % 1000))
  #for nice_frag in [i for i in list if i[0] > 2.1]:
  #print nice_frag[0], basename(nice_frag[1][0]), nice_frag[1][1]['human_time']
  #shutil.copy2(nice_frag[3], u'/cygdrive/C/Program Files (x86)/jka/base/demos/acur/' + str(nice_frag[0]) + ' ' + nice_frag[2].replace(':', '-').replace('.', '_') + ' ' + basename(nice_frag[3]))
  
  scriptfile = 'C:\\cygwin\\tmp\\script'
  cygscriptfile = '/tmp/script'
  scriptfd = open( cygscriptfile, u'wb' )
  for pair in script:
    demofile, startmillis = pair
    scriptfd.write( demofile[1] + ' ' + str(demofile[0]) + ' ' + format_time(startmillis) + "\n" )
  scriptfd.close()
  
  proc = Popen([democutter, scriptfile, filename])
  proc.wait()

def findmap(maps):
  longestmapidx = 0
  longestmap = maps[0]
  for idx, map in enumerate(maps):
    if map['map_end_time'] - map['map_start_time'] > longestmap['map_end_time'] - longestmap['map_start_time']:
      longestmapidx = idx
      longestmap = map
  return (longestmapidx, longestmap)

def mergematch(match):
  global maps
  events = []
  demosbyid = {}
  for demo in match['demos']:
    print 'Processing', demo['id']
    client = demo['client_id']
    demodata = demodb.find({'_id':demo['id']})[0]
    mapidx, map = findmap(demodata['metadata']['maps'])
    demosbyid[client] = (mapidx, demo['id'].replace('/cygdrive/U/', 'U:/'))
    for event in map['ctfevents']:
      if 'attacker' in event:
        attacker = event['attacker']
        if event['eventtype'] == 'FRAGGED_FLAG_CARRIER':
          continue # this is easier to do by scanning frags instead
        if attacker != client:
          continue
        humantime = event['human_time']
        time = ((int(humantime[0:2]) * 60 + int(humantime[3:5])) * 60 + int(humantime[6:8])) * 1000 + int(humantime[9:12])
        events.append( {'attacker': attacker, 'time': time, 'event': event['eventtype'], 'team': event['team']} )
    for frag in map['frags']:
      if frag['target_had_flag'] == 0:
        continue
      attacker = frag['attacker']
      if attacker != client:
        continue
      humantime = frag['human_time']
      time = ((int(humantime[0:2]) * 60 + int(humantime[3:5])) * 60 + int(humantime[6:8])) * 1000 + int(humantime[9:12])
      events.append( {'attacker': attacker, 'time': time, 'event': 'FRAGGED_FLAG_CARRIER', 'team': frag['attacker_team'], 'target': frag['target']} )
  events = sorted(events, key = lambda event: event['time'])
  cuts = [[demosbyid[events[0]['attacker']], 0]]
  # make sure all caps have decent buffer of time around them
  i = 0
  while i < len(events) - 1:
    event = events[i]
    nextevent = events[i + 1]
    if nextevent['event'] == 'PLAYER_CAPTURED_FLAG' and nextevent['time'] - event['time'] < 5000:
      del events[i]
      i -= 1
      if i < 0:
        i = 0
      continue
    if event['event'] == 'PLAYER_CAPTURED_FLAG' and nextevent['time'] - event['time'] < 3000:
      del events[i + 1]
      continue
    i += 1
  # make sure all cuts have a minimum buffer of time or else cut them
  i = 1
  while i < len(events) - 1:
    prevevent = events[i - 1]
    event = events[i]
    nextevent = events[i + 1]
    if nextevent['time'] - prevevent['time'] < 5000:
      del events[i]
      continue
    i += 1
  # if fragged with flag, keep same pov
  i = 0
  while i < len(events) - 1:
    event = events[i]
    nextevent = events[i + 1]
    if nextevent['event'] == 'FRAGGED_FLAG_CARRIER' and event['attacker'] == nextevent['target']:
      attacker = random.choice([event['attacker'], nextevent['attacker']])
      nextevent['attacker'] = attacker
      event['attacker'] = attacker
    if nextevent['event'] == 'PLAYER_RETURNED_FLAG' and nextevent['time'] - event['time'] < 5000:
      del events[i + 1]
    i += 1
  for i in range(0, len(events) - 1):
    event = events[i]
    nextevent = events[i + 1]
    if event['attacker'] == nextevent['attacker']:
      continue
    cuts.append([demosbyid[nextevent['attacker']], (nextevent['time'] + event['time']) / 2])
  mapidx, map = findmap(match['maps'])
  mapname = map['mapname']
  tm = match['time_created']
  tm = timezone('UTC').localize(tm, is_dst=True)
  tm = tm.astimezone(timezone('US/Pacific'))
  filename = strip_fs(mapname + ' ' + tm.strftime('%Y-%m-%d %H_%M_%S'))
  prefix = 'C:/Program Files (x86)/jka/base/demos/acur/test/' + filename
  mergedemo(cuts, prefix + '.dm_26')
  scores = map['scores']
  description = 'Final score: Blue ' + str(scores['blue_score']) + ' Red ' + str(scores['red_score']) + "\r\n"
  description += "Blue team:\r\n"
  description += "Time Name Score Caps\r\n"
  for player in scores['blueplayers']:
    description +=  str(player['time']) + ' ' + strip_html(strip_colors(player['client_name'])) + ' ' + str(player['score']) + ' ' + str(player['captures']) + "\r\n"
  description += "Red team:\r\n"
  description += "Time Name Score Caps\r\n"
  for player in scores['redplayers']:
    description +=  str(player['time']) + ' ' + strip_html(strip_colors(player['client_name'])) + ' ' + str(player['score']) + ' ' + str(player['captures']) + "\r\n"
  video = dict(
    snippet = dict(
      title = strip_html(strip_colors(maps[mapname]['longname'])) + ' played on ' + tm.strftime('%A %B %d %Y at %I:%M %p'),
      description = description,
      categoryId = 22
    ),
    status = dict(
      privacyStatus = 'public'
    )
  )
  print video
  metafd = open(prefix + '.json', 'w')
  metafd.write(json.dumps(video))
  metafd.close()
  return

if __name__ == '__main__':
  db = MongoClient().demos
  demodb = db.demos
  matchdb = db.matches
  #matches = list(matchdb.find({'is_match': True}))
  id = 'be626a7cdc71cca6eb25f7a89b3c2a92afba7b94343e6e2f61a28c1d8c76782066bbcc804a2632b7ed3281d82806ff85b02d0973e012fad2cc23a2a6cc96bd0d'
  #matches = list(matchdb.find({'_id': '24061e497b5ba53c951c3374e7c142ec59fe3b1f72cd0b925fe9bc7f833c4c1dd7d98bf74cbad8d609960da9a25983475349d85b3f0f663058e1e70ae483ada8'}))
  #matches = list(matchdb.find({'_id': 'ee4c29f3b73cd86631f182c4ec9cc419bacca8eedfb146afe9f805021576e99b31111b1c463e1a3b3c6ec5691cb541392e5eefdbb5ee35067c98040b572b579d'}))
  #matches = list(matchdb.find({'_id': 'b9636735cc3ac2b09155cbe323cda1ffd1df6855d304c258df9ef6588a09f5fa0b953bcd7c72badc8f34636fb4c70028a68169af87f6bf493a9001fc7f80d7eb'}))
  #matches = list(matchdb.find({'_id': '39dd8a23965328658041d9df2510a2aeb905ebdbad71bc1662d0044d56d7b4db487f52c34aaf24808ef2d2b92ea599ceecc0a6c8865e46dd5fb985b1ddc154f3'}))
  #matches = list(matchdb.find({'_id': '958c5945124f7224720f2041f831b26c9b66c01ab8bd0eb775f34eaf8ff8444dadfeffccef6275d4841825ebec2993805885eabf3a22dc271638fa2a4b9bec80'}))
  match = matchdb.find({'_id': id})[0]
  mergematch(match)
