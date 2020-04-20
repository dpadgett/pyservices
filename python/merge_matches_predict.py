#!/usr/bin/python
# version of merge_matches which uses prediction to generate any missing povs
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import re
import random
import json
import shlex
from os.path import isfile, isdir, join, exists, splitext
from os import listdir, remove
import os
import zipfile
import sys
import hashlib
import struct
import shutil

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
    try:
      pak = zipfile.ZipFile(file)
    except zipfile.BadZipfile:
      print 'Failed to open pk3', pak
      continue
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

#maps = load_arenas()
with open('maps.json') as mapsfile:
  maps = json.loads(mapsfile.read())

from subprocess import Popen, PIPE, call
from os import listdir, stat, remove
from os.path import isfile, isdir, join, exists, basename, dirname

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

def format_time(time):
  return ("%02d" % (time / 1000 / 60 / 60)) + ':' + ("%02d" % (time / 1000 / 60 % 60)) + ':' + ("%02d" % (time / 1000 % 60)) + '.' + ("%03d" % (time % 1000))

# script should be a list of [demofile, startmillis] pairs
def mergedemo(script, filename):
  democutter = u'./demomerger'
  #for nice_frag in [i for i in list if i[0] > 2.1]:
  #print nice_frag[0], basename(nice_frag[1][0]), nice_frag[1][1]['human_time']
  #shutil.copy2(nice_frag[3], u'/cygdrive/C/Program Files (x86)/jka/base/demos/acur/' + str(nice_frag[0]) + ' ' + nice_frag[2].replace(':', '-').replace('.', '_') + ' ' + basename(nice_frag[3]))
  
  scriptstr = ''
  scriptfile = '/dev/stdin'
  for pair in script:
    demofile, startmillis = pair
    scriptstr += demofile[1] + ' ' + str(demofile[0]) + ' ' + format_time(startmillis) + "\n"

  print 'Merging demos.  Script:'
  #print script
  print '<redacted>'
  proc = Popen([democutter, scriptfile, filename], stdin=PIPE)
  proc.stdin.write(scriptstr)
  proc.stdin.close()
  proc.wait()

def map_match_hash( map ):
  if 'match_hash' in map:
    return map['match_hash']
  # compute a hash to identify this match
  match_id = ((map['serverId'] & 0xFFFFFFFF) << 32) + (map['checksumFeed'] & 0xFFFFFFFF)
  match_hash = struct.pack('!Q', match_id).encode('hex')
  return match_hash

def findmap(maps, match_hash):
  longestmapidx = 0
  longestmap = maps[0]
  for idx, map in enumerate(maps):
    if map_match_hash(map) == match_hash:
      return (idx, map)
  raise Exception("Couldn't find map with hash %s" % (match_hash))

''' outdir is the non-cygwin dir (since it is passed to a non-cygwin c program '''
def mergematch(match, outdir):
  global maps
  db = MongoClient('mongodb').demos
  demodb = db.demos
  events = []
  demosbyid = {}
  demofiles = []
  demometabyid = {}
  clientids = {}
  demofiles = []
  for player in [i for l in [sc.get(team, []) for team in ['b', 'r', 'f'] for sc in match['sc'] ] for i in l]:
    clientids[player['c']] = False
  for demo in match['d']:
    clientids[demo['c']] = True#False#True
    # overloading this for now
    demosbyid[demo['c']] = '/cygdrive/U/demos/' + demo['id']
    demofiles.append('/cygdrive/U/demos/' + demo['id'])
  #demofiles = demosbyid.values()
  #TODO: trim each demo to just the relevant map before generating predicted demos.
  #needed because the missing chunk data isn't keyed by match id.
  demochanger = u'./demochanger'
  demoparser = u'./jkdemometadata'
  mapstart = None
  mapend = None
  for demo in match['d']:
    print 'Checking', demo['id']
    client = demo['c']
    #demodata = demodb.find({'_id':'/cygdrive/U/demos/' + demo['id']})[0]
    with open('/cygdrive/U/demos/' + demo['id'] + '.dm_meta') as metaf:
      demodata = {'_id': '/cygdrive/U/demos/' + demo['id'], 'metadata': json.loads(metaf.read())}
    demo['meta'] = demodata['metadata']
    mapidx, map = findmap(demodata['metadata']['maps'], match['_id'])
    if mapstart == None:
      mapstart = map['map_start_time']
    else:
      mapstart = min(mapstart, map['map_start_time'])
    if mapend == None:
      mapend = map['map_end_time']
    else:
      mapend = max(mapend, map['map_end_time'])
    # add in any clients that left mid match
    for (clientid, teams) in map['teams'].items():
      if len([t for t in teams if t['team'] != 'SPECTATOR']) > 0:
        if not int(clientid) in clientids:
          print 'Adding', clientid
          clientids[int(clientid)] = False
  generated_files = []
  for client in [id for (id, has_demo) in clientids.items() if not has_demo]:
    demofile = '/tmp/%d.%s.dm_26' % (client, match['_id'][0:5])
    missingmetafile = demofile + '.missing'
    print 'Generating demo for client', client, 'at', demofile
    #print ' '.join(['"' + x + '"' for x in [demochanger, '%d' % (client)] + demofiles + [u'C:%s' % (demofile)]])
    if not exists(missingmetafile):
      with open(missingmetafile, 'w') as missingmetafd:
        proc = Popen([demochanger, '%d' % (client)] + demofiles + [demofile], stdout=missingmetafd)
      proc.wait()
    else:
      print 'reusing existing missingmeta'
    with open(missingmetafile, 'r') as missingmetafd:
      missingmeta = json.loads(missingmetafd.read())
    demofilefd = open( demofile, u'rb' )
    proc = Popen([demoparser, u'-'], stdin=demofilefd, stdout=PIPE)
    meta = json.loads(proc.communicate()[0])
    meta['missing'] = missingmeta
    proc.wait()
    demofilefd.close()
    match['d'] += [{'id': demofile, 'c': client, 'meta': meta}]
    generated_files += [demofile, missingmetafile]
  #print match['demos']
  #return
  for demo in match['d']:
    print 'Processing', demo['id']
    client = demo['c']
    if 'meta' in demo:
      demodata = {'_id': '/cygdrive/U/demos/' + demo['id'], 'metadata': demo['meta']}
    else:
      demodata = None #demodb.find({'_id':'/cygdrive/U/demos/' + demo['id']})[0]
    demometabyid[client] = demodata
    mapidx, map = findmap(demodata['metadata']['maps'], match['_id'])
    demosbyid[client] = (mapidx, '/cygdrive/U/demos/' + demo['id'])
    for event in map['ctfevents']:
      if 'attacker' in event:
        attacker = event['attacker']
        if event['eventtype'] == 'FRAGGED_FLAG_CARRIER':
          continue # this is easier to do by scanning frags instead
        if attacker != client:
          continue
        humantime = event['human_time']
        timeparts = humantime.replace('.', ':').split(':')
        time = ((int(timeparts[0]) * 60 + int(timeparts[1])) * 60 + int(timeparts[2])) * 1000 + int(timeparts[3])
        if time < 0:
          raise Exception("Weird event: " + json.dumps(event) + " in demo " + demo['id'] + "\nFull metadata:\n" + json.dumps(demo))
        events.append( {'attacker': attacker, 'time': time, 'event': event['eventtype'], 'team': event['team']} )
    for frag in map['frags']:
      if frag['target_had_flag'] == 0:
        continue
      attacker = frag['attacker']
      if attacker != client:
        continue
      humantime = frag['human_time']
      timeparts = humantime.replace('.', ':').split(':')
      #time = ((int(humantime[0:2]) * 60 + int(humantime[3:5])) * 60 + int(humantime[6:8])) * 1000 + int(humantime[9:12])
      time = ((int(timeparts[0]) * 60 + int(timeparts[1])) * 60 + int(timeparts[2])) * 1000 + int(timeparts[3])
      events.append( {'attacker': attacker, 'time': time, 'event': 'FRAGGED_FLAG_CARRIER', 'team': frag['attacker_team'], 'target': frag['target']} )
  # drop events where the actual event is missing
  i = 0
  while i < len(events):
    demometa = demometabyid[events[i]['attacker']]['metadata']
    if 'missing' in demometa:
      #print 'Checking event', events[i], 'for missing data'
      time = events[i]['time']
      missingparts = [missingrange for missingrange in demometa['missing'] if missingrange[0] <= events[i]['time'] <= missingrange[1]]
      if len(missingparts) > 0:
        print 'Dropping event', events[i], 'due to missing data'
        del events[i]
        continue
    i += 1
  events = sorted(events, key = lambda event: event['time'])
  
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

  #globalmissingtime = 0
  #globaltotaltime = 0
  i = 0
  while i < len(events):
    event = events[i]
    demometa = demometabyid[event['attacker']]['metadata']
    mapidx = demosbyid[event['attacker']][0]
    map = demometa['maps'][mapidx]
    if i == 0:
      begin = mapstart
    else:
      prevevent = events[i - 1]
      begin = (prevevent['time'] + event['time']) / 2
    if i == len(events) - 1:
      end = mapend
    else:
      nextevent = events[i + 1]
      end = (event['time'] + nextevent['time']) / 2
    totaltime = end - begin
    #globaltotaltime += totaltime
    if 'missing' in demometa:
      #missingparts = [[max(begin, missingrange[0]), min(end, missingrange[1])]
      #  for missingrange in demometa['missing']
      #  if missingrange[0] <= begin <= missingrange[1]
      #    or missingrange[0] <= end <= missingrange[1]
      #    or begin <= missingrange[0] <= end
      #    or begin <= missingrange[1] <= end]
      #totalmissing = 0
      #for missingrange in missingparts:
      #  totalmissing += missingrange[1] - missingrange[0]
      #missingfraction = (totalmissing + 0.0) / totaltime
      #if missingfraction > 0.1 and totalmissing > 1000:
      newbegin = max([missingpart[1] for missingpart in demometa['missing'] if missingpart[1] < event['time']] + [map['map_start_time']])
      newend = min([missingpart[0] for missingpart in demometa['missing'] if missingpart[0] > event['time']] + [ map['map_end_time']])
      newduration = newend - newbegin
      if len([missingrange for missingrange in demometa['missing'] if missingrange[0] <= events[i]['time'] <= missingrange[1]]) > 0:
        # this event is missing - can happen since we switched around povs above
        newduration = 0
      if newduration > 3000:
        event['start'] = newbegin
        event['end'] = newend
        #print i, ((begin, end)), ': Set splice max duration to', newduration, 'ms, new range', ((newbegin, newend))
      else:
        print i, ((begin, end)), ': Only', newduration, 'ms, not enough for splice'
        del events[i]
        i = max(0, i - 1)
        continue
      #globalmissingtime += totalmissing
    i += 1
  
  #globalmissingfraction = (globalmissingtime + 0.0) / globaltotaltime
  #print 'Missing', globalmissingtime, '/', globaltotaltime, '=', globalmissingfraction
  
  #cuts = [[demosbyid[events[0]['attacker']], 0]]
  cuts = []
  # determine if there is a gap of time missing between the prev and cur events
  def computegap(basetime, prevend, curstart):
    # both have limitation, check if there's an overlap
    if curstart <= prevend:
      # they overlap, find the closest spot in the overlap to the basetime
      if curstart <= basetime <= prevend:
        return (basetime, basetime)
      elif basetime <= curstart:
        return (curstart, curstart)
      else: # prevend <= basetime
        return (prevend, prevend)
    else:
      # no overlap, return the full gap
      return (prevend, curstart)
  def range_available(id, starttime, endtime):
    demometa = demometabyid[id]['metadata']
    rangestart = demometa['maps'][demosbyid[id][0]]['map_start_time']
    rangeend = demometa['maps'][demosbyid[id][0]]['map_end_time']
    print 'Checking range', starttime, ':', endtime, 'for', id, 'from', rangestart, 'to', rangeend
    if rangestart > starttime or rangeend < endtime:
      return False
    if 'missing' in demometa:
      # check if there are any missing sections in the range [starttime, endtime]
      missingparts = [r for r in demometa['missing'] if
        (r[0] <= starttime and r[1] >= starttime) or
        (r[0] <= endtime and r[1] >= endtime) or
        (r[0] >= starttime and r[1] <= endtime)]
      #print 'missing parts:', missingparts
      return len(missingparts) == 0
    return True
  def glueid(starttime, endtime):
    choices = [id for (id, has_demo) in clientids.items() if range_available(id, starttime, endtime)]
    print 'Choices for', starttime, ':', endtime, choices
    return random.choice(choices)
  for i in range(0, len(events)):
    event = events[i]
    basestart = mapstart
    prevend = mapstart
    prevattacker = -1
    if i > 0:
      prevevent = events[i - 1]
      basestart = (event['time'] + prevevent['time']) / 2
      prevend =  prevevent.get('end', event['time'])
      prevattacker = prevevent['attacker']
    #print 'computing gap for', basestart, prevend, event.get('start', 0)
    startrange = computegap(basestart, prevend, event.get('start', 0))
    #print i, 'client', event['attacker'], 'range', [format_time(x) for x in startrange]
    if startrange[0] < startrange[1]:
      # find a slice we can actually use and use it
      prevattacker = glueid(startrange[0], startrange[1])
      cuts.append([demosbyid[prevattacker], startrange[0]])
      print 'added glue slice:', cuts[-1], 'length', (startrange[1] - startrange[0])
    if prevattacker != event['attacker']:
      cuts.append([demosbyid[event['attacker']], startrange[1]])
    if i == len(events) - 1:
      # last event, check if we need to append extra
      if 'end' in event and event['end'] < mapend:
        cuts.append([demosbyid[glueid(event['end'], mapend)], event['end']])
        print 'added end glue slice:', cuts[-1], 'length', (mapend - event['end'])

  mapname = match['n']
  tm = match['t']
  tm = timezone('UTC').localize(tm, is_dst=True)
  tm = tm.astimezone(timezone('US/Pacific'))
  filename = strip_fs(mapname + '_' + tm.strftime('%Y-%m-%d_%H-%M-%S'))
  prefix = outdir + filename
  mergedemo(cuts, prefix + '.dm_26')
  scores = match['sc'][0]
  for sc in match['sc']:
    if sc['fi']:
      scores = sc
      print 'Found final scores'
  description = 'Final score: Blue ' + str(scores['bs']) + ' Red ' + str(scores['rs']) + "\r\n"
  description += "Blue team:\r\n"
  description += "Time Name Score\r\n"
  for player in scores['b']:
    description +=  str(player['t']) + ' ' + strip_html(strip_colors(player['n'])) + ' ' + str(player['s']) + "\r\n"
  description += "Red team:\r\n"
  description += "Time Name Score\r\n"
  for player in scores['r']:
    description +=  str(player['t']) + ' ' + strip_html(strip_colors(player['n'])) + ' ' + str(player['s']) + "\r\n"
  description += "\r\nRendered from teh's JKA match database: http://demos.jactf.com/match.html#rpc=lookup&id=" + match['_id']
  video = dict(
    snippet = dict(
      title = strip_html(strip_colors(maps[mapname.lower()]['longname'])) + ' played on ' + tm.strftime('%A %B %d %Y at %I:%M %p'),
      description = description,
      categoryId = 20
    ),
    status = dict(
      privacyStatus = 'public'
    )
  )
  print video
  metafd = open(prefix + '.json', 'w')
  metafd.write(json.dumps(video))
  metafd.close()
  for file in generated_files:
    os.remove(file)
  return prefix + '.dm_26'

import zipfile

if __name__ == '__main__':
  db = MongoClient('mongodb').demos
  demodb = db.demos
  matchdb = db.minmatches
  #matches = list(matchdb.find({'is_match': True}))
  id = 'ebe90688634b8bf612b514c4a65bd1d4434ecc8361a176c845bddd6597904fadf90070f164bfeb04b141a58a349b8796047f1f7ae2e5d35d840ab8f9e8019bb1'
  if len(sys.argv) > 1:
    id = sys.argv[1]
  #matches = list(matchdb.find({'_id': '24061e497b5ba53c951c3374e7c142ec59fe3b1f72cd0b925fe9bc7f833c4c1dd7d98bf74cbad8d609960da9a25983475349d85b3f0f663058e1e70ae483ada8'}))
  #matches = list(matchdb.find({'_id': 'ee4c29f3b73cd86631f182c4ec9cc419bacca8eedfb146afe9f805021576e99b31111b1c463e1a3b3c6ec5691cb541392e5eefdbb5ee35067c98040b572b579d'}))
  #matches = list(matchdb.find({'_id': 'b9636735cc3ac2b09155cbe323cda1ffd1df6855d304c258df9ef6588a09f5fa0b953bcd7c72badc8f34636fb4c70028a68169af87f6bf493a9001fc7f80d7eb'}))
  #matches = list(matchdb.find({'_id': '39dd8a23965328658041d9df2510a2aeb905ebdbad71bc1662d0044d56d7b4db487f52c34aaf24808ef2d2b92ea599ceecc0a6c8865e46dd5fb985b1ddc154f3'}))
  #matches = list(matchdb.find({'_id': '958c5945124f7224720f2041f831b26c9b66c01ab8bd0eb775f34eaf8ff8444dadfeffccef6275d4841825ebec2993805885eabf3a22dc271638fa2a4b9bec80'}))
  match = matchdb.find({'_id': id})[0]

  origout = sys.stdout
  sys.stdout = sys.stderr

  file = mergematch(match, '/tmp/')

  sys.stdout = origout

  metafile = splitext(file)[0] + '.json'
  # we want to pass back both the demo file and the json data...
  # seems the simplest way is to either tar or zip the results
  # zip is more portable for users so will use it for now
  zipfile = splitext(file)[0] + '.zip'
  zip = Popen(['zip', basename(zipfile), basename(file), basename(metafile)], cwd=dirname(file), stdout=sys.stderr)
  zip.wait()

  os.remove(file)
  os.remove(metafile)

  print 'Content-type: application/octet-stream'
  print 'Content-Disposition: attachment; filename="' + basename(zipfile) + '"'
  print 'Status: 200 OK'
  print ''

  with open(zipfile, 'r') as f:
    shutil.copyfileobj(f, sys.stdout)

  os.remove(zipfile)
