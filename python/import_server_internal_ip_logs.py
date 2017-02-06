#!/usr/bin/python

from os import listdir, stat, utime
from os.path import isfile, isdir, join, exists, dirname, basename
import hashlib
import struct
import re

from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
import traceback

from pytz import timezone
import datetime

def hashForIp(ip):
  pieces = [int(num) for num in ip.split('.')]
  packedip = struct.pack('BBBB', pieces[3], pieces[2], pieces[1], pieces[0])
  hasher = hashlib.sha1()
  hasher.update(packedip)
  return struct.unpack('!i', hasher.digest()[:4])[0]

# accounts for any renaming of the demos from their original filenames on the server
def newDemoId(demo_id):
  dir = dirname(demo_id)
  #print dir
  match = re.match(r'.*/([0-9]{4})/([0-9]{2})/([0-9]{2})/', dir)
  if match != None:
    return demo_id
  match = re.match(r'(.*)/(.*) ([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2})-([0-9]{2})-([0-9]{2})', dir)
  if match == None:
    match = re.match(r'(.*)/(.*) ([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2})([0-9]{2})([0-9]{2})', dir)
    if match == None:
      return demo_id
  (subdir, map, year, month, day, hour, minute, second) = match.groups()
  #match = re.match(r'.*/(.*)', dir)
  #if match == None:
  #  continue
  #leaf = match.groups()[0]
  leaf = "%s %s-%s-%s_%s-%s-%s" % (map, year, month, day, hour, minute, second)
  newdir = "%s/%s/%s/%s/%s" % (subdir, year, month, day, leaf)
  #print 'Moving %s to %s' % (dir, newdir)
  return join(newdir, basename(demo_id))

def demo_ip_iter(logdir):
  db = MongoClient().demos
  matchdb = db.minmatches
  #logdir = '/cygdrive/Q/dumbledore3@gmail.com/japlus_data (2)/logs'
  active_players = {}
  last_client_num = None
  curmatch = None
  hostname = None
  mapname = None
  matchstart = None
  missing_matches = 0
  for log in sorted(listdir(logdir)):
    if not log.endswith('.log'):
      continue
    print log
    
    def find_match():
      match = None
      cursor = matchdb.find({'t': {'$lt': matchstart + datetime.timedelta(minutes=20), '$gte': matchstart}, 'h': hostname, 'n': mapname}).sort('t', -1)
      for row in cursor:
        row['t'] = row['t'].replace(tzinfo=timezone('UTC')).astimezone(timezone('America/Los_Angeles'))
        if row['t'] >= matchstart:
          match = row
      if match != None:
        truestart = match['t']
        # while match time in the db might be off, the pathnames should still have it.
        for demo in match['d']:
          #print demo['id']
          pattern = '.*/%s ([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2})-([0-9]{2})-([0-9]{2})/' % (mapname.replace('/', '').replace('.', ''))
          rematch = re.match(pattern, demo['id'])
          if rematch != None:
            groups = [int(g) for g in rematch.groups()]
            truestart = datetime.datetime(groups[0], groups[1], groups[2], groups[3], groups[4], groups[5])
            truestart = timezone('CET').localize(truestart, is_dst=True)
            truestart = truestart.astimezone(timezone('America/Los_Angeles'))
            break
        if truestart - matchstart < datetime.timedelta(minutes=1):
          print match['_id'], truestart, (truestart - matchstart).total_seconds()
          curmatch = match
          return curmatch
      print 'Failed to find match: %s %s %s' % (matchstart, hostname, mapname)
      return None
    
    with open(join(logdir, log), 'r') as f:
      for line in f:
        try:
          line = line.decode('cp1252')#.encode('utf-8')
        except UnicodeDecodeError:
          #continue
          pass
        content = line[line.find(': ') + 2:-1]
        if content.startswith(']\010 \010'):
          content = content[4:]
        if content.startswith("Timestamp: UTC "):
          matchstart = parse(content[len("Timestamp: UTC "):])
          matchstart = timezone('UTC').localize(matchstart, is_dst=False)
          local_tz = timezone('America/Los_Angeles')
          matchstart = matchstart.astimezone(local_tz)
          #matchstart = matchstart.replace(tzinfo=None)
          #print 'Match start:', matchstart
          #if hostname != None and mapname != None:
          #  curmatch = find_match()
          #  if curmatch == None:
          #    return
          hostname = None
          mapname = None
          #active_players = {}
          #last_client_num = None
          curmatch = None
        if content.startswith('InitGame: '):
          config = content[len('InitGame: '):]
          config = config[1:].split('\\')
          vars = {}
          for i in range(0, len(config) / 2):
            vars[config[i * 2]] = config[i * 2 + 1]
          if 'sv_hostname' in vars:
            hostname = vars['sv_hostname']
          if 'mapname' in vars:
            mapname = vars['mapname']
          #print hostname, mapname
        if content.startswith('ClientConnect'):
          match = re.match(r'ClientConnect: ([0-9]+) \((.*)\) from (.*):', content)
          if match != None:
            groups = match.groups()
            #print groups
            time = parse(line[0:line.find(': ')])
            active_players[groups[0]] = {'name': groups[1], 'ip': groups[2], 'time': time}
            continue
          match = re.match(r'ClientConnect: ([0-9]+) \((.*)\) \[IP: (.*):', content)
          if match != None:
            groups = match.groups()
            #print groups
            time = parse(line[0:line.find(': ')])
            active_players[groups[0]] = {'name': groups[1], 'ip': groups[2], 'time': time}
            continue
          match = re.match(r'ClientConnect: ([0-9]+)', content)
          if match != None:
            last_client_num = match.groups()[0]
            continue
          #print content, match
          continue
          exit(0)
        match = re.match(r'(.*) connected under IP -> (.*)', content)
        if match != None:
          if last_client_num == None:
            print 'Unknown clientnum:', content
            continue
          groups = match.groups()
          time = parse(line[0:line.find(': ')])
          active_players[last_client_num] = {'name': groups[0], 'ip': groups[1], 'time': time}
          last_client_num = None
        if content.startswith('recording to '):
          demo_file = content[len('recording to '):-1]
          match = re.match(r'recording to .*/([0-9]+) .*', content)
          if match == None:
            print content, match
            exit(0)
          groups = match.groups()
          client_id = groups[0]
          if client_id not in active_players:
            #print 'Missing client number', client_id, 'for', demo_file, 'file', log
            continue
            exit(0)
          player = active_players[client_id]
          yield (demo_file, player['ip'])
          #print demo_file, player['ip']
        if content.startswith('ClientDisconnect'):
          match = re.match(r'ClientDisconnect: ([0-9]+)', content)
          if match == None:
            print content, match
            continue
            exit(0)
          groups = match.groups()
          #print groups
          if groups[0] in active_players:
            time = parse(line[0:line.find(': ')])
            player = active_players[groups[0]]
            player['time'] = (time - player['time']).total_seconds()
            del active_players[groups[0]]
        if content.startswith('Server terminated'):
          #print line[31:-1].decode('cp1252').encode('utf-8')
          active_players = {}
        match = re.match(r'ClientBegin: ([0-9]+)', content)
        if match != None:
          # demos start recording at client begin
          if curmatch == None:
            curmatch = find_match()
            if curmatch == None:
              missing_matches += 1
              print 'Unknown match, skipping'#, content
              #if missing_matches > 1:
              #  return
              continue
          client_id = int(match.groups()[0])
          client_id_str = match.groups()[0]
          if client_id_str not in active_players:
            print 'Not active player, skipping:', content
            continue
          player = active_players[client_id_str]
          time = timezone('CET').localize(parse(line[0:line.find(': ')]), is_dst=True)
          demomatch = None
          matchtime = None
          for demo in curmatch['d']:
            if demo['c'] != client_id:
              continue
            pattern = r'.* ([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2})-([0-9]{2})-([0-9]{2}).dm_2'
            idmatch = re.match(pattern, demo['id'])
            if idmatch != None:
              groups = [int(g) for g in idmatch.groups()]
              starttime = datetime.datetime(groups[0], groups[1], groups[2], groups[3], groups[4], groups[5])
              starttime = timezone('CET').localize(starttime, is_dst=True)
              #starttime = starttime.astimezone(timezone('America/Los_Angeles'))
              #print demo['id'], starttime, time
              if (matchtime == None or matchtime < starttime) and starttime <= time:
                demomatch = demo
                matchtime = starttime
                #print demo
          if demomatch != None:
            yield demomatch['id'], player['ip']
            #print demomatch['n'], time - matchtime, player['ip']
            #pass
          #return

def import_ip_logs(logdir, demo_folder):
  db = MongoClient().demos
  demodb = db.mindemos
  ipdb = db.minips
  basedir = u'/cygdrive/U/demos/'
  
  base_folder = basedir + demo_folder #'japlus'
  num_ips = 0
  for demo_file, ip in demo_ip_iter(logdir):
    #demo_id = newDemoId(base_folder + u'/' + demo_file[len('demos/autorecord/'):])
    demo_id = basedir + demo_file
    #print demo_id
    exists = False
    min_id = ''
    for possible_id in [demo_id, demo_id.replace('/whoracle3/autorecord/', '/whoracle2/')]:
      min_id = possible_id[len(basedir):]
      for existing in demodb.find({'_id': min_id}, {}):
        exists = True
      if exists:
        break
    ip_hash = hashForIp(ip)
    #print exists, min_id, ip_hash
    if exists:
      cursor = ipdb.find({'_id': min_id})
      for row in cursor:
        if row['i'] != ip_hash:
          print 'Existing row:', row, ip_hash
          #return
      #continue
      ip_dao = {'_id': min_id, 'i': ip_hash}
      ipdb.find_and_modify(query = {'_id': min_id}, update = ip_dao, upsert = True)
      num_ips += 1
  print num_ips

if __name__ == '__main__':
  logdirs = [
    #(u'/cygdrive/Q/dumbledore3@gmail.com/japlus_data/logs', u'japlus'),
    #(u'/cygdrive/Q/dumbledore3@gmail.com/refresh_data/logs', u'pug'),
    #(u'/cygdrive/Q/dumbledore3@gmail.com/whoracle_data/logs', u'whoracle3/autorecord'),
    (u'/tmp/logs', u'whoracle3/autorecord'),
  ]
  for (logdir, demo_folder) in logdirs:
    import_ip_logs(logdir, demo_folder)