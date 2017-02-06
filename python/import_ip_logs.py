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
  ip_to_names = {}
  #logdir = '/cygdrive/Q/dumbledore3@gmail.com/japlus_data (2)/logs'
  for log in sorted(listdir(logdir)):
    if not log.endswith('.log'):
      continue
    print log
    with open(join(logdir, log), 'r') as f:
      active_players = {}
      last_client_num = None
      for line in f:
        try:
          line = line.decode('cp1252')#.encode('utf-8')
        except UnicodeDecodeError:
          #continue
          pass
        content = line[line.find(': ') + 2:-1]
        if content.startswith(']\010 \010'):
          content = content[4:]
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
            #print player['ip'], player['name'], player['time']
            # check blacklisted names
            if player['name'].lower() not in ['padawan', 'padwan', '', 'spec', 'specs', 'specss', 'specsss', '1']:
              if player['ip'] not in ip_to_names:
                ip_to_names[player['ip']] = {}
              names = ip_to_names[player['ip']]
              if player['name'] not in names:
                names[player['name']] = 0
              names[player['name']] += player['time']
            del active_players[groups[0]]
        if content.startswith('Server terminated'):
          #print line[31:-1].decode('cp1252').encode('utf-8')
          active_players = {}


def import_ip_logs(logdir, demo_folder):
  db = MongoClient().demos
  demodb = db.mindemos
  ipdb = db.minips
  basedir = u'/cygdrive/U/demos/'
  
  base_folder = basedir + demo_folder #'japlus'
  num_ips = 0
  for demo_file, ip in demo_ip_iter(logdir):
    demo_id = newDemoId(base_folder + u'/' + demo_file[len('demos/autorecord/'):])
    exists = False
    min_id = ''
    for possible_id in [demo_id, demo_id.replace('/whoracle3/autorecord/', '/whoracle2/')]:
      min_id = possible_id[len(basedir):]
      for existing in demodb.find({'_id': min_id}, {}):
        exists = True
      if exists:
        break
    ip_hash = hashForIp(ip)
    print exists, min_id, ip_hash
    if exists:
      ip_dao = {'_id': min_id, 'i': ip_hash}
      ipdb.find_and_modify(query = {'_id': min_id}, update = ip_dao, upsert = True)
      num_ips += 1
  print num_ips
  
  '''
  name_to_ip = {}
  dedup_ip = {}
  for ip, names in ip_to_names.iteritems():
    sorted_names = sorted(names.items(), key=lambda entry: -entry[1])
    dip = ip
    for idx, (name, time) in enumerate(sorted_names):
      if time < 800 and idx > 2:
        break
      if name in name_to_ip:
        dip = name_to_ip[name]
        break
    for idx, (name, time) in enumerate(sorted_names):
      if time < 800 and idx > 2:
        break
      name_to_ip[name] = dip
    if dip not in dedup_ip:
      dedup_ip[dip] = []
    dedup_ip[dip].append(ip)
  for ip, allips in dedup_ip.iteritems():
    allnames = {}
    for aip in allips:
      for name, time in ip_to_names[aip].iteritems():
        if name not in allnames:
          allnames[name] = 0
        allnames[name] += time
    sorted_names = sorted(allnames.items(), key=lambda entry: -entry[1])
    sorted_names = [s for s in sorted_names if s[1] >= 800]
    print ip, allips, sorted_names
  '''

if __name__ == '__main__':
  logdirs = [
    #(u'/cygdrive/Q/dumbledore3@gmail.com/japlus_data/logs', u'japlus'),
    #(u'/cygdrive/Q/dumbledore3@gmail.com/refresh_data/logs', u'pug'),
    (u'/cygdrive/Q/dumbledore3@gmail.com/whoracle_data_2/logs', u'whoracle3/autorecord'),
    #(u'/tmp/logs', u'whoracle3/autorecord'),
  ]
  for (logdir, demo_folder) in logdirs:
    import_ip_logs(logdir, demo_folder)