#!/usr/bin/python

from subprocess import Popen, PIPE
from os import listdir, stat, remove
from os.path import isfile, isdir, join, exists
import locale
import sys
import json
from dateutil.parser import *
from dateutil.tz import *
from datetime import *
from dateutil.relativedelta import *
import commands
from multiprocessing import Pool

pugpath = u'/cygdrive/U/demos/pug'
basepaths = [u'/cygdrive/U/demos/japlus', pugpath]
directories = [ join(basepath,f) for basepath in basepaths for f in listdir(basepath) if isdir(join(basepath,f)) ]
demos = [ join(d,file) for d in directories for file in listdir(d) if file.endswith(".dm_meta")]

def getValueFreq(data, name, client_id):
  names = name + 's'
  values = []
  for map in data['maps']:
    if map[names].has_key(str(client_id)):
      values.extend(map[names][str(client_id)])
  value_freq = {}
  for value in values:
    cur_value = 0
    if value_freq.has_key(value[name]):
      cur_value = value_freq[value[name]]
    value_freq[value[name]] = cur_value + value[name + '_end_time'] - value[name + '_start_time']
  return value_freq

end_time = parse(commands.getoutput("date") + ' -0800') + relativedelta(hours=-1)

def processDemo(demo):
  print 'Processing demo: ' + demo.encode('utf8')
  tmstr = demo.rsplit(' ', 1)[1][0:-14]
  if (tmstr.find('-') == -1):
    tmstr = ' '.join(demo.rsplit(' ', 2)[-2:])[0:-14]
  tm = parse(tmstr.replace('_', ' ') + ' -0500')
  if tm > end_time:
    print 'Skipping too recent demo'
    return 0
  data = json.loads( open( demo, u'rb' ).read().decode('utf8') )
  client_id = data['client']['id']
  name_freq = getValueFreq(data, 'name', client_id)
  max_time = 0;
  max_name = '';
  for (name, freq) in name_freq.iteritems():
    if freq > max_time:
      max_time = freq
      max_name = name
  if max_time == 0:
    print 'Failed to find a name'
  else:
    print 'Found player name ' + max_name
  team_freq = getValueFreq(data, 'team', client_id)
  print 'Found player teams ' + str(team_freq)
  if team_freq.keys() == ['SPECTATOR'] or team_freq == {}:
    print 'Player was only spectator, deleting demo'
    remove( demo )
    remove( demo[0:-8] )
  return 0
  #break

#p = Pool(5)
#p.map(processDemo, demos)
for demo in demos:
  result = processDemo(demo)
  if result != 0:
    break
