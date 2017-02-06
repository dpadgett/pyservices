#!/usr/bin/python

from subprocess import Popen, PIPE, call
from os import listdir, stat, remove
from os.path import isfile, isdir, join, exists, basename
import locale
import sys
import json
from dateutil.parser import *
from dateutil.tz import *
from datetime import *
from dateutil.relativedelta import *
import commands
from multiprocessing import Pool
from bisect import insort_left
import shutil
import math

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

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
  if demo.startswith(pugpath):
    return processPugDemo(demo, data)
  return 0
  #break

#p = Pool(5)
#p.map(processDemo, demos)

def merge_history(datas, name):
  history = {}
  names = name + 's'
  sort_values = lambda value_list: value_list.sort(cmp = lambda x, y: x[name + '_start_time'] - y[name + '_start_time'])
  for data in datas:
    map = data['maps'][0]
    for (client_id, value_list) in map[names].items():
      if ( not history.has_key(client_id) ):
        history[client_id] = []
      for value in value_list:
        if value[name + '_start_time'] > value[name + '_end_time']:
          continue
        for current_value in history[client_id]:
          if ( not name in value ):
            raise Exception('Error: value missing name: ' + str(value) + ' for client ' + str(data['client']['id']) + ' on map ' + map['mapname'])
          if ( value[name + '_start_time'] <= current_value[name + '_end_time'] and
              value[name + '_end_time'] >= current_value[name + '_start_time'] and
              value[name] == current_value[name] ):
            current_value[name + '_end_time'] = max( current_value[name + '_end_time'], value[name + '_end_time'] )
            current_value[name + '_start_time'] = min( current_value[name + '_start_time'], value[name + '_start_time'] )
            break
        else:
          history[client_id].append( dict(value) ) # makes a copy
      for value_list in history.values():
        sort_values(value_list)
        needs_check = True
        while needs_check:
          needs_check = False
          if len( value_list ) > 1:
            last_value = value_list[0]
            for value in value_list[1:]:
              if last_value[name + '_end_time'] > value[name + '_start_time']:
                # could happen if a client misses snaps
                if last_value[name + '_end_time'] > value[name + '_end_time']:
                  # if no other piece is after, then need to split this piece
                  for next_value in value_list:
                    if next_value[name + '_start_time'] == value[name + '_end_time']:
                      break
                  else:
                    # need another bit at the end then
                    new_value = dict(last_value)
                    new_value[name + '_start_time'] = value[name + '_end_time']
                    value_list.append( new_value )
                    sort_values(value_list)
                    needs_check = True
                last_value[name + '_end_time'] = value[name + '_start_time']
          for idx in range(len(value_list)):
            if idx >= len(value_list):
              break
            value = value_list[idx]
            while idx < len(value_list) - 1:
              next_value = value_list[idx + 1]
              if value[name + '_end_time'] == next_value[name + '_start_time'] and value[name] == next_value[name]:
                value[name + '_end_time'] = max( value[name + '_end_time'], next_value[name + '_end_time'] )
                del value_list[idx + 1]
              else:
                break
  return history

def merge_frags(datas):
  # rather than do a complicated merge w/ dedup, just pick own frags from each demo
  frags = []
  for data in datas:
    map = data['maps'][0]
    frags.extend( map['ownfrags'] )
  frags.sort( cmp = lambda x, y: x['time'] - y['time'] )
  return frags

def merge_histories(datas):
  return (merge_history(datas, 'name'), merge_history(datas, 'team'), merge_frags(datas))

def parse_demo(demos):
  for demo in demos:
    try:
      #print 'Searching for', demo
      yield demodb.find({'_id': demo})[0]
    except ValueError:
      pass

def find_team( team_history, client, time ):
  for team in team_history[str(client)]:
    if team['team_start_time'] <= time and team['team_end_time'] >= time:
      return team['team']
  raise Exception( "Couldn't find team for client " + str(client) + " at time " + str(time))

'''
def find_nice_frags(frag_history, team_history):
  nice_frags = []
  mod_weights = {
    4: 20,  # MOD_BRYAR_PISTOL
    5: 10,  # MOD_BRYAR_PISTOL_ALT
    6: 10,  # MOD_BLASTER
    8: 10,  # MOD_DISRUPTOR
    10: 10, # MOD_DISRUPTOR_SNIPER
    11: 10, # MOD_BOWCASTER
    12: 10, # MOD_REPEATER
    13: 30, # MOD_REPEATER_ALT
    14: 5,  # MOD_REPEATER_ALT_SPLASH
    15: 20, # MOD_DEMP2
    16: 0,  # MOD_DEMP2_ALT
    17: 30, # MOD_FLECHETTE
    19: 40, # MOD_ROCKET
    20: 5,  # MOD_ROCKET_SPLASH
    21: 10, # MOD_ROCKET_HOMING
    22: 2,  # MOD_ROCKET_HOMING_SPLASH
    23: 20, # MOD_THERMAL
    24: 5,  # MOD_THERMAL_SPLASH
    29: 20, # MOD_CONC
    30: 20, # MOD_CONC_ALT
    37: 50  # MOD_TELEFRAG
  }
  for frag in frag_history:
    attacker_team = find_team( team_history, frag['attacker'], frag['time'] )
    target_team = find_team( team_history, frag['target'], frag['time'] )
    if attacker_team == target_team:
      continue
    if ( not mod_weights.has_key( frag['mod'] ) ):
      continue
    score = mod_weights[frag['mod']]
    if frag.has_key('missile_lifetime'):
      score += frag['missile_lifetime'] / 2
    if frag.has_key('missile_pitch'):
      # favor angled shots over straight
      score += abs(frag['missile_pitch'])
    if frag.has_key('attacker_target_distance'):
      # favor shots where attacker is far away from target
      score += frag['attacker_target_distance'] / 40
    if frag.has_key('attacker_xy_speed'):
      score += frag['attacker_xy_speed'] / 40
    if frag.has_key('target_xy_speed'):
      score += frag['target_xy_speed'] / 40
    if frag.has_key('attacker_z_speed'):
      score += frag['attacker_z_speed'] / 10
    if frag.has_key('target_z_speed'):
      score += frag['target_z_speed'] / 10
    if frag.has_key('target_had_flag'):
      score += frag['target_had_flag'] * 20
    if frag.has_key('target_corpse_travel_z_distance'):
      score += int(math.log( 1 + frag['target_corpse_travel_z_distance'], 2 ) * 40)
    if frag.has_key('target_corpse_travel_distance'):
      score += frag['target_corpse_travel_distance'] / 60
    nice_frags.append([score, frag])
  nice_frags.sort( cmp = lambda x, y: y[0] - x[0] )
  return nice_frags
'''

def find_nice_frags(frag_history, team_history):
  nice_frags = []
  mod_weights = {
    4: 20,  # MOD_BRYAR_PISTOL
    5: 10,  # MOD_BRYAR_PISTOL_ALT
    6: 10,  # MOD_BLASTER
    8: 10,  # MOD_DISRUPTOR
    10: 10, # MOD_DISRUPTOR_SNIPER
    11: 10, # MOD_BOWCASTER
    12: 10, # MOD_REPEATER
    13: 30, # MOD_REPEATER_ALT
    14: 5,  # MOD_REPEATER_ALT_SPLASH
    15: 20, # MOD_DEMP2
    16: 0,  # MOD_DEMP2_ALT
    17: 30, # MOD_FLECHETTE
    19: 40, # MOD_ROCKET
    20: 5,  # MOD_ROCKET_SPLASH
    21: 10, # MOD_ROCKET_HOMING
    22: 2,  # MOD_ROCKET_HOMING_SPLASH
    23: 20, # MOD_THERMAL
    24: 5,  # MOD_THERMAL_SPLASH
    29: 20, # MOD_CONC
    30: 20, # MOD_CONC_ALT
    37: 50  # MOD_TELEFRAG
  }
  for frag in frag_history:
    try:
      attacker_team = find_team( team_history, frag['attacker'], frag['time'] )
      target_team = find_team( team_history, frag['target'], frag['time'] )
    except Exception as e:
      print e
      continue
    if attacker_team == target_team:
      # skip team kills
      continue
    if frag['attacker_is_bot'] == 1 or frag['target_is_bot'] == 1:
      # skip bot frags
      continue
    if ( not mod_weights.has_key( frag['mod'] ) ):
      continue
    score = [0]
    explanation = {}
    def update_score(name, delta):
      score[0] += delta
      explanation[name] = delta
    def update_score_component_log(name, multiplier):
      if name in frag:
        update_score( name, math.log( 1 + frag[name], 2 ) * multiplier )
    '''
    version of the above which instead penalizes any value below the new 'zero' and caps the component at a maximum value
    '''
    def update_score_component_log_rezero_cap(name, zero, cap, multiplier):
      if name in frag:
        update_score( name, (math.log( 1 + min(cap, frag[name]), 2 ) - math.log( 1 + zero, 2 )) * multiplier )
    def update_score_component(name, multiplier):
      if name in frag:
        update_score( name, abs( frag[name] ) * multiplier )
    update_score_component_log('missile_lifetime', 100)
    update_score_component('missile_pitch', 1)
    update_score_component_log('attacker_target_distance', 20)
    update_score_component_log('attacker_xy_speed', 10)
    update_score_component_log('target_xy_speed', 10)
    update_score_component_log_rezero_cap('attacker_z_speed', 0, 300, 10)
    update_score_component_log_rezero_cap('target_z_speed', 100, 500, 20)
    update_score_component('target_had_flag', 50)
    update_score_component_log_rezero_cap('target_corpse_travel_z_distance', 300, 1500, 60)
    update_score_component_log('target_corpse_travel_distance', 1)
    explanation['mod_weight'] = mod_weights[frag['mod']]
    score[0] *= mod_weights[frag['mod']] / float( max( mod_weights.values() ) )
    score = int(score[0])
    nice_frags.append([score, frag, explanation])
  nice_frags.sort( cmp = lambda x, y: y[0] - x[0] )
  return nice_frags

db = MongoClient().demos
demodb = db.demos
matchdb = db.matches
matches = list(matchdb.find({'is_match': True}))
best_frags = []
num_matches = 0
for match in matches:
  #print match
  #if not any('teh-manaan-' in demo['id'] for demo in match['demos']):
  #  continue
  last_match = match
  num_matches += 1
  print 'Processing match', num_matches, 'at', match['sv_hostname'], 'on', match['maps'][0]['mapname'], 'at', match['time_created']
  demos_by_client = {}
  for demo in match['demos']:
    demos_by_client[str(demo['client_id'])] = demo['id']
  demos = demos_by_client.values() #[demo['id'] for demo in match['demos']]
  map_duration = 0
  map_start = -1
  map_end = 0
  demo_datas = [ data for data in parse_demo( demos ) ]
  #print demo_datas
  # track team changes
  ( name_history, team_history, frag_history ) = merge_histories( [demo['metadata'] for demo in demo_datas] )
  nice_frags = find_nice_frags( frag_history, team_history )
  #print 'Nice frags: '
  for nice_frag in nice_frags[0:300]:
    #print nice_frag[0], nice_frag[1]['mod_name'], nice_frag[1]['human_time'], demos_by_client[str(nice_frag[1]['attacker'])]
    best_frags.append([nice_frag[0], nice_frag[1]['mod_name'], nice_frag[1]['human_time'], demos_by_client[str(nice_frag[1]['attacker'])], nice_frag[1]['time'], nice_frag[2]])
  best_frags.sort( cmp = lambda x, y: y[0] - x[0] )
  best_frags = best_frags[0:300]
  #break

#for nice_frag in best_frags[0:10]:
#  print nice_frag[0], nice_frag[1], nice_frag[2], basename(nice_frag[3])
#  print nice_frag[-1]
#sys.exit(0)
#raise Exception

democutter = u'/cygdrive/C/Users/dan/Documents/Visual Studio 2010/Projects/JKDemoMetadata/Release/DemoTrimmer.exe'
def format_time(time):
  return ("%02d" % (time / 1000 / 60 / 60)) + ':' + ("%02d" % (time / 1000 / 60 % 60)) + ':' + ("%02d" % (time / 1000 % 60)) + '.' + ("%04d" % (time % 1000))
def strip_non_ascii(string):
  ''' Returns the string without non ASCII characters'''
  stripped = (c for c in string if 0 < ord(c) < 127)
  return ''.join(stripped)
best_frags.sort( cmp = lambda x, y: y[0] - x[0] )
print 'Best overall frags:'
for nice_frag in best_frags:
  print nice_frag[0], nice_frag[1], nice_frag[2], basename(nice_frag[3])
  print nice_frag[-1]
  #shutil.copy2(nice_frag[3], u'/cygdrive/C/Program Files (x86)/jka/base/demos/acur/' + str(nice_frag[0]) + ' ' + nice_frag[2].replace(':', '-').replace('.', '_') + ' ' + basename(nice_frag[3]))
  
  demofd = open( nice_frag[3], u'rb' )
  demometafd = open( u'/cygdrive/C/Program Files (x86)/jka/base/demos/acur/' + str(nice_frag[0]) + ' ' + nice_frag[2].replace(':', '-').replace('.', '_') + ' ' + strip_non_ascii(basename(nice_frag[3])), u'wb' )
  proc = Popen([democutter, '-', '-', format_time(nice_frag[4] - 5000), format_time(nice_frag[4] + 3000)], stdout=demometafd, stdin=demofd)
  proc.wait()
  demofd.close()
  demometafd.close()
