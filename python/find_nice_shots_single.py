#!/usr/bin/python

from subprocess import Popen, PIPE
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

pugpath = u'/cygdrive/U/demos/japlus'
basepaths = [pugpath]
directories = [ join(basepath,f) for basepath in basepaths for f in listdir(basepath) if isdir(join(basepath,f)) ]
alldemos = [[ join(directory,file) ] for directory in directories for file in listdir(directory) if file.endswith(".dm_meta")]

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
          history[client_id].append( value )
      for value_list in history.values():
        sort_values(value_list)
        if len( value_list ) > 1:
          last_value = value_list[0]
          for value in value_list[1:]:
            if last_value[name + '_end_time'] > value[name + '_start_time']:
              # could happen if a client misses snaps
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
      yield json.loads( open( demo, u'rb' ).read().decode('utf8') )
    except ValueError:
      pass

def find_team( team_history, client, time ):
  if str(client) in team_history:
    for team in team_history[str(client)]:
      if team['team_start_time'] <= time and team['team_end_time'] >= time:
        return team['team']
  #raise Exception( "Couldn't find team for client " + str(client) + " at time " + str(time))
  print "Couldn't find team for client " + str(client) + " at time " + str(time)
  return 'FREE'

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
    def update_score_component(name, multiplier):
      if name in frag:
        update_score( name, abs( frag[name] ) * multiplier )
    update_score_component_log('missile_lifetime', 50)
    update_score_component('missile_pitch', 1)
    update_score_component_log('attacker_target_distance', 40)
    update_score_component_log('attacker_xy_speed', 10)
    update_score_component_log('target_xy_speed', 10)
    update_score_component_log('attacker_z_speed', 10)
    update_score_component_log('target_z_speed', 20)
    update_score_component('target_had_flag', 50)
    update_score_component_log('target_corpse_travel_z_distance', 10)
    update_score_component_log('target_corpse_travel_distance', 1)
    explanation['mod_weight'] = mod_weights[frag['mod']]
    score[0] *= mod_weights[frag['mod']] / float( max( mod_weights.values() ) )
    score = int(score[0])
    nice_frags.append([score, frag, explanation])
  nice_frags.sort( cmp = lambda x, y: y[0] - x[0] )
  return nice_frags

best_frags = []
for demos in alldemos:
  #demos = [ join(directory,file) for file in listdir(directory) if file.endswith(".dm_meta") ]
  map_duration = 0
  map_start = -1
  map_end = 0
  demo_datas = [ data for data in parse_demo( demos ) if len(data['maps']) >= 1 ]
  for data in demo_datas:
    map = data['maps'][0]
    if map_start == -1:
      map_start = map['map_start_time']
    else:
      map_start = min( map_start, map['map_start_time'] )
    map_end = max( map_end, map['map_end_time'] )
  map_duration = map_end - map_start
  # only a pug if map_start is (near) 0.  otherwise, noone was on when map started
  # actual map_restart min start time is 400.  but, server probably lags a few frames to reload everything
  # so say it's ok if map_start is <1s.  this filters out map changes, since clients take longer than .6s to load
  #if map_start >= 1000:
  #  #print 'Not a pug - map_start was ' + str(map_start) + ': ' + directory
  #  continue
  #if map_duration < 10 * 60 * 1000:
  #  continue
  # track team changes
  ( name_history, team_history, frag_history ) = merge_histories( demo_datas )
  starting_teams = {'RED': [], 'BLUE': [], 'FREE': [], 'SPECTATOR': []}
  for (client, teams) in team_history.items():
    if teams[0]['team_start_time'] == map_start:
      starting_teams[teams[0]['team']].append(client)
  if len(starting_teams['RED']) < 2 or len(starting_teams['BLUE']) < 2:
    print 'Few players at start: ', starting_teams
    #continue
  if len(starting_teams['RED']) != len(starting_teams['BLUE']):
    print 'Not equal number of players: ', starting_teams
    #continue
  ending_teams = {'RED': [], 'BLUE': [], 'FREE': [], 'SPECTATOR': []}
  # still count players that quit less than a minute before map end time ("RQ")
  ending_team_time = map_end - 60 * 1000
  for (client, teams) in team_history.items():
    if teams[-1]['team_start_time'] <= ending_team_time and teams[-1]['team_end_time'] >= ending_team_time:
      ending_teams[teams[-1]['team']].append(client)
  for team in ['RED', 'BLUE']:
    if len(starting_teams[team]) != len(ending_teams[team]):
      print 'Too many left ' + team + ': ', starting_teams[team], ending_teams[team]
      #break
  else:
    # check that not many players were replaced
    ending_players = [client for players in [ending_teams['RED'], ending_teams['BLUE']] for client in players]
    starting_players = [client for players in [starting_teams['RED'], starting_teams['BLUE']] for client in players]
    original_players = []
    for client in ending_players:
      if (team_history[client][0]['team_start_time'] == map_start and
          team_history[client][0]['team'] == team_history[client][-1]['team'] and
          team_history[client][-1]['team_start_time'] <= ending_team_time and
          team_history[client][-1]['team_end_time'] >= ending_team_time):
        original_players.append(client)
    lost_players = [client for client in starting_players if client not in original_players]
    if len(ending_players) - len(original_players) > 2:
      print 'More than 2 substitutions were made: ', lost_players
    # map must end in intermission (scoreboard shown).  demo file will only record scores during intermission
    
    print 'Is a pug - map_start was ' + str(map_start) + ', ' + str(len(starting_teams['RED'])) + '\'s: ' + demos[0]
    print 'Players:'
    pane_size = 40
    print 'RED'.ljust(pane_size) + 'BLUE'
    for idx in range(len(starting_teams['RED'])):
      red_client = starting_teams['RED'][idx]
      red_client_name = name_history[red_client][0]['name']
      if len(starting_teams['BLUE']) > idx:
        blue_client = starting_teams['BLUE'][idx]
        blue_client_name = name_history[blue_client][0]['name']
      else:
        blue_client = ''
        blue_client_name = ''
      print (red_client + ': ' + red_client_name.ljust(pane_size) +
          blue_client + ': ' + blue_client_name)
    if len(lost_players) > 0:
      print 'Lost players: ', lost_players
    nice_frags = find_nice_frags( frag_history, team_history )
    print 'Nice frags: '
    #demos_by_client = {}
    #for demo in demos:
    #  file = basename( demo )
    #  demos_by_client[file.split(' ', 1)[0]] = demo[0:-8]
    for nice_frag in nice_frags[0:10]:
      print nice_frag[0], nice_frag[1]['mod_name'], nice_frag[1]['human_time'], demos[0][0:-8] #demos_by_client[str(nice_frag[1]['attacker'])]
      best_frags.append([nice_frag[0], nice_frag[1]['mod_name'], nice_frag[1]['human_time'], demos[0][0:-8], nice_frag[2]])#demos_by_client[str(nice_frag[1]['attacker'])]])
    #break

best_frags.sort( cmp = lambda x, y: y[0] - x[0] )
print 'Best overall frags:'
for nice_frag in best_frags[0:10]:
  print nice_frag[0], nice_frag[1], nice_frag[2], basename(nice_frag[3])
  print nice_frag[4]
  shutil.copy2(nice_frag[3], u'/cygdrive/C/Program Files (x86)/jka/base/demos/acur/' + str(nice_frag[0]) + ' ' + nice_frag[2].replace(':', '-').replace('.', '_') + ' ' + basename(nice_frag[3]))
