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
from bisect import insort_left

pugpath = u'/cygdrive/U/demos/pug'
basepaths = [pugpath]
directories = [ join(basepath,f) for basepath in basepaths for f in listdir(basepath) if isdir(join(basepath,f)) ]

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


def merge_histories(datas):
  names = {}
  teams = {}
  return (merge_history(datas, 'name'), merge_history(datas, 'team'))

def parse_demo(demos):
  for demo in demos:
    try:
      yield json.loads( open( demo, u'rb' ).read().decode('utf8') )
    except ValueError:
      pass

for directory in directories:
  demos = [ join(directory,file) for file in listdir(directory) if file.endswith(".dm_meta") ]
  map_duration = 0
  map_start = -1
  map_end = 0
  demo_datas = [ data for data in parse_demo( demos ) if len(data['maps']) == 1 ]
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
  if map_start >= 1000:
    #print 'Not a pug - map_start was ' + str(map_start) + ': ' + directory
    continue
  if map_duration < 10 * 60 * 1000:
    continue
  # track team changes
  (name_history, team_history) = merge_histories(demo_datas)
  starting_teams = {'RED': [], 'BLUE': [], 'FREE': [], 'SPECTATOR': []}
  for (client, teams) in team_history.items():
    if teams[0]['team_start_time'] == map_start:
      starting_teams[teams[0]['team']].append(client)
  if len(starting_teams['RED']) < 2 or len(starting_teams['BLUE']) < 2:
    print 'Few players at start: ', starting_teams
    continue
  if len(starting_teams['RED']) != len(starting_teams['BLUE']):
    print 'Not equal number of players: ', starting_teams
    continue
  ending_teams = {'RED': [], 'BLUE': [], 'FREE': [], 'SPECTATOR': []}
  # still count players that quit less than a minute before map end time ("RQ")
  ending_team_time = map_end - 60 * 1000
  for (client, teams) in team_history.items():
    if teams[-1]['team_start_time'] <= ending_team_time and teams[-1]['team_end_time'] >= ending_team_time:
      ending_teams[teams[-1]['team']].append(client)
  for team in ['RED', 'BLUE']:
    if len(starting_teams[team]) != len(ending_teams[team]):
      print 'Too many left ' + team + ': ', starting_teams[team], ending_teams[team]
      break
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
    
    print 'Is a pug - map_start was ' + str(map_start) + ', ' + str(len(starting_teams['RED'])) + '\'s: ' + directory
    print 'Players:'
    pane_size = 40
    print 'RED'.ljust(pane_size) + 'BLUE'
    for idx in range(len(starting_teams['RED'])):
      red_client = starting_teams['RED'][idx]
      blue_client = starting_teams['BLUE'][idx]
      print ((red_client + ': ' + name_history[red_client][0]['name']).ljust(pane_size) +
          blue_client + ': ' + name_history[blue_client][0]['name'])
    if len(lost_players) > 0:
      print 'Lost players: ', lost_players
    #break
