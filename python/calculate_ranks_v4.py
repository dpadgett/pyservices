#!/usr/bin/python
# -*- coding: utf-8 -*-
# version of merge_matches which uses prediction to generate any missing povs
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import pymongo
import re
import random
import json
import os
import signal
import shlex
from os.path import isfile, isdir, join, exists
from os import listdir
import zipfile
import sys
import hashlib
import struct

from dateutil.parser import *
from dateutil.tz import *
from datetime import *
from dateutil.relativedelta import *
from pytz import timezone

from subprocess import Popen, PIPE, call
from os import listdir, stat, remove
from os.path import isfile, isdir, join, exists, basename

import trueskill

import merge_metadata
import shrinker
import traceback

import shrinker
import copy

import time

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

def run(script, args):
  cwd = '/home/pyservices/'
  if script[0:1] != '/':
    script = cwd + script
  start = time.time()

  origargv = sys.argv
  sys.argv = [script]
  sys.argv.extend(args)
  #print sys.argv
  d = dict(locals(), **globals())
  try:
    execfile(script, d, d)
  except:
    print traceback.format_exc()
  sys.argv = origargv
  #print 'Elapsed:', time.time() - start

ratings = {}

def updaterank(match):
  global ratings
  # first see if we even know the players in this match
  db = MongoClient("mongodb").demos
  playergamedb = db.playerGames
  sessionplayerdb = db.sessionPlayers
  sessiondb = db.sessions
  sessiongamedb = db.sessionGames
  ratingdb = db.playerGameRatings
  sessiongames = [sessiongame for sessiongame in sessiongamedb.find({'_id.match': match['_id'], 'is_match': True})]
  if len(sessiongames) == 0:
    print 'No players found'
    return
  playermap = {}
  playergamemap = {}
  for session in sessiondb.find({'_id': {'$in': [sessiongame['_id']['session'] for sessiongame in sessiongames]}}):
    if 'playerid' not in session:
      print 'Unknown player for session', session['_id']
      exit()
      return
    playermap[json.dumps(session['_id'])] = session['playerid']
    for idx, sessiongame in enumerate(sessiongames):
      if sessiongame['_id']['session'] != session['_id']:
        continue
      sessiongame['games'].sort(cmp = lambda x, y: x['start'] - y['start'])
      if session['playerid'] in playergamemap:
        # could be that they reconnected with a new IP.  just merge the 2 sessiongames
        print 'Multiple sessions in game for same player', session['playerid']
        print [n['name'] for g in sessiongame['games'] for n in g['names']]
        print [n['name'] for g in [s for s in sessiongames if s['_id'] == playergamemap[session['playerid']]][0]['games'] for n in g['names']]
        print [(g['ip'], g['guid'], g.get('newmod_id', None)) for g in sessiongame['games']]
        print [(g['ip'], g['guid'], g.get('newmod_id', None)) for s in sessiongames if s['_id'] == playergamemap[session['playerid']] for g in s['games']]
        #print sessiongame
        #print [s for s in sessiongames if s['_id'] == playergamemap[session['playerid']]][0]
        matchingsessiongame = [s for s in sessiongames if s['_id'] == playergamemap[session['playerid']]][0]
        matchingsessiongame['games'] += sessiongame['games']
        matchingsessiongame['games'].sort(cmp = lambda x, y: x['start'] - y['start'])
        del sessiongames[idx]
        #continue
        sessiongame = matchingsessiongame
        #exit()
        #print [s for s in sessiongames if s['_id'] == playergamemap[session['playerid']]][0]
        #return
      # sanity check.  if there are overlapping times now, then there were actually different players
      allteams = [team for game in sessiongame['games'] for team in game['teams'] if team['team'] in ['RED', 'BLUE']]
      for idx, team in enumerate(allteams):
        for idx2, t2 in enumerate(allteams[idx+1:]):
          # t2.start == team.end means touching but not overlapping
          if t2['start'] < team['end'] and team['start'] <= t2['end']:
            print 'Found overlapping team times', team, t2
            print idx, sessiongame
            print session['playerid']
            #exit()
            if (team['end'] - t2['start']) < 60 * 1000 and len(sessiongame['games']) > 1:
              # ignore short overlap, could be due to reconnecting before ghost disconnected
              print 'Short overlap, ignoring'
              continue
            #if match['_id'] in ['2175ac73bb8b80d4', '0e4beca72cda80e7', '17eca4042b36266a', '04ccf99bc48c9e6c']:
            #  # known issue of reconnecting before ghost disconnected
            #  continue
            if str(session['playerid']) == '58665935cb15c72b1c0acf25':
              # skz / duck player known issue
              return
            raise ValueError
      playergamemap[session['playerid']] = sessiongame['_id']
  match = shrinker.inflate_match(copy.deepcopy(match))
  #exit(1)
  for scores in match['scores']:
    if scores['is_final']:
      break
  if not scores['is_final']:
    print "Couldn't find final scores"
    exit()
    return
  maptime = match['map_end_time'] - match['map_start_time']
  result = {}
  if scores['blue_score'] > scores['red_score']:
    winner = 'BLUE'
    result = {'RED': 1, 'BLUE': 0}
  elif scores['blue_score'] == scores['red_score']:
    winner = 'TIE'
    result = {'RED': 0, 'BLUE': 0}
  else:
    winner = 'RED'
    result = {'RED': 0, 'BLUE': 1}
  winners = []
  losers = []
  teamrating = {'RED': {}, 'BLUE': {}}
  teamtime = {'RED': {}, 'BLUE': {}}
  for sessiongame in sessiongames:
    if json.dumps(sessiongame['_id']['session']) not in playermap:
      print 'Unknown player %s in match %s' % (sessiongame['_id']['session'], match['_id'])
      exit()
      return

    for game in sessiongame['games']:
      for idx in range(len(game['teams']))[1:-1]:
        prevteam = game['teams'][idx-1]
        team = game['teams'][idx]
        nextteam = game['teams'][idx+1]
        if prevteam['team'] == 'SPECTATOR' and team['team'] in ['RED', 'BLUE'] and nextteam['team'] in ['RED', 'BLUE'] and team['end'] - team['start'] < 30 * 1000:
          # player just joined the wrong team for a second.  ignore it
          del game['teams'][idx]
          break
    active_by_team = {'RED': 0, 'BLUE': 0}
    for game in sessiongame['games']:
      for team in game['teams']:
        if team['team'] in active_by_team:
          raw_dur = max(0, team['end'] - team['start'])
          if raw_dur < 180 * 1000 and 'gamestart' in team and 'gameend' in team:
            active_by_team[team['team']] += max(0, team['gameend'] - team['gamestart'])
          else:
            active_by_team[team['team']] += raw_dur
    if (active_by_team['RED'] + active_by_team['BLUE']) < 30 * 1000:
      # Spectator who hopped onto RED/BLUE while paused or for <30s of active game clock
      continue
    teams = set([tm for tm in ['RED', 'BLUE'] if active_by_team[tm] >= 30 * 1000])
    if len(teams) != 1:
      print 'Player played on different teams:', sessiongame['_id']['session'], ':', teams
      print sessiongame['games']
      #exit()
      return
    playerteam = teams.pop()
    playtime = sum([team['end'] - team['start'] for game in sessiongame['games'] for team in game['teams'] if team['team'] == playerteam])
    playerid = playermap[json.dumps(sessiongame['_id']['session'])]
    if playerid not in ratings:
      # first check database
      playerratings = ratingdb.find({'_id.player': playerid, 'time': {'$lt': match['time_created']}}).sort('time', pymongo.DESCENDING).limit(1)
      rating = None
      for rating in playerratings:
        break
      if rating != None:
        ratings[playerid] = {'rating': trueskill.Rating(mu=rating['rating']['updated']['raw']['mu'], sigma=rating['rating']['updated']['raw']['sigma']), 'games': rating['num_games']}
      else:
        # check old ratings
        playergames = playergamedb.find({'_id.player': playerid, 'time': {'$lt': match['time_created']}, 'rating.updated.raw.sigma': {'$exists': True}}).sort('time', pymongo.DESCENDING).limit(1)
        playergame = None
        for playergame in playergames:
          break
        if playergame != None:
          numgames = playergamedb.find({'_id.player': playerid, 'time': {'$lt': match['time_created']}, 'rating.updated.raw.sigma': {'$exists': True}}).count()
          ratings[playerid] = {'rating': trueskill.Rating(mu=playergame['rating']['updated']['raw']['mu'], sigma=playergame['rating']['updated']['raw']['sigma']), 'games': numgames}
        else:
          if match['time_created'] > datetime(2021, 4, 10):
            ratings[playerid] = {'rating': trueskill.Rating(mu=15.0), 'games': 0}
          else:
            ratings[playerid] = {'rating': trueskill.Rating(), 'games': 0}

    #update['rating'] = ratings[playerid]['rating']
    rating = ratings[playerid]['rating']
    teamrating[playerteam][playerid] = rating
    teamtime[playerteam][playerid] = playtime
    print 'Player', playerid, 'played for', playtime, 'on team', playerteam, 'on a map lasting', maptime
  teamratings = [teamrating[team] for team in ['RED', 'BLUE']]
  all_times = [t_val for team, players in teamtime.iteritems() for playerid, t_val in players.iteritems()]
  if not all_times:
    return
  maxtime = max(all_times)
  if maxtime > 0 and maptime > maxtime * 2:
    # Pre-map_restart warmup fragment inflated match['map_end_time']; normalize to actual played duration
    maptime = maxtime
  if maxtime > maptime:
    # some bug whereby players played longer than possible, just scale it back for now until bug is found
    for players in teamtime.values():
      for playerid in players.keys():
        players[playerid] = players[playerid] * maptime / maxtime
  foursred = sum([tt * 1.0 / maptime for id, tt in teamtime['RED'].iteritems()])
  foursblue = sum([tt * 1.0 / maptime for id, tt in teamtime['BLUE'].iteritems()])
  pbulk = sessionplayerdb.initialize_unordered_bulk_op()
  ratingbulk = ratingdb.initialize_unordered_bulk_op()
  if abs(foursred - foursblue) > 0.5:
    print 'unbalanced teams, skipping!'
    ratingbulk.find({'_id.match': match['_id']}).remove()
    print ratingbulk.execute()
    return
  if foursred < 2.5 or foursblue < 2.5:
    print 'skipping match with less than 3s!'
    ratingbulk.find({'_id.match': match['_id']}).remove()
    print ratingbulk.execute()
    return
  if foursred > 5.5 or foursblue > 5.5:
    print 'skipping match with more than 5s!'
    ratingbulk.find({'_id.match': match['_id']}).remove()
    print ratingbulk.execute()
    return
  teamtimes = {}
  for idx, team in enumerate(['RED', 'BLUE']):
    for id, tt in teamtime[team].iteritems():
      teamtimes[(idx, id)] = tt * 1.0 / maptime
  print teamratings, teamtimes
  quality = trueskill.quality(teamratings, weights=teamtimes)
  print('{:.1%} chance to draw'.format(quality))
  updates = trueskill.rate(teamratings, ranks=[result['RED'], result['BLUE']], weights=teamtimes)
  print updates
  for teamupdate in updates:
    for playerid, update in teamupdate.iteritems():
      #playergames = playergamedb.find({'_id.player': playerid, '_id.match': match['_id']})
      #for playergame in playergames:
      #  break
      sessiongame = playergamemap[playerid]
      start = ratings[playerid]['rating']
      def ratingObj(rating):
        return {'raw': {'mu': rating.mu, 'sigma': rating.sigma}, 'friendly': trueskill.expose(rating)}
      ratingbulk.find({'_id': {'player': playerid, 'match': sessiongame['match']}}).upsert().update_one({
        '$set': {
          'rating': {'start': ratingObj(start), 'updated': ratingObj(update)},
          'num_games': ratings[playerid]['games'] + 1,
          'time': match['time_created']
        }})
      pbulk.find({'_id': playerid}).update_one({'$set': {'rating': ratingObj(update)}})
      ratings[playerid]['rating'] = update
      ratings[playerid]['games'] += 1
  ratingbulk.find({'_id.player': {'$not': {'$in': playergamemap.keys()}}, '_id.match': match['_id']}).remove()
  print ratingbulk.execute()
  print pbulk.execute()
  return

LOCK_DIR = '/tmp/jactf_locks'
RANK_LOCK_PATH = '/tmp/jactf_locks/calculate_ranks_v4.lock'
RANK_STATUS_PATH = '/tmp/jactf_locks/calculate_ranks_v4.status.json'
RANK_BG_LOG_PATH = '/tmp/jactf_locks/calculate_ranks_v4_bg.log'

def _ensure_lock_dir():
  if not os.path.isdir(LOCK_DIR):
    old_umask = os.umask(0)
    try:
      os.makedirs(LOCK_DIR, 0777)
    except OSError:
      pass
    finally:
      os.umask(old_umask)
  try:
    os.chmod(LOCK_DIR, 0777)
  except OSError:
    pass

def _open_shared_file(path, flags=os.O_CREAT | os.O_RDWR, mode_str='r+'):
  _ensure_lock_dir()
  old_umask = os.umask(0)
  try:
    fd = os.open(path, flags, 0666)
  finally:
    os.umask(old_umask)
  try:
    os.chmod(path, 0666)
  except OSError:
    pass
  return os.fdopen(fd, mode_str)

def _write_rank_status(state_dict):
  try:
    _ensure_lock_dir()
    tmp = '%s.tmp.%d' % (RANK_STATUS_PATH, os.getpid())
    old_umask = os.umask(0)
    try:
      with open(tmp, 'w') as f:
        json.dump(state_dict, f)
      os.chmod(tmp, 0666)
    finally:
      os.umask(old_umask)
    os.rename(tmp, RANK_STATUS_PATH)
  except Exception:
    pass

def get_rank_status():
  status_file = RANK_STATUS_PATH if os.path.isfile(RANK_STATUS_PATH) else '/tmp/calculate_ranks_v4.status.json'
  if not os.path.isfile(status_file):
    return {'running': False}
  try:
    with open(status_file, 'r') as f:
      st = json.load(f)
    # Check if lock is actually held; if not and st says running, mark finished
    if st.get('running') and (int(time.time()) - int(st.get('started_at', 0)) > 5):
      import fcntl
      lf = _open_shared_file(RANK_LOCK_PATH)
      try:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
        st['running'] = False
      except IOError:
        pass
      finally:
        lf.close()
    return st
  except Exception:
    return {'running': False}

def recalculate_since(start_datetime, extra_playerids=None):
  import fcntl
  from bson.objectid import ObjectId
  global ratings
  lf = _open_shared_file(RANK_LOCK_PATH)
  fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
  started_ts = int(time.time())
  my_pid = os.getpid()
  try:
    trueskill.TrueSkill(backend='mpmath').make_as_global()
    ratings = {}
    db = MongoClient("mongodb").demos
    matchdb = db.minmatches
    ratingdb = db.playerGameRatings
    sessionplayerdb = db.sessionPlayers

    # Recover any interrupted prior replay checkpoint so preempting a worker is 100% lossless
    ckpt = db.profileLinkConfig.find_one({'_id': 'rank_replay_checkpoint'})
    ckpt_pids = set()
    if ckpt:
      if ckpt.get('since') and ckpt['since'] < start_datetime:
        start_datetime = ckpt['since']
      for p in ckpt.get('affected_pids', []):
        if p is not None:
          ckpt_pids.add(p if isinstance(p, ObjectId) else ObjectId(str(p)))

    affected_pids = set(ratingdb.distinct('_id.player', {'time': {'$gte': start_datetime}}))
    affected_pids.update(ckpt_pids)
    if extra_playerids:
      for pid in extra_playerids:
        if pid is not None:
          affected_pids.add(pid if isinstance(pid, ObjectId) else ObjectId(str(pid)))

    db.profileLinkConfig.replace_one(
      {'_id': 'rank_replay_checkpoint'},
      {
        '_id': 'rank_replay_checkpoint',
        'since': start_datetime,
        'affected_pids': list(affected_pids),
        'pid': my_pid,
        'started_at': datetime.utcnow(),
      },
      upsert=True
    )

    ratingdb.delete_many({'time': {'$gte': start_datetime}})

    for pid in affected_pids:
      prev = None
      for prev in ratingdb.find({'_id.player': pid, 'time': {'$lt': start_datetime}}).sort('time', pymongo.DESCENDING).limit(1):
        break
      if prev is not None and 'rating' in prev and 'updated' in prev['rating']:
        sessionplayerdb.update_one({'_id': pid}, {'$set': {'rating': prev['rating']['updated']}})
      else:
        sessionplayerdb.update_one({'_id': pid}, {'$unset': {'rating': 1}})

    total_matches = matchdb.find({'ma': True, 't': {'$gte': start_datetime}}).count()
    _write_rank_status({
      'running': True,
      'pid': my_pid,
      'since': str(start_datetime),
      'processed': 0,
      'total': total_matches,
      'started_at': started_ts,
      'updated_at': int(time.time()),
    })
    matches = matchdb.find({'ma': True, 't': {'$gte': start_datetime}}).sort('t', 1)
    idx = 0
    for match in matches:
      idx += 1
      if idx % 25 == 0 or idx == total_matches:
        _write_rank_status({
          'running': True,
          'pid': my_pid,
          'since': str(start_datetime),
          'processed': idx,
          'total': total_matches,
          'started_at': started_ts,
          'updated_at': int(time.time()),
        })
      try:
        updaterank(match)
      except ValueError:
        print traceback.format_exc()
      except SystemExit:
        print 'Skipped match due to missing session player in updaterank:', match.get('_id')
    db.profileLinkConfig.delete_one({'_id': 'rank_replay_checkpoint'})
    _write_rank_status({
      'running': False,
      'since': str(start_datetime),
      'processed': idx,
      'total': total_matches,
      'started_at': started_ts,
      'completed_at': int(time.time()),
    })
    return {'recalculated': idx, 'total': total_matches, 'background': False}
  finally:
    try:
      fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
    except Exception:
      pass
    lf.close()

def trigger_recalculate_since(start_datetime, extra_playerids=None, sync_max_matches=250):
  if start_datetime is None:
    return {'recalculated': 0, 'background': False}
  db = MongoClient("mongodb").demos
  total_matches = db.minmatches.find({'ma': True, 't': {'$gte': start_datetime}}).count()
  if total_matches <= sync_max_matches and not get_rank_status().get('running'):
    return recalculate_since(start_datetime, extra_playerids=extra_playerids)
  epoch = (start_datetime - datetime(1970, 1, 1)).total_seconds()
  cmd = ['/usr/bin/python', '/home/pyservices/calculate_ranks_v4.py', 'since=%.3f' % epoch]
  if extra_playerids:
    cmd.append('pids=' + ','.join([str(p) for p in extra_playerids if p is not None]))
  logf = _open_shared_file(RANK_BG_LOG_PATH, flags=os.O_CREAT | os.O_RDWR | os.O_APPEND, mode_str='a')
  proc = Popen(cmd, stdout=logf, stderr=logf, close_fds=True)
  logf.close()
  if not get_rank_status().get('running'):
    _write_rank_status({
      'running': True,
      'pid': proc.pid,
      'since': str(start_datetime),
      'processed': 0,
      'total': total_matches,
      'started_at': int(time.time()),
    })
  return {'recalculated': 'background', 'queued_matches': total_matches, 'background': True, 'pid': proc.pid}

def queue_or_trigger_recalculate(start_datetime, extra_playerids=None, immediate_max_matches=40):
  if start_datetime is None:
    return {'recalculated': 0, 'queued': False}
  db = MongoClient("mongodb").demos
  total_matches = db.minmatches.find({'ma': True, 't': {'$gte': start_datetime}}).count()
  st = get_rank_status()
  if total_matches <= immediate_max_matches and not st.get('running'):
    return recalculate_since(start_datetime, extra_playerids=extra_playerids)

  cur = db.profileLinkConfig.find_one({'_id': 'pending_rank_rewind'})
  earliest = start_datetime
  pids = set([str(p) for p in (extra_playerids or []) if p is not None])
  mutation_count = 1
  if cur:
    if cur.get('earliest_since') and cur['earliest_since'] < earliest:
      earliest = cur['earliest_since']
    for ep in cur.get('extra_playerids', []):
      if ep:
        pids.add(str(ep))
    mutation_count = int(cur.get('mutation_count', 0)) + 1

  queued_matches = (
    total_matches
    if earliest == start_datetime
    else db.minmatches.find({'ma': True, 't': {'$gte': earliest}}).count()
  )
  doc = {
    '_id': 'pending_rank_rewind',
    'earliest_since': earliest,
    'extra_playerids': sorted(list(pids)),
    'queued_matches': queued_matches,
    'mutation_count': mutation_count,
    'updated_at': datetime.utcnow(),
  }
  db.profileLinkConfig.replace_one({'_id': 'pending_rank_rewind'}, doc, upsert=True)
  return {
    'recalculated': 'deferred',
    'queued': True,
    'queued_matches': queued_matches,
    'earliest_since': str(earliest),
    'mutation_count': mutation_count,
  }

def flush_pending_rank_rewind():
  from bson.objectid import ObjectId
  db = MongoClient("mongodb").demos
  cur = db.profileLinkConfig.find_one({'_id': 'pending_rank_rewind'})
  if not cur or not cur.get('earliest_since'):
    return {'ok': True, 'flushed': False, 'rank_status': get_rank_status()}

  earliest = cur['earliest_since']
  extra_pids = []
  for x in cur.get('extra_playerids', []):
    if x:
      try:
        extra_pids.append(ObjectId(str(x)))
      except Exception:
        pass

  st = get_rank_status()
  # Only preempt a running worker if it registered a checkpoint-backed PID
  if st.get('running') and st.get('pid') and db.profileLinkConfig.find_one({'_id': 'rank_replay_checkpoint'}):
    try:
      os.kill(int(st['pid']), signal.SIGTERM)
      time.sleep(0.25)
    except OSError:
      pass

  db.profileLinkConfig.delete_one({'_id': 'pending_rank_rewind'})
  res = trigger_recalculate_since(earliest, extra_playerids=extra_pids, sync_max_matches=0)
  return {'ok': True, 'flushed': True, 'result': res, 'rank_status': get_rank_status()}

if __name__ == '__main__':
  if len(sys.argv) > 1 and sys.argv[1].startswith('since='):
    from bson.objectid import ObjectId
    epoch = float(sys.argv[1].split('=', 1)[1])
    since_dt = datetime.utcfromtimestamp(epoch)
    extra_pids = []
    if len(sys.argv) > 2 and sys.argv[2].startswith('pids='):
      extra_pids = [ObjectId(x) for x in sys.argv[2].split('=', 1)[1].split(',') if x]
    res = recalculate_since(since_dt, extra_playerids=extra_pids)
    print 'Completed recalculate_since:', res
    sys.exit(0)

  trueskill.TrueSkill(backend='mpmath').make_as_global()
  db = MongoClient("mongodb").demos
  #demodb = db.demos
  matchdb = db.minmatches
  playerdb = db.players
  playergamedb = db.playerGames
  ratingdb = db.playerGameRatings
  sessiongamedb = db.sessionGames
  sessionplayerdb = db.sessionPlayers
  
  if len(sys.argv) > 1 and sys.argv[1] == 'reset':
    sys.argv.pop()
    res = recalculate_since(datetime(2000, 1, 1))
    print 'Completed full reset:', res
    sys.exit(0)

  #matches = list(matchdb.find({'is_match': True}))
  startdate = datetime(2016, 8, 17)
  #startdate = [g for g in sessiongamedb.find({'is_match': True}, {'time': 1}).sort('time', pymongo.ASCENDING).limit(1)][0]['time'] - timedelta(seconds=1)
  #print startdate
  #exit()
  # check for usage of index rather than full row scan.
  # since $exists cannot be indexed, index is on is_match = true which should have majority matching.
  # db.playerGames.find({'is_match': true, 'rating.updated.raw.sigma': {'$exists': true}}, {'time':1}).sort({'time':-1}).explain()
  if len(sys.argv) <= 1:
    lastgame = [g for g in ratingdb.find({}, {'time': 1}).sort('time', pymongo.DESCENDING).limit(1)]
    if len(lastgame) > 0:
      print 'Last elo run updated up to', lastgame[0]['time']
      startdate = lastgame[0]['time']
    startdate -= timedelta(days=1)
    pending_rank_doc = db.profileLinkConfig.find_one({'_id': 'pending_rank_matches'})
    if pending_rank_doc and pending_rank_doc.get('matches'):
      pending_mids = list(pending_rank_doc['matches'])
      older_pugs = list(matchdb.find({'_id': {'$in': pending_mids}, 'ma': True, 't': {'$lte': startdate}}, {'t': 1}).sort('t', 1).limit(1))
      db.profileLinkConfig.delete_one({'_id': 'pending_rank_matches'})
      if older_pugs:
        earliest_pending_t = older_pugs[0]['t']
        print 'Delayed PUG sync detected at %s (<= %s); triggering chronological replay' % (earliest_pending_t, startdate)
        res = trigger_recalculate_since(earliest_pending_t)
        print 'Replay result:', res
        sys.exit(0)
  #startdate = datetime(2022, 8, 1)
  #startdate = startdate - timedelta(seconds=5)
  #startdate = datetime(2020, 5, 17, 22, 30, 50)
  #startdate = datetime(2015, 2, 13)
  i = 0
  while True:
    if len(sys.argv) > 1:
      matches = matchdb.find({'_id': {'$in': sys.argv[1:]}}).sort('t', 1)
    else:
      matches = matchdb.find({'ma': True, 't': {'$gt': startdate}}).sort('t', 1)#.limit(1000)
    #matches = matchdb.find({'_id': '038ac62eeb06d6a1'}).sort('t', 1)
    try:
      for match in matches:
        i += 1
        print 'Processing match', i, match['_id']
        print match['t']
        startdate = match['t']
        try:
          updaterank(match)
        except ValueError:
          print traceback.format_exc()
          if match['_id'] not in ['02cb8a6f67bd9e21', '136e49fd7bae0d38', '2057f31a27547dff']:
            #exit()
            #run('create_sessions.py', [match['_id']])
            #run('create_session_players.py', [match['_id']])
            #updaterank(match)
            pass
      break
    except:
      print traceback.format_exc()
      exit()
  sortedratings = [rating for rating in ratings.iteritems() if rating[1]['games'] > 46]  # trueskill website says 46 game minimum for 4:4 games
  sortedratings = sorted(sortedratings, key = lambda entry: trueskill.expose(entry[1]['rating']), reverse=True)
  
  print 'Ratings:'
  for entry in sortedratings[0:80]:
    player = None
    cursor = sessionplayerdb.find({'_id': entry[0]})
    for row in cursor:
      player = row
    print strip_colors(player['name'] if 'name' in player else player['last_name']).encode('utf8'), entry[0], trueskill.expose(entry[1]['rating']), entry[1]
