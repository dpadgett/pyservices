#!/usr/bin/python -u
import time
import sys
import os
import traceback
import urlparse
import StringIO
import json

from pymongo import MongoClient
import pymongo

from bson.objectid import ObjectId
import json
import bson.json_util

import player_lib
import session_alias_lib
import shrinker

from dateutil.parser import *
from dateutil.tz import *
#from datetime import *
import datetime
from dateutil.relativedelta import *
from pytz import timezone

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

args = urlparse.parse_qs(urlparse.urlparse(os.environ['REQUEST_URI']).query)

if 'rpc' not in args:
  print 'Content-type: text/plain'
  print 'Status: 200 OK'
  print ''

  print 'Missing RPC'
  exit()

if args['rpc'] == ['bundle']:
  for arg in ['start', 'end']:
    if arg not in args or len(args[arg]) != 1:
      print 'Missing', arg
      exit()
  start = int(args['start'][0])
  end = int(args['end'][0])
  run('bundle.py', [start, end])
  exit()
elif args['rpc'] == ['matchdemo']:
  if 'id' not in args or len(args['id']) != 1:
    print 'Missing id'
    exit()
  id = args['id'][0]
  fmt = args.get('format', [''])[0]
  if fmt in ('mme', 'mme_init', 'mme_meta'):
    import hashlib
    import mme_stream_lib
    from subprocess import Popen
    cache_key = hashlib.sha1('matchdemo:%s:v1' % id).hexdigest()
    def _spawn_matchdemo(mme_path, meta_path, lock_fd):
      devnull = open('/dev/null', 'wb')
      safe_id = ''.join(c for c in id if c.isalnum() or c in '_-')
      cmd = '/usr/bin/python -u /home/pyservices/merge_matches_predict.py "%s" --raw-stdout | %s - "%s" "%s"' % (
        safe_id, mme_stream_lib.CONVERTER_BIN, mme_path, meta_path
      )
      Popen(['/bin/sh', '-c', cmd], stdin=devnull, stdout=devnull, stderr=devnull, close_fds=False, preexec_fn=mme_stream_lib.make_child_setup([lock_fd]))
      devnull.close()
    mme_stream_lib.handle_mme_request(
      fmt,
      args,
      cache_key,
      _spawn_matchdemo,
      {'kind': 'matchdemo', 'matchid': id},
    )
  zipped = 'zip' in args and len(args['zip']) == 1
  run('merge_matches_predict.py', [id, zipped])
  exit()

print 'Content-type: text/plain'
print 'Status: 200 OK'
print ''

#db = MongoClient("mongodb").demos

def wrap_result(result):
  if args.get('jsonp', None) == ['true']:
    return args['rpc'][0] + ' = ' + bson.json_util.dumps(result) + ';'
  else:
    return bson.json_util.dumps(result)

def _skey(sid):
  return bson.json_util.dumps(sid, sort_keys=True)

def read_matches(cursor):
  raw_matches = [shrinker.inflate_match(m) for m in cursor]
  if not raw_matches:
    return []

  db = MongoClient("mongodb").demos
  sessiongamedb = db.sessionGames
  ratingdb = db.playerGameRatings
  sessiondb = db.sessions
  playerdb = db.sessionPlayers

  mids = [m['_id'] for m in raw_matches]
  sgs_by_mid = {mid: [] for mid in mids}
  unique_sids_by_key = {}
  for sg in sessiongamedb.find({'_id.match': {'$in': mids}}):
    mid = sg['_id']['match']
    if mid in sgs_by_mid:
      sgs_by_mid[mid].append(sg)
    sk = _skey(sg['_id']['session'])
    if sk not in unique_sids_by_key:
      unique_sids_by_key[sk] = sg['_id']['session']

  sessions = {}
  all_pids = set()
  if unique_sids_by_key:
    for s in sessiondb.find({'_id': {'$in': list(unique_sids_by_key.values())}}):
      sessions[_skey(s['_id'])] = s
      if s.get('playerid') is not None:
        all_pids.add(s['playerid'])

  ratings_by_mid = {mid: {} for mid in mids}
  for r in ratingdb.find({'_id.match': {'$in': mids}}):
    mid = r['_id']['match']
    if mid in ratings_by_mid:
      ratings_by_mid[mid][str(r['_id']['player'])] = r

  canonical_by_pid = {}
  if all_pids:
    pdocs = {
      p['_id']: p
      for p in playerdb.find(
        {'_id': {'$in': list(all_pids)}},
        {'name': 1, 'last_name': 1, 'enhanced_account': 1, 'num_matches': 1, 'num_games': 1}
      )
    }
    unverified_oids = [
      oid for oid, pl in pdocs.iteritems()
      if not pl.get('name') and (int(pl.get('num_matches', 0)) >= 2 or int(pl.get('num_games', 0)) >= 3)
    ]
    unv_sess_by_pid = {}
    unv_alias_map = {}
    if unverified_oids:
      unv_skeys = []
      for us in sessiondb.find({'playerid': {'$in': unverified_oids}}, {'_id': 1, 'playerid': 1, 'num_games': 1, 'last_name': 1}):
        unv_sess_by_pid.setdefault(us['playerid'], []).append(us)
        unv_skeys.append(player_lib.canonical_session_key(us['_id']))
      if unv_skeys:
        for adoc in db.sessionAliasSummary.find({'_id': {'$in': unv_skeys}}):
          unv_alias_map[adoc['_id']] = adoc

    for oid, pl in pdocs.iteritems():
      pid_s = str(oid)
      if pl.get('name'):
        canonical_by_pid[pid_s] = {
          'name': pl['name'],
          'verified': True,
          'account': pl.get('enhanced_account'),
        }
      elif oid in unv_sess_by_pid:
        prof_al = session_alias_lib.summarize_profile_aliases(unv_sess_by_pid[oid], unv_alias_map, pl)
        best_nm = prof_al.get('best_name') or pl.get('last_name')
        if best_nm and session_alias_lib.is_helpful_alias(best_nm):
          canonical_by_pid[pid_s] = {
            'name': best_nm,
            'verified': False,
            'account': pl.get('enhanced_account'),
          }

  matches = []
  for match in raw_matches:
    sessiongames = sgs_by_mid.get(match['_id'], [])
    ratings = ratings_by_mid.get(match['_id'], {})

    best_by_cid = {}
    for sg in sessiongames:
      sdoc = sessions.get(_skey(sg['_id']['session']))
      if not sdoc or 'playerid' not in sdoc or sdoc['playerid'] is None:
        continue
      pid_str = str(sdoc['playerid'])
      is_rated = 1 if pid_str in ratings else 0
      is_m = 1 if sg.get('is_match') else 0
      for g in sg.get('games', []):
        cid = g.get('clientid')
        if cid is None:
          continue
        gdur = max(0, int(g.get('end', 0)) - int(g.get('start', 0)))
        cand_rank = (is_rated, is_m, gdur)
        prev_entry = best_by_cid.get(cid)
        if prev_entry is None or cand_rank > prev_entry[0]:
          info = {'player': pid_str}
          if pid_str in ratings:
            info['rating'] = ratings[pid_str]['rating']
          cmeta = canonical_by_pid.get(pid_str)
          if cmeta:
            info['player_name'] = cmeta['name']
            info['player_verified'] = cmeta['verified']
            if cmeta.get('account'):
              info['player_account'] = cmeta['account']
          best_by_cid[cid] = (cand_rank, info)

    match['client_players'] = {str(cid): pair[1] for cid, pair in best_by_cid.iteritems()}

    for demo in match['demos']:
      rel_id = demo['id'][len('/cygdrive/U/demos/'):]
      for sg in sessiongames:
        if len([g for g in sg['games'] if rel_id in g.get('demos', [])]) > 0:
          sdoc = sessions.get(_skey(sg['_id']['session']))
          if sdoc and sdoc.get('playerid') is not None:
            demo['player'] = str(sdoc['playerid'])
            if demo['player'] in ratings:
              demo['rating'] = ratings[demo['player']]['rating']
            cmeta = canonical_by_pid.get(demo['player'])
            if cmeta:
              demo['player_name'] = cmeta['name']
              demo['player_verified'] = cmeta['verified']
              if cmeta.get('account'):
                demo['player_account'] = cmeta['account']
          for g in sg['games']:
            if rel_id not in g.get('demos', []):
              continue
            idx = g['demos'].index(rel_id)
            if 'ns' in g:
              shots = [shot for shot in g['ns']['shots'] if shot['d'] == idx]
              if shots != []:
                demo['ns'] = shots
            if 'bookmarks' in g:
              bookmarks = [bookmark for bookmark in g['bookmarks'] if bookmark['d'] == idx]
              if bookmarks != []:
                demo['bookmarks'] = bookmarks
      if 'player' not in demo and demo.get('client_id') in best_by_cid:
        fb = best_by_cid[demo['client_id']][1]
        demo['player'] = fb['player']
        if 'rating' in fb:
          demo['rating'] = fb['rating']
        if 'player_name' in fb:
          demo['player_name'] = fb['player_name']
          demo['player_verified'] = fb.get('player_verified', False)
        if 'player_account' in fb:
          demo['player_account'] = fb['player_account']
    matches.append(match)
  return matches

if args['rpc'] == ['endmatch']:
  if 'demo' not in args or len(args['demo']) != 1:
    print 'Missing demo'
    exit()
  demo = args['demo'][0]
  origstdin = sys.stdin
  sys.stdin = StringIO.StringIO(json.dumps({'demo': demo}))
  run('endmatch3.py', [])
  sys.stdin = origstdin
elif args['rpc'] == ['newdemos']:
  for arg in ['since']:
    if arg not in args or len(args[arg]) != 1:
      print 'Missing', arg
      exit()
  since = int(args['since'][0])
  run('newdemos.py', [since])
  exit()
elif args['rpc'] == ['recentmatches'] or args['rpc'] == ['searchmatches'] :
  db = MongoClient("mongodb").demos
  matchdb = db.minmatches

  limit = int(args.get('limit', ['5'])[0])
  offset = int(args.get('offset', ['0'])[0])

  if args['rpc'] == ['recentmatches']:
    query = {'ma': True, 'n': {'$nin': ['siege_cargobarge3_v6', 'siege_korriban2_v1', 'siege_hoth3_v6', 'siege_narshaddaa_v5', 'siege_destroyer_v2']}}
  else:
    # searchmatches
    query = {}
    if 'before' in args:
      query['t'] = {'$lte': datetime.datetime.fromtimestamp(int(args['before'][0]))}
    if 'map' in args:
      query['n'] = args['map'][0]
    if 'server' in args:
      query['h'] = args['server'][0]
    if 'match' in args:
      query['ma'] = args['match'][0] == 'true'
    if 'player' in args:
      #$query['demos'] = array('$elemMatch' => array('name' => $_GET['player']));
      if 'ma' in query:
        # duplicate it so that it uses the index
        nameor = []
        for team in ['b', 'r', 'f', 's']:
          teamquery = dict(query)
          teamquery['sc.%s.n' % (team)] = args['player'][0]
          nameor.append(teamquery)
        query = {'$or': nameor}
      else:
        query['$or'] = [
            {'sc.b.n': args['player'][0]},
            {'sc.r.n': args['player'][0]},
            {'sc.f.n': args['player'][0]},
            {'sc.s.n': args['player'][0]}]

  cursor = matchdb.find(query).sort('t', -1)
  total = cursor.count()
  cursor = cursor.skip(offset).limit(limit)
  matches = read_matches(cursor)
  print wrap_result({'result': matches, 'offset': offset, 'limit': limit, 'total': total})
elif args['rpc'] == ['lookupmatch']:
  db = MongoClient("mongodb").demos
  matchdb = db.minmatches

  limit = int(args.get('limit', ['5'])[0])
  offset = int(args.get('offset', ['0'])[0])

  cursor = matchdb.find({'_id': args['id'][0]}).sort('t', -1)
  total = cursor.count()
  cursor = cursor.skip(offset).limit(limit)
  matches = read_matches(cursor)
  print wrap_result({'result': matches, 'offset': offset, 'limit': limit, 'total': total})
elif args['rpc'] == ['numdemos']:
  db = MongoClient("mongodb").demos
  demodb = db.mindemos
  print wrap_result({'numdemos': demodb.estimated_document_count()})
elif args['rpc'] == ['topnames']:
  db = MongoClient("mongodb").demos
  cursor = db.minnames.find()
  names = [name['_id'] for name in cursor]
  print wrap_result(names)
elif args['rpc'] == ['topmaps']:
  db = MongoClient("mongodb").demos
  cursor = db.minmaps.find()
  maps = [mapname['_id'] for mapname in cursor]
  print wrap_result(maps)
  db = MongoClient("mongodb").demos
elif args['rpc'] == ['topservers']:
  db = MongoClient("mongodb").demos
  cursor = db.minservers.find()
  servers = [server['_id'] for server in cursor]
  print wrap_result(servers)
elif args['rpc'] == ['recentdemos'] or args['rpc'] == ['search'] or args['rpc'] == ['lookupdemo']:
  db = MongoClient("mongodb").demos
  demodb = db.mindemos

  limit = int(args.get('limit', ['10'])[0])
  offset = int(args.get('offset', ['0'])[0])

  query = {}
  if args['rpc'] == ['lookupdemo'] and 'id' in args:
    query['_id'] = args['id'][0]
  elif args['rpc'] == ['search']:
    if 'player' in args:
      query['p'] = args['player'][0]
    if 'before' in args:
      bval = args['before'][0]
      try:
        dt = datetime.datetime.fromtimestamp(int(bval))
      except ValueError:
        dt = parse(bval)
      query['t'] = {'$lte': dt}
    if 'map' in args:
      query['m.m'] = {'$elemMatch': {'n': args['map'][0]}}
    if 'server' in args:
      query['m.h'] = args['server'][0]
    if 'match' in args:
      query['ma'] = args['match'][0] == 'true'

  proj = {
    't': 1,
    'p': 1,
    'm.c.id': 1,
    'm.h': 1,
    'm.m.n': 1,
    'm.m.s': 1,
    'm.m.e': 1,
    'm.m.sc': 1,
    'm.m.te': 1,
  }
  cursor = demodb.find(query, proj).sort('mt', -1)
  total = demodb.estimated_document_count() if not query else cursor.count()
  if args['rpc'] == ['recentdemos']:
    raw_docs = list(cursor.skip(offset).limit(limit + 60))
    demos = []
    for doc in raw_docs:
      inf = shrinker.inflate(doc)
      maps = [x.get('mapname', '') for x in inf.get('metadata', {}).get('maps', [])]
      if any(mp.startswith('siege_') and mp not in ('siege_hoth', 'siege_desert', 'siege_korriban') for mp in maps):
        continue
      did = inf.get('_id', '')
      if did.startswith('/cygdrive/U/demos/') and os.path.exists(did) and os.path.getsize(did) < 256:
        continue
      demos.append(inf)
      if len(demos) >= limit:
        break
  else:
    cursor = cursor.skip(offset).limit(limit)
    demos = [shrinker.inflate(doc) for doc in cursor]
  print wrap_result({'result': demos, 'offset': offset, 'limit': limit, 'total': total})

