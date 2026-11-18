#!/usr/bin/python -u
# -*- coding: utf-8 -*-
"""
Computes and caches informative per-session and per-profile player aliases
from db.sessionGames, prioritizing competitive RED/BLUE match playtime and
filtering out unhelpful spectator/placeholder aliases (Padawan, spec, AFK, etc.).
"""
import re
import player_lib

UNHELPFUL_EXACT = set([
  '', 'padawan', 'pedowan', 'pedowan*', 'unnamedplayer', 'unnamed',
  'spec', 'specs', 'spectator', 'spectating', 'speccing',
  'player', 'play', 'playing', 'playing if needed', 'steam if 4s',
  'jedi', 'jedi knight', 'afk', 'sith', 'unknown', 'elo bot',
  '.', '..', '...', '-', '?', '*'
])

UNHELPFUL_RE = re.compile(
  r'^(?:\[?\s*afk\s*\]?(?:\s*\d+\s*(?:m|min|mins|h|hr)?)?|spec(?:tator|cing|ing)?\s*\d*|padawan\s*\d*)$',
  re.I
)


def strip_colors(s):
  if not s:
    return u''
  if isinstance(s, str):
    try:
      s = s.decode('utf-8')
    except Exception:
      s = s.decode('cp1252', 'replace')
  out = []
  i = 0
  n = len(s)
  while i < n:
    if s[i] == u'^' and i + 1 < n and u'0' <= s[i + 1] <= u'9':
      i += 2
      continue
    if ord(s[i]) >= 0x20:
      out.append(s[i])
    i += 1
  return u''.join(out).strip()


def is_helpful_alias(raw_name):
  c = strip_colors(raw_name).lower().strip()
  if len(c) <= 1 or c in UNHELPFUL_EXACT or UNHELPFUL_RE.match(c):
    return False
  # Strip leading/trailing [afk] tags to see if anything real remains
  stripped_afk = re.sub(r'\[?\b(?:afk|spec)\b\]?', '', c).strip(' -_()*[]')
  if not stripped_afk or stripped_afk in UNHELPFUL_EXACT:
    return False
  return True


def _compute_for_sessions(db, sess_list):
  """Scan db.sessionGames for sess_list and upsert into db.sessionAliasSummary."""
  if not sess_list:
    return {}
  by_skey = {}
  sids = []
  for s in sess_list:
    sk = player_lib.canonical_session_key(s['_id'])
    by_skey[sk] = s
    sids.append(s['_id'])

  stats_by_skey = {sk: {} for sk in by_skey.keys()}

  if len(sids) > 2500:
    cur = db.sessionGames.find(
      {},
      {'_id.session': 1, 'is_match': 1, 'games.names': 1, 'games.teams': 1}
    )
    chunks = [cur]
  else:
    chunks = []
    batch_size = 350
    for idx in range(0, len(sids), batch_size):
      chunk = sids[idx:idx + batch_size]
      chunks.append(db.sessionGames.find(
        {'_id.session': {'$in': chunk}},
        {'_id.session': 1, 'is_match': 1, 'games.names': 1, 'games.teams': 1}
      ))

  for cur in chunks:
    for sg in cur:
      sk = player_lib.canonical_session_key(sg['_id']['session'])
      nmap = stats_by_skey.get(sk)
      if nmap is None:
        continue
      is_m = bool(sg.get('is_match'))
      seen_in_match = set()
      for g in sg.get('games', []):
        rb_ivs = []
        act_ivs = []
        for t in g.get('teams', []):
          tm = t.get('team')
          ts, te = int(t.get('start', 0)), int(t.get('end', 0))
          if te > ts:
            if tm in ('RED', 'BLUE'):
              rb_ivs.append((ts, te))
              act_ivs.append((ts, te))
            elif tm == 'FREE':
              act_ivs.append((ts, te))
        for nm in g.get('names', []):
          raw = nm.get('name') or u''
          if not raw:
            continue
          ns, ne = int(nm.get('start', 0)), int(nm.get('end', 0))
          dur = max(0, ne - ns)
          rec = nmap.get(raw)
          if rec is None:
            rec = {'match_ms': 0, 'active_ms': 0, 'total_ms': 0, 'matches': 0}
            nmap[raw] = rec
          rec['total_ms'] += dur
          if act_ivs and dur > 0:
            rec['active_ms'] += sum([max(0, min(ne, pe) - max(ns, ps)) for ps, pe in act_ivs])
          if is_m and rb_ivs:
            ov = sum([max(0, min(ne, pe) - max(ns, ps)) for ps, pe in rb_ivs])
            if ov > 0:
              rec['match_ms'] += ov
              seen_in_match.add(raw)
      for raw in seen_in_match:
        nmap[raw]['matches'] += 1

  out = {}
  bulk = db.sessionAliasSummary.initialize_unordered_bulk_op()
  has_ops = False
  for sk, s in by_skey.iteritems():
    nmap = stats_by_skey.get(sk, {})
    items = []
    for raw, rec in nmap.iteritems():
      helpful = is_helpful_alias(raw)
      items.append({
        'name': raw,
        'clean': strip_colors(raw),
        'helpful': helpful,
        'match_ms': int(rec['match_ms']),
        'active_ms': int(rec['active_ms']),
        'total_ms': int(rec['total_ms']),
        'matches': int(rec['matches']),
      })

    # Rank helpful names with competitive match playtime first, then active playtime, then total playtime
    items.sort(
      key=lambda x: (
        1 if x['helpful'] else 0,
        1 if x['match_ms'] > 0 else 0,
        x['match_ms'],
        x['active_ms'],
        x['total_ms'],
      ),
      reverse=True
    )

    fallback_last = s.get('last_name') or u'^7Unnamed'
    if items:
      primary_name = items[0]['name']
    else:
      primary_name = fallback_last

    top_names = [
      {
        'name': it['name'],
        'helpful': it['helpful'],
        'match_ms': it['match_ms'],
        'active_ms': it['active_ms'],
        'total_ms': it['total_ms'],
        'matches': it['matches'],
      }
      for it in items[:5]
    ]
    doc = {
      '_id': sk,
      'num_games': int(s.get('num_games', 0)),
      'primary_name': primary_name,
      'primary_helpful': is_helpful_alias(primary_name),
      'top_names': top_names,
    }
    out[sk] = doc
    bulk.find({'_id': sk}).upsert().replace_one(doc)
    has_ops = True

  if has_ops:
    try:
      bulk.execute()
    except Exception:
      pass
  return out


def ensure_session_aliases(db, session_docs):
  """
  Returns dict {skey: alias_summary_doc} for all given session_docs,
  computing only missing or stale entries incrementally.
  """
  if not session_docs:
    return {}
  skeys = []
  by_skey = {}
  for s in session_docs:
    sk = player_lib.canonical_session_key(s['_id'])
    skeys.append(sk)
    by_skey[sk] = s

  cached = {}
  batch = 800
  for i in range(0, len(skeys), batch):
    chunk = skeys[i:i + batch]
    for doc in db.sessionAliasSummary.find({'_id': {'$in': chunk}}):
      cached[doc['_id']] = doc

  to_compute = []
  for sk, s in by_skey.iteritems():
    cdoc = cached.get(sk)
    if cdoc is None or int(cdoc.get('num_games', -1)) != int(s.get('num_games', 0)):
      to_compute.append(s)

  if to_compute:
    fresh = _compute_for_sessions(db, to_compute)
    cached.update(fresh)
  return cached


def summarize_profile_aliases(session_docs, alias_map, pdoc=None):
  """
  Aggregate top aliases across a profile's sessions so profiles whose last_name
  was 'spec' or 'Padawan' display their real competitive aliases.
  """
  agg = {}
  for s in session_docs:
    sk = player_lib.canonical_session_key(s['_id'])
    adoc = alias_map.get(sk)
    if not adoc:
      continue
    for tn in adoc.get('top_names', []):
      raw = tn['name']
      rec = agg.get(raw)
      if rec is None:
        rec = {
          'name': raw,
          'helpful': bool(tn.get('helpful')),
          'match_ms': 0,
          'active_ms': 0,
          'total_ms': 0,
          'matches': 0,
        }
        agg[raw] = rec
      rec['match_ms'] += int(tn.get('match_ms', 0))
      rec['active_ms'] += int(tn.get('active_ms', 0))
      rec['total_ms'] += int(tn.get('total_ms', 0))
      rec['matches'] += int(tn.get('matches', 0))

  ranked = sorted(
    agg.values(),
    key=lambda x: (
      1 if x['helpful'] else 0,
      1 if x['match_ms'] > 0 else 0,
      x['match_ms'],
      x['active_ms'],
      x['total_ms'],
    ),
    reverse=True
  )

  pdoc = pdoc or {}
  if pdoc.get('name'):
    best_name = pdoc['name']
  elif ranked:
    best_name = ranked[0]['name']
  else:
    best_name = pdoc.get('last_name') or u'^7Unnamed'

  return {
    'best_name': best_name,
    'last_name': pdoc.get('last_name') or u'',
    'top_names': ranked[:5],
  }
