#!/usr/bin/python -u
# -*- coding: utf-8 -*-
from __future__ import division

import io
import json
import os
import re
import struct

try:
    text_type = unicode
except NameError:
    text_type = str

DEMOS_PREFIX = '/cygdrive/U/demos/'
DEFAULT_CACHE_DIR = '/home/pyservices/mme_cache'

MOD_LABELS = {
    'MOD_SABER': 'Saber',
    'MOD_BRYAR_PISTOL': 'Pistol',
    'MOD_BRYAR_PISTOL_ALT': 'Pistol Charge',
    'MOD_BLASTER': 'Blaster',
    'MOD_DISRUPTOR': 'Disruptor',
    'MOD_DISRUPTOR_SNIPER': 'Sniper',
    'MOD_BOWCASTER': 'Bowcaster',
    'MOD_REPEATER': 'Repeater',
    'MOD_REPEATER_ALT': 'Mortar',
    'MOD_REPEATER_ALT_SPLASH': 'Mortar Splash',
    'MOD_DEMP2': 'DEMP2',
    'MOD_DEMP2_ALT': 'DEMP2 Alt',
    'MOD_FLECHETTE': 'Flechette',
    'MOD_FLECHETTE_ALT_SPLASH': 'Flechette Alt',
    'MOD_ROCKET': 'Direct Rocket',
    'MOD_ROCKET_SPLASH': 'Rocket',
    'MOD_ROCKET_HOMING': 'Homing Rocket',
    'MOD_ROCKET_HOMING_SPLASH': 'Homing Splash',
    'MOD_THERMAL': 'Direct Thermal',
    'MOD_THERMAL_SPLASH': 'Thermal',
    'MOD_TRIP_MINE_SPLASH': 'Trip Mine',
    'MOD_TIMED_MINE_SPLASH': 'Timed Mine',
    'MOD_DET_PACK_SPLASH': 'Det Pack',
    'MOD_CONC': 'Concussion',
    'MOD_CONC_ALT': 'Concussion Beam',
    'MOD_FORCE_DARK': 'Force',
    'MOD_SENTRY': 'Sentry',
    'MOD_MELEE': 'Melee',
    'MOD_VEHICLE': 'Vehicle',
    'MOD_TELEFRAG': 'Telefrag',
    'MOD_FALLING': 'Crater',
    'MOD_SUICIDE': 'Self-kill',
    'MOD_TRIGGER_HURT': 'Void / Pit',
    'MOD_CRUSH': 'Crushed',
    'MOD_LAVA': 'Lava',
    'MOD_SLIME': 'Slime',
    'MOD_WATER': 'Drowned',
}


def strip_q3_colors(s):
    if not s:
        return ''
    if not isinstance(s, text_type):
        try:
            s = s.decode('utf-8', 'replace')
        except Exception:
            s = text_type(s)
    out = []
    i = 0
    n = len(s)
    while i < n:
        ch = s[i]
        if ch == u'^' and i + 1 < n and u'0' <= s[i + 1] <= u'9':
            i += 2
            continue
        out.append(ch)
        i += 1
    return u''.join(out).strip()


def format_weapon(mod_name):
    if not mod_name:
        return None
    if mod_name in MOD_LABELS:
        return MOD_LABELS[mod_name]
    s = str(mod_name)
    if s.startswith('MOD_'):
        s = s[4:]
    return s.replace('_', ' ').title()


def other_team(team_str):
    if team_str == 'RED':
        return 'BLUE'
    if team_str == 'BLUE':
        return 'RED'
    return team_str or ''


def load_json_file(path):
    if not path or not os.path.isfile(path):
        return None
    try:
        with io.open(path, 'rb') as f:
            raw = f.read().decode('utf-8', 'replace')
        return json.loads(raw)
    except Exception:
        return None


def compute_map_match_hash(demometa, map_obj):
    if not map_obj:
        return ''
    if 'match_hash' in map_obj and map_obj['match_hash']:
        return str(map_obj['match_hash'])
    version = demometa.get('version', 3) if isinstance(demometa, dict) else 3
    if version >= 2 and 'serverId' in map_obj and 'checksumFeed' in map_obj:
        match_id = ((int(map_obj['serverId']) & 0xFFFFFFFF) << 32) + (
            int(map_obj['checksumFeed']) & 0xFFFFFFFF
        )
        packed = struct.pack('!Q', match_id)
        try:
            return packed.encode('hex')
        except LookupError:
            return packed.hex()
    return ''


def normalize_map_name(name):
    if not name:
        return ''
    return str(name).strip().lower().replace('\\', '/')


def select_matching_map(demometa, start_raw, end_raw, map_name):
    if not isinstance(demometa, dict):
        return (0, None)
    maps = demometa.get('maps') or []
    if not maps:
        return (0, None)
    if len(maps) == 1:
        return (0, maps[0])

    want_map = normalize_map_name(map_name)
    best_idx = 0
    best_score = -1e18
    for idx, m in enumerate(maps):
        score = 0.0
        m_name = normalize_map_name(m.get('mapname'))
        if want_map and m_name:
            if m_name == want_map or m_name.endswith('/' + want_map) or want_map.endswith('/' + m_name):
                score += 1000000.0
        ms_raw = m.get('map_start_time_raw')
        me_raw = m.get('map_end_time_raw')
        if ms_raw is not None and me_raw is not None and end_raw > start_raw:
            ov_start = max(int(start_raw) - 2500, int(ms_raw))
            ov_end = min(int(end_raw) + 2500, int(me_raw))
            if ov_end > ov_start:
                score += float(ov_end - ov_start)
            else:
                score -= abs(float(int(ms_raw) - int(start_raw))) * 0.1
        else:
            dur = int(m.get('map_end_time', 0)) - int(m.get('map_start_time', 0))
            score += dur * 0.01
        if score > best_score:
            best_score = score
            best_idx = idx
    return (best_idx, maps[best_idx])


def get_level_start_raw(map_obj, fallback_start_raw=0):
    if not map_obj:
        return int(fallback_start_raw or 0)
    ms_raw = map_obj.get('map_start_time_raw')
    ms_rel = map_obj.get('map_start_time')
    if ms_raw is not None and ms_rel is not None:
        return int(ms_raw) - int(ms_rel)
    if ms_rel is not None and fallback_start_raw:
        return int(fallback_start_raw) - int(ms_rel)
    return 0


def event_raw_time(ev, level_start_raw):
    if 'time_raw' in ev and ev['time_raw'] is not None:
        return int(ev['time_raw'])
    if 'tr' in ev and ev['tr'] is not None:
        return int(ev['tr'])
    t_rel = ev.get('time', ev.get('t'))
    if t_rel is not None:
        return int(level_start_raw) + int(t_rel)
    return None


def resolve_player_name_at(names_dict, client_id, time_rel=None, time_raw=None):
    if not isinstance(names_dict, dict) or client_id is None:
        return None
    entries = names_dict.get(str(client_id)) or names_dict.get(int(client_id)) or []
    if not entries:
        return None
    best_name = None
    best_dur = -1
    for entry in entries:
        nm = entry.get('name', entry.get('n'))
        if not nm:
            continue
        sr = entry.get('name_start_time_raw', entry.get('sr'))
        er = entry.get('name_end_time_raw', entry.get('er'))
        if time_raw is not None and sr is not None and er is not None:
            if int(sr) - 500 <= int(time_raw) <= int(er) + 500:
                return nm
        s = entry.get('name_start_time', entry.get('s'))
        e = entry.get('name_end_time', entry.get('e'))
        if time_rel is not None and s is not None and e is not None:
            if int(s) - 500 <= int(time_rel) <= int(e) + 500:
                return nm
        dur = 0
        if s is not None and e is not None:
            dur = int(e) - int(s)
        if dur > best_dur:
            best_dur = dur
            best_name = nm
    return best_name


def resolve_player_team_at(teams_dict, client_id, time_rel=None, time_raw=None):
    if not isinstance(teams_dict, dict) or client_id is None:
        return None
    entries = teams_dict.get(str(client_id)) or teams_dict.get(int(client_id)) or []
    if not entries:
        return None
    for entry in entries:
        tm = entry.get('team', entry.get('t'))
        sr = entry.get('team_start_time_raw', entry.get('sr'))
        er = entry.get('team_end_time_raw', entry.get('er'))
        if time_raw is not None and sr is not None and er is not None:
            if int(sr) - 250 <= int(time_raw) <= int(er) + 250:
                return tm
        s = entry.get('team_start_time', entry.get('s'))
        e = entry.get('team_end_time', entry.get('e'))
        if time_rel is not None and s is not None and e is not None:
            if int(s) - 250 <= int(time_rel) <= int(e) + 250:
                return tm
    return entries[-1].get('team', entries[-1].get('t'))


def is_pure_spectator(teams_dict, client_id, start_raw=None, end_raw=None):
    if not isinstance(teams_dict, dict) or client_id is None or int(client_id) < 0:
        return True
    entries = teams_dict.get(str(client_id)) or teams_dict.get(int(client_id)) or []
    if not entries:
        return True
    played_ms = 0
    for entry in entries:
        tm = entry.get('team', entry.get('t'))
        if tm == 'SPECTATOR' or not tm:
            continue
        sr = entry.get('team_start_time_raw', entry.get('sr'))
        er = entry.get('team_end_time_raw', entry.get('er'))
        if sr is not None and er is not None and start_raw is not None and end_raw is not None and end_raw > start_raw:
            ov = min(int(er), int(end_raw)) - max(int(sr), int(start_raw))
            if ov > 0:
                played_ms += ov
        else:
            s = entry.get('team_start_time', entry.get('s'), 0)
            e = entry.get('team_end_time', entry.get('e'), 0)
            if int(e) > int(s):
                played_ms += int(e) - int(s)
    return played_ms < 1000


def query_session_highlights(match_hash, rel_demo_path=None, target_client_id=None, mongo_host='mongodb'):
    """
    Look up Nice Shots (ns.shots) and sessionGames bookmarks in MongoDB.
    Fails open (returns [], []) if MongoDB is unreachable or unindexed.
    """
    if not match_hash:
        return ([], [])
    try:
        from pymongo import MongoClient
        client = MongoClient(
            mongo_host,
            serverSelectionTimeoutMS=400,
            connectTimeoutMS=400,
            socketTimeoutMS=600,
        )
        db = client.demos
        docs = list(db.sessionGames.find({'_id.match': match_hash}))
    except Exception:
        return ([], [])

    ns_shots = []
    bookmarks = []
    rel_norm = rel_demo_path
    if rel_norm and rel_norm.startswith(DEMOS_PREFIX):
        rel_norm = rel_norm[len(DEMOS_PREFIX):]

    for sg in docs:
        for g in sg.get('games', []):
            demos = g.get('demos') or []
            if rel_norm is not None:
                if rel_norm not in demos:
                    continue
                d_idx = demos.index(rel_norm)
                for shot in (g.get('ns') or {}).get('shots') or []:
                    if shot.get('d', 0) == d_idx and 'time' in shot:
                        ns_shots.append({
                            'time': int(shot['time']),
                            'score': float(shot.get('score', 1.0)),
                            'demo': rel_norm,
                        })
                for bm in g.get('bookmarks') or []:
                    if bm.get('d', 0) == d_idx and 'time' in bm:
                        bookmarks.append({
                            'time': int(bm['time']),
                            'mark': bm.get('mark') or 'ns',
                            'demo': rel_norm,
                        })
            else:
                for shot in (g.get('ns') or {}).get('shots') or []:
                    if 'time' in shot:
                        d_i = shot.get('d', 0)
                        d_rel = demos[d_i] if isinstance(d_i, int) and 0 <= d_i < len(demos) else None
                        ns_shots.append({
                            'time': int(shot['time']),
                            'score': float(shot.get('score', 1.0)),
                            'demo': d_rel,
                        })
                for bm in g.get('bookmarks') or []:
                    if 'time' in bm:
                        d_i = bm.get('d', 0)
                        d_rel = demos[d_i] if isinstance(d_i, int) and 0 <= d_i < len(demos) else None
                        bookmarks.append({
                            'time': int(bm['time']),
                            'mark': bm.get('mark') or 'ns',
                            'demo': d_rel,
                        })
    return (ns_shots, bookmarks)


def build_carry_spans_and_flag_deaths(
    ctfevents,
    frags,
    teams_dict,
    level_start_raw,
    active_pov_fn,
    map_end_raw,
):
    """
    Build flag-carry intervals [startRaw, endRaw] whenever a tracked POV player
    picks up the enemy flag, closing at the earliest cap, death, flag return/re-grab,
    team switch to spectator, or map end. Also identifies deaths while carrying flag.
    """
    raw_Actions = []
    for idx, ev in enumerate(ctfevents):
        tr = event_raw_time(ev, level_start_raw)
        if tr is None:
            continue
        raw_Actions.append((tr, 0, idx, 'ctf', ev))
    for idx, fr in enumerate(frags):
        tr = event_raw_time(fr, level_start_raw)
        if tr is None:
            continue
        raw_Actions.append((tr, 1, idx, 'frag', fr))
    raw_Actions.sort(key=lambda item: (item[0], item[1], item[2]))

    # Track active carries keyed by carrier client_id -> dict
    open_carries = {}
    completed_spans = []
    inferred_flag_death_frag_ids = set()
    cap_run_start_by_ctf_idx = {}
    grab_run_end_by_ctf_idx = {}

    def close_carry(cid, end_raw, outcome, cap_ctf_idx=None):
        carry = open_carries.pop(cid, None)
        if not carry:
            return
        s_raw = carry['start_raw']
        e_raw = max(s_raw, int(end_raw))
        span = {
            'start_raw': s_raw,
            'end_raw': e_raw,
            'carrier_id': cid,
            'team': carry['team'],
            'flagTeam': carry['flagTeam'],
            'outcome': outcome,
            'grab_ctf_idx': carry['grab_ctf_idx'],
            'pov': carry['pov'],
        }
        completed_spans.append(span)
        grab_run_end_by_ctf_idx[carry['grab_ctf_idx']] = (e_raw, outcome)
        if cap_ctf_idx is not None:
            cap_run_start_by_ctf_idx[cap_ctf_idx] = s_raw

    for tr, _, idx, kind, obj in raw_Actions:
        # Check if any active carrier left their team before tr
        for cid in list(open_carries.keys()):
            carry = open_carries[cid]
            cur_team = resolve_player_team_at(teams_dict, cid, time_raw=tr)
            if cur_team and cur_team != carry['team']:
                close_carry(cid, tr, 'drop')

        if kind == 'ctf':
            etype = obj.get('eventtype')
            actor = obj.get('attacker')
            flag_team = obj.get('team')
            if etype == 'PLAYER_GOT_FLAG' and actor is not None:
                actor = int(actor)
                # Close any stale carry on the same flag_team (e.g. /dropflag picked up by teammate)
                for cid in list(open_carries.keys()):
                    if open_carries[cid]['flagTeam'] == flag_team:
                        close_carry(cid, tr, 'drop')
                pov_cid = active_pov_fn(tr)
                is_pov = (pov_cid is not None and actor == int(pov_cid))
                if is_pov:
                    carrier_team = other_team(flag_team)
                    open_carries[actor] = {
                        'start_raw': tr,
                        'team': carrier_team,
                        'flagTeam': flag_team,
                        'grab_ctf_idx': idx,
                        'pov': True,
                    }
            elif etype == 'PLAYER_CAPTURED_FLAG':
                actor = int(actor) if actor is not None else -1
                if actor in open_carries:
                    close_carry(actor, tr, 'cap', cap_ctf_idx=idx)
                for cid in list(open_carries.keys()):
                    if open_carries[cid]['flagTeam'] == flag_team:
                        close_carry(cid, tr, 'drop')
            elif etype in ('PLAYER_RETURNED_FLAG', 'FLAG_RETURNED'):
                for cid in list(open_carries.keys()):
                    if open_carries[cid]['flagTeam'] == flag_team:
                        close_carry(cid, tr, 'drop')
        elif kind == 'frag':
            target = obj.get('target')
            if target is not None:
                target = int(target)
                if target in open_carries:
                    inferred_flag_death_frag_ids.add(idx)
                    close_carry(target, tr, 'death')

    if map_end_raw:
        for cid in list(open_carries.keys()):
            close_carry(cid, map_end_raw, 'end')

    return (
        completed_spans,
        inferred_flag_death_frag_ids,
        cap_run_start_by_ctf_idx,
        grab_run_end_by_ctf_idx,
    )


def dedupe_single_map_events(ctfevents, frags, level_start_raw):
    """
    JKA temporary entity events (EV_CTFMESSAGE / EV_OBITUARY) live for EVENT_VALID_MSEC (~300ms)
    and can occasionally be emitted on two adjacent snapshots in .dm_meta (~320ms apart).
    Deduplicate identical events within 650ms (CTF) / 250ms (frags) before state-machine analysis.
    """
    clean_ctf = []
    for ev in ctfevents or []:
        tr = event_raw_time(ev, level_start_raw)
        if tr is None:
            continue
        etype = ev.get('eventtype')
        att = ev.get('attacker')
        tm = ev.get('team')
        if any(
            prev_tr is not None
            and prev.get('eventtype') == etype
            and prev.get('attacker') == att
            and prev.get('team') == tm
            and abs(prev_tr - tr) <= 650
            for prev_tr, prev in clean_ctf
        ):
            continue
        clean_ctf.append((tr, ev))

    clean_frags = []
    for fr in frags or []:
        tr = event_raw_time(fr, level_start_raw)
        if tr is None:
            continue
        att = fr.get('attacker')
        tgt = fr.get('target')
        mod = fr.get('mod')
        if any(
            prev_tr is not None
            and prev.get('attacker') == att
            and prev.get('target') == tgt
            and prev.get('mod') == mod
            and abs(prev_tr - tr) <= 650
            for prev_tr, prev in clean_frags
        ):
            continue
        clean_frags.append((tr, fr))

    return ([ev for _, ev in clean_ctf], [fr for _, fr in clean_frags])


def build_timeline_from_map_data(
    map_obj,
    start_raw,
    end_raw,
    mode,
    active_pov_fn,
    pov_client_id=None,
    pov_name=None,
    ns_shots=None,
    extra_bookmarks=None,
    trim_focus=None,
    cuts_out=None,
):
    duration_ms = max(0, int(end_raw) - int(start_raw))
    if duration_ms <= 0 or not isinstance(map_obj, dict):
        return None

    level_start_raw = get_level_start_raw(map_obj, start_raw)
    names_dict = map_obj.get('names') or {}
    teams_dict = map_obj.get('teams') or {}
    ctfevents, frags = dedupe_single_map_events(
        map_obj.get('ctfevents') or [],
        map_obj.get('frags') or [],
        level_start_raw,
    )
    map_bookmarks = map_obj.get('bookmarks') or []
    map_end_raw = map_obj.get('map_end_time_raw')
    if map_end_raw is None:
        map_end_raw = level_start_raw + int(map_obj.get('map_end_time', duration_ms))

    is_ctf_map = bool(ctfevents) or ('ctf' in normalize_map_name(map_obj.get('mapname')))
    if mode == 'pov' and not is_ctf_map:
        effective_mode = 'non_ctf'
    else:
        effective_mode = mode

    (
        raw_spans,
        inferred_flag_death_ids,
        cap_run_start_by_ctf_idx,
        grab_run_end_by_ctf_idx,
    ) = build_carry_spans_and_flag_deaths(
        ctfevents=ctfevents,
        frags=frags,
        teams_dict=teams_dict,
        level_start_raw=level_start_raw,
        active_pov_fn=active_pov_fn,
        map_end_raw=map_end_raw,
    )

    events = []

    def name_for(cid, fallback_name, tr):
        if fallback_name:
            return fallback_name
        resolved = resolve_player_name_at(names_dict, cid, time_raw=tr)
        if resolved:
            return resolved
        return 'Player %s' % cid if cid is not None else ''

    # 1. CTF Events (Captures, Returns, Grabs, plus fallback FC kills if missing in frags)
    fc_frag_times_by_attacker = {}
    for fr_idx, fr in enumerate(frags):
        if fr.get('target_had_flag') == 1:
            tr = event_raw_time(fr, level_start_raw)
            att = fr.get('attacker')
            if tr is not None and att is not None:
                fc_frag_times_by_attacker.setdefault(int(att), []).append(tr)

    for ctf_idx, ev in enumerate(ctfevents):
        tr = event_raw_time(ev, level_start_raw)
        if tr is None:
            continue
        t_off = tr - int(start_raw)
        if t_off < 0 or t_off > duration_ms:
            continue

        etype = ev.get('eventtype')
        actor = ev.get('attacker')
        actor = int(actor) if actor is not None else None
        flag_team = ev.get('team') or ''
        pov_at_t = active_pov_fn(tr)
        is_actor_pov = (pov_at_t is not None and actor is not None and actor == int(pov_at_t))
        actor_name = name_for(actor, ev.get('attacker_name'), tr)

        if etype == 'PLAYER_CAPTURED_FLAG':
            scoring_team = other_team(flag_team) or resolve_player_team_at(teams_dict, actor, time_raw=tr) or ''
            ev_out = {
                't': int(t_off),
                'type': 'cap',
                'pov': True if effective_mode == 'spectator' else bool(is_actor_pov),
                'team': scoring_team,
                'flagTeam': flag_team,
                'actorId': actor,
                'actor': actor_name,
                'actorClean': strip_q3_colors(actor_name),
            }
            if ctf_idx in cap_run_start_by_ctf_idx:
                ev_out['runStartMs'] = max(0, int(cap_run_start_by_ctf_idx[ctf_idx]) - int(start_raw))
            events.append(ev_out)

        elif etype == 'PLAYER_RETURNED_FLAG':
            if effective_mode == 'spectator':
                continue
            if not is_actor_pov:
                continue
            events.append({
                't': int(t_off),
                'type': 'flag_return',
                'pov': True,
                'team': flag_team or resolve_player_team_at(teams_dict, actor, time_raw=tr) or '',
                'flagTeam': flag_team,
                'actorId': actor,
                'actor': actor_name,
                'actorClean': strip_q3_colors(actor_name),
            })

        elif etype == 'PLAYER_GOT_FLAG':
            if effective_mode in ('spectator', 'matchdemo'):
                continue
            if not is_actor_pov:
                continue
            carrier_team = other_team(flag_team) or resolve_player_team_at(teams_dict, actor, time_raw=tr) or ''
            ev_out = {
                't': int(t_off),
                'type': 'flag_grab',
                'pov': True,
                'team': carrier_team,
                'flagTeam': flag_team,
                'actorId': actor,
                'actor': actor_name,
                'actorClean': strip_q3_colors(actor_name),
            }
            if ctf_idx in grab_run_end_by_ctf_idx:
                e_raw, outcome = grab_run_end_by_ctf_idx[ctf_idx]
                ev_out['runEndMs'] = min(duration_ms, max(0, int(e_raw) - int(start_raw)))
                ev_out['runOutcome'] = outcome
            events.append(ev_out)

        elif etype == 'FRAGGED_FLAG_CARRIER' and actor is not None:
            # Only emit fallback if no detailed `frags` entry with target_had_flag==1 matches within 300ms
            near = fc_frag_times_by_attacker.get(actor) or []
            if any(abs(ft - tr) <= 300 for ft in near):
                continue
            if effective_mode != 'spectator' and not is_actor_pov:
                continue
            events.append({
                't': int(t_off),
                'type': 'fc_kill',
                'pov': True,
                'team': flag_team or resolve_player_team_at(teams_dict, actor, time_raw=tr) or '',
                'flagTeam': flag_team,
                'actorId': actor,
                'actor': actor_name,
                'actorClean': strip_q3_colors(actor_name),
            })

    # 2. Frags (FC Kills, POV Flag Deaths, and Non-CTF POV Kills fallback)
    for fr_idx, fr in enumerate(frags):
        tr = event_raw_time(fr, level_start_raw)
        if tr is None:
            continue
        t_off = tr - int(start_raw)
        if t_off < 0 or t_off > duration_ms:
            continue

        att = fr.get('attacker')
        tgt = fr.get('target')
        att = int(att) if att is not None else None
        tgt = int(tgt) if tgt is not None else None
        mod_name = fr.get('mod_name') or ''
        weapon = format_weapon(mod_name)
        had_flag = (fr.get('target_had_flag') == 1) or (fr_idx in inferred_flag_death_ids)
        pov_at_t = active_pov_fn(tr)
        is_att_pov = (pov_at_t is not None and att is not None and att == int(pov_at_t))
        is_tgt_pov = (pov_at_t is not None and tgt is not None and tgt == int(pov_at_t))

        att_name = name_for(att, fr.get('attacker_name'), tr)
        tgt_name = name_for(tgt, fr.get('target_name'), tr)
        att_team = fr.get('attacker_team') or resolve_player_team_at(teams_dict, att, time_raw=tr) or ''
        tgt_team = fr.get('target_team') or resolve_player_team_at(teams_dict, tgt, time_raw=tr) or ''

        # 2a. Flag Carrier Kill (enemy kill)
        if fr.get('target_had_flag') == 1 and att != tgt and att_team != tgt_team:
            if effective_mode == 'spectator' or is_att_pov:
                events.append({
                    't': int(t_off),
                    'type': 'fc_kill',
                    'pov': True,
                    'team': att_team,
                    'flagTeam': att_team,
                    'actorId': att,
                    'actor': att_name,
                    'actorClean': strip_q3_colors(att_name),
                    'targetId': tgt,
                    'target': tgt_name,
                    'targetClean': strip_q3_colors(tgt_name),
                    'weapon': weapon,
                })

        # 2b. POV Died Carrying Flag (killed by enemy, cratered, voided, or /kill while carrying)
        if had_flag and is_tgt_pov and effective_mode not in ('spectator', 'matchdemo'):
            events.append({
                't': int(t_off),
                'type': 'flag_death',
                'pov': True,
                'team': tgt_team,
                'flagTeam': other_team(tgt_team),
                'actorId': att if att != tgt else tgt,
                'actor': att_name if att != tgt else tgt_name,
                'actorClean': strip_q3_colors(att_name if att != tgt else tgt_name),
                'targetId': tgt,
                'target': tgt_name,
                'targetClean': strip_q3_colors(tgt_name),
                'selfKill': bool(att == tgt or mod_name in ('MOD_SUICIDE', 'MOD_FALLING', 'MOD_TRIGGER_HURT')),
                'weapon': weapon,
            })

        # 2c. Non-CTF POV Kills fallback (FFA / Duel / TFFA / Siege)
        if effective_mode == 'non_ctf' and is_att_pov and att != tgt and mod_name != 'MOD_SUICIDE':
            if not (att_team in ('RED', 'BLUE') and att_team == tgt_team):
                events.append({
                    't': int(t_off),
                    'type': 'kill',
                    'pov': True,
                    'team': att_team,
                    'actorId': att,
                    'actor': att_name,
                    'actorClean': strip_q3_colors(att_name),
                    'targetId': tgt,
                    'target': tgt_name,
                    'targetClean': strip_q3_colors(tgt_name),
                    'weapon': weapon,
                })

    # 3. Bookmarks (from .dm_meta + sessionGames, deduplicated within 500ms)
    bm_times = []
    all_bms = []
    for bm in map_bookmarks:
        tr = event_raw_time(bm, level_start_raw)
        if tr is not None:
            all_bms.append((tr, bm.get('mark', bm.get('m')) or 'mark'))
    for bm in (extra_bookmarks or []):
        tr = int(level_start_raw) + int(bm['time'])
        all_bms.append((tr, bm.get('mark') or 'ns'))

    for tr, mark_label in sorted(all_bms, key=lambda x: x[0]):
        t_off = tr - int(start_raw)
        if t_off < 0 or t_off > duration_ms:
            continue
        if any(abs(prev - t_off) <= 500 for prev in bm_times):
            continue
        bm_times.append(t_off)
        events.append({
            't': int(t_off),
            'type': 'bookmark',
            'pov': True,
            'team': '',
            'actorId': pov_client_id,
            'actor': pov_name or '',
            'actorClean': strip_q3_colors(pov_name or ''),
            'detail': str(mark_label),
        })

    # 4. ML Nice Shots (from sessionGames)
    ns_times = []
    for shot in sorted(ns_shots or [], key=lambda s: s.get('time', 0)):
        tr = int(level_start_raw) + int(shot['time'])
        t_off = tr - int(start_raw)
        if t_off < 0 or t_off > duration_ms:
            continue
        if any(abs(prev - t_off) <= 400 for prev in ns_times):
            continue
        ns_times.append(t_off)
        raw_score = float(shot.get('score', 1.0))
        rating_10 = int(round(((raw_score - 1.0) * 10.0) + 1.0))
        events.append({
            't': int(t_off),
            'type': 'nice_shot',
            'pov': True,
            'team': '',
            'actorId': pov_client_id,
            'actor': pov_name or '',
            'actorClean': strip_q3_colors(pov_name or ''),
            'detail': '%d/10' % rating_10,
            'score': round(raw_score, 3),
        })

    # 5. Focal marker for trimdemo.php if not already matched by NS/Bookmark
    if trim_focus and trim_focus.get('focus_time') is not None:
        focus_raw = int(level_start_raw) + int(trim_focus['focus_time'])
        focus_off = focus_raw - int(start_raw)
        if 0 <= focus_off <= duration_ms:
            has_near_highlight = any(
                e['type'] in ('nice_shot', 'bookmark') and abs(e['t'] - focus_off) <= 750
                for e in events
            )
            if not has_near_highlight:
                prefix = str(trim_focus.get('prefix') or '').strip()
                is_numeric_ns = False
                rating_str = None
                try:
                    sc = float(prefix)
                    is_numeric_ns = True
                    rating_str = '%d/10' % int(round(((sc - 1.0) * 10.0) + 1.0))
                except Exception:
                    pass
                events.append({
                    't': int(focus_off),
                    'type': 'nice_shot' if is_numeric_ns else 'bookmark',
                    'pov': True,
                    'team': '',
                    'actorId': pov_client_id,
                    'actor': pov_name or '',
                    'actorClean': strip_q3_colors(pov_name or ''),
                    'detail': rating_str if is_numeric_ns else (prefix or 'Clip Highlight'),
                })

    events.sort(key=lambda e: (e['t'], e['type']))
    for idx, ev in enumerate(events):
        ev['id'] = 'ev_%d' % idx

    # Clamp carry spans to [0, duration_ms]
    carry_spans = []
    for sp in raw_spans:
        if effective_mode == 'matchdemo' and sp.get('outcome') != 'cap':
            continue
        s_off = int(sp['start_raw']) - int(start_raw)
        e_off = int(sp['end_raw']) - int(start_raw)
        if e_off <= 0 or s_off >= duration_ms:
            continue
        clamped_s = max(0, s_off)
        clamped_e = min(duration_ms, e_off)
        if clamped_e - clamped_s < 150:
            continue
        carry_spans.append({
            'startMs': clamped_s,
            'endMs': clamped_e,
            'team': sp['team'],
            'flagTeam': sp['flagTeam'],
            'outcome': sp['outcome'],
        })

    out = {
        'mode': effective_mode,
        'povClientId': pov_client_id,
        'povName': pov_name or '',
        'povNameClean': strip_q3_colors(pov_name or ''),
        'durationMs': duration_ms,
        'events': events,
        'carrySpans': carry_spans,
    }
    if cuts_out:
        out['cuts'] = cuts_out
    return out


def merge_match_demos_map_data(match_doc, match_id):
    """
    Synthesize a unified map_obj from all available .dm_meta files in a match
    (used for mergedemo.php reconstructed POVs and matchdemo highlights).
    """
    demos = match_doc.get('d') or []
    loaded = []
    for d in demos:
        rel_id = d.get('id')
        if not rel_id:
            continue
        full_demo = rel_id if rel_id.startswith('/') else (DEMOS_PREFIX + rel_id)
        dm = load_json_file(full_demo + '.dm_meta')
        if not dm or not dm.get('maps'):
            continue
        matched_map = None
        for m in dm['maps']:
            if compute_map_match_hash(dm, m) == match_id:
                matched_map = m
                break
        if matched_map is None:
            matched_map = dm['maps'][0]
        loaded.append((d.get('c', dm.get('client', {}).get('id')), full_demo, dm, matched_map))

    if not loaded:
        return ([], None)

    # Pick longest map as reference base for names/teams/non-own frags
    longest = max(
        loaded,
        key=lambda item: int(item[3].get('map_end_time', 0)) - int(item[3].get('map_start_time', 0)),
    )
    base_map = longest[3]
    level_start_raw = get_level_start_raw(base_map, 0)

    merged_names = {}
    merged_teams = {}
    for _, _, _, m in loaded:
        for cid, nlist in (m.get('names') or {}).items():
            if cid not in merged_names or len(nlist) > len(merged_names[cid]):
                merged_names[cid] = nlist
        for cid, tlist in (m.get('teams') or {}).items():
            if cid not in merged_teams or len(tlist) > len(merged_teams[cid]):
                merged_teams[cid] = tlist

    # Deduplicate ctfevents across demos (same eventtype, attacker, team within 650ms)
    merged_ctf = []
    for _, _, _, m in loaded:
        ls = get_level_start_raw(m, level_start_raw)
        for ev in m.get('ctfevents') or []:
            tr = event_raw_time(ev, ls)
            if tr is None:
                continue
            etype = ev.get('eventtype')
            att = ev.get('attacker')
            tm = ev.get('team')
            dup = False
            for existing in merged_ctf:
                if (
                    existing.get('eventtype') == etype
                    and existing.get('attacker') == att
                    and existing.get('team') == tm
                    and abs(int(existing['time_raw']) - tr) <= 650
                ):
                    dup = True
                    break
            if not dup:
                ev_copy = dict(ev)
                ev_copy['time_raw'] = tr
                merged_ctf.append(ev_copy)
    merged_ctf.sort(key=lambda e: int(e['time_raw']))

    # Merge frags: prefer ownfrags/attacker's own demo, plus longest demo for remaining attackers
    own_attackers = set()
    merged_frags = []

    def add_frag(fr, ls):
        tr = event_raw_time(fr, ls)
        if tr is None:
            return
        att = fr.get('attacker')
        tgt = fr.get('target')
        mod = fr.get('mod')
        for existing in merged_frags:
            if (
                existing.get('attacker') == att
                and existing.get('target') == tgt
                and existing.get('mod') == mod
                and abs(int(existing['time_raw']) - tr) <= 650
            ):
                if fr.get('target_had_flag') == 1:
                    existing['target_had_flag'] = 1
                return
        fr_copy = dict(fr)
        fr_copy['time_raw'] = tr
        merged_frags.append(fr_copy)

    for cid, _, _, m in loaded:
        if cid is None:
            continue
        ls = get_level_start_raw(m, level_start_raw)
        own_attackers.add(int(cid))
        for fr in m.get('frags') or []:
            if fr.get('attacker') == int(cid):
                add_frag(fr, ls)

    longest_ls = get_level_start_raw(base_map, level_start_raw)
    for fr in base_map.get('frags') or []:
        if fr.get('attacker') not in own_attackers:
            add_frag(fr, longest_ls)

    # Also ensure any target==cid deaths from other demos are captured
    for _, _, _, m in loaded:
        ls = get_level_start_raw(m, level_start_raw)
        for fr in m.get('frags') or []:
            add_frag(fr, ls)

    merged_frags.sort(key=lambda f: int(f['time_raw']))

    synth_map = {
        'mapname': base_map.get('mapname', match_doc.get('n', '')),
        'map_start_time': base_map.get('map_start_time', 0),
        'map_end_time': base_map.get('map_end_time', 0),
        'map_start_time_raw': base_map.get('map_start_time_raw'),
        'map_end_time_raw': base_map.get('map_end_time_raw'),
        'names': merged_names,
        'teams': merged_teams,
        'ctfevents': merged_ctf,
        'frags': merged_frags,
        'bookmarks': [],
    }
    return (loaded, synth_map)


def build_single_or_trim_timeline(demo_path, start_raw, end_raw, map_name, trim_focus=None):
    dm = load_json_file(demo_path + '.dm_meta')
    if not dm:
        return None
    _, map_obj = select_matching_map(dm, start_raw, end_raw, map_name)
    if not map_obj:
        return None

    cid = (dm.get('client') or {}).get('id')
    cid = int(cid) if cid is not None else None
    teams_dict = map_obj.get('teams') or {}
    names_dict = map_obj.get('names') or {}
    pov_name = resolve_player_name_at(names_dict, cid, time_raw=start_raw) or ''

    spectator = is_pure_spectator(teams_dict, cid, start_raw, end_raw)
    mode = 'spectator' if spectator else 'pov'

    match_hash = compute_map_match_hash(dm, map_obj)
    ns_shots, extra_bms = query_session_highlights(match_hash, rel_demo_path=demo_path, target_client_id=cid)

    active_pov_fn = (lambda _tr: None) if spectator else (lambda _tr: cid)
    return build_timeline_from_map_data(
        map_obj=map_obj,
        start_raw=start_raw,
        end_raw=end_raw,
        mode=mode,
        active_pov_fn=active_pov_fn,
        pov_client_id=None if spectator else cid,
        pov_name='' if spectator else pov_name,
        ns_shots=ns_shots,
        extra_bookmarks=extra_bms,
        trim_focus=trim_focus,
    )


def build_merged_pov_timeline(match_id, client_id, start_raw, end_raw):
    try:
        from pymongo import MongoClient
        db = MongoClient('mongodb', serverSelectionTimeoutMS=500, socketTimeoutMS=800).demos
        match_doc = db.minmatches.find_one({'_id': match_id})
    except Exception:
        match_doc = None
    if not match_doc:
        return None

    loaded, synth_map = merge_match_demos_map_data(match_doc, match_id)
    if not synth_map:
        return None

    cid = int(client_id)
    names_dict = synth_map.get('names') or {}
    teams_dict = synth_map.get('teams') or {}
    pov_name = resolve_player_name_at(names_dict, cid, time_raw=start_raw) or ''
    spectator = is_pure_spectator(teams_dict, cid, start_raw, end_raw)
    mode = 'spectator' if spectator else 'pov'

    # Check if any demo in match_doc belongs to cid for Nice Shots / Bookmarks
    cid_rel_demo = None
    for d in match_doc.get('d') or []:
        if d.get('c') == cid:
            cid_rel_demo = d.get('id')
            break
    ns_shots, extra_bms = ([], [])
    if cid_rel_demo:
        ns_shots, extra_bms = query_session_highlights(match_id, rel_demo_path=cid_rel_demo, target_client_id=cid)

    active_pov_fn = (lambda _tr: None) if spectator else (lambda _tr: cid)
    return build_timeline_from_map_data(
        map_obj=synth_map,
        start_raw=start_raw,
        end_raw=end_raw,
        mode=mode,
        active_pov_fn=active_pov_fn,
        pov_client_id=None if spectator else cid,
        pov_name='' if spectator else pov_name,
        ns_shots=ns_shots,
        extra_bookmarks=extra_bms,
    )


def find_matchdemo_cuts_file(match_doc):
    try:
        from pytz import timezone
        mapname = match_doc.get('n', '')
        tm = match_doc.get('t')
        if not mapname or tm is None:
            return None
        if getattr(tm, 'tzinfo', None) is None:
            tm = timezone('UTC').localize(tm, is_dst=True)
        tm = tm.astimezone(timezone('US/Pacific'))
        raw = mapname + '_' + tm.strftime('%Y-%m-%d_%H-%M-%S')
        safe = ''.join(c for c in raw if 0 < ord(c) < 127 and c not in './\\')
        candidate = '/tmp/' + safe + '.cuts.json'
        if os.path.isfile(candidate):
            return candidate
    except Exception:
        pass
    return None


def infer_fallback_matchdemo_cuts(synth_map, level_start_raw):
    """
    Deterministic fallback when a pre-existing /tmp/<match>.dm_26 predates .cuts.json:
    infer active POV windows from the same CTF highlight events merge_matches_predict selects,
    applying the same 5-second spacing rules used by merge_matches_predict.py.
    """
    candidates = []
    for ev in synth_map.get('ctfevents') or []:
        etype = ev.get('eventtype')
        att = ev.get('attacker')
        tr = event_raw_time(ev, level_start_raw)
        if tr is None or att is None or etype == 'FRAGGED_FLAG_CARRIER':
            continue
        candidates.append({'tr': tr, 'att': int(att), 'etype': etype})
    for fr in synth_map.get('frags') or []:
        if fr.get('target_had_flag') == 1 and fr.get('attacker') is not None:
            tr = event_raw_time(fr, level_start_raw)
            if tr is not None:
                candidates.append({'tr': tr, 'att': int(fr['attacker']), 'etype': 'FRAGGED_FLAG_CARRIER'})
    candidates.sort(key=lambda x: x['tr'])
    # Apply merge_matches_predict buffer pruning around captures and dense sequences
    i = 0
    while i < len(candidates) - 1:
        cur = candidates[i]
        nxt = candidates[i + 1]
        if nxt['etype'] == 'PLAYER_CAPTURED_FLAG' and nxt['tr'] - cur['tr'] < 5000:
            del candidates[i]
            i = max(0, i - 1)
            continue
        if cur['etype'] == 'PLAYER_CAPTURED_FLAG' and nxt['tr'] - cur['tr'] < 3000:
            del candidates[i + 1]
            continue
        i += 1
    i = 1
    while i < len(candidates) - 1:
        if candidates[i + 1]['tr'] - candidates[i - 1]['tr'] < 5000:
            del candidates[i]
            continue
        i += 1
    if not candidates:
        return []
    slices = []
    for i, item in enumerate(candidates):
        start_tr = 0 if i == 0 else int((candidates[i - 1]['tr'] + item['tr']) // 2)
        slices.append({'startRaw': start_tr, 'clientId': item['att']})
    return slices


def build_matchdemo_timeline(match_id, start_raw, end_raw):
    try:
        from pymongo import MongoClient
        db = MongoClient('mongodb', serverSelectionTimeoutMS=500, socketTimeoutMS=800).demos
        match_doc = db.minmatches.find_one({'_id': match_id})
    except Exception:
        match_doc = None
    if not match_doc:
        return None

    loaded, synth_map = merge_match_demos_map_data(match_doc, match_id)
    if not synth_map:
        return None

    level_start_raw = get_level_start_raw(synth_map, start_raw)
    names_dict = synth_map.get('names') or {}
    cuts_path = find_matchdemo_cuts_file(match_doc)
    raw_cuts = load_json_file(cuts_path) if cuts_path else None

    cut_slices = []
    if isinstance(raw_cuts, list) and raw_cuts:
        for c in raw_cuts:
            c_ms = c.get('startMillis', 0)
            c_cid = c.get('clientId')
            if c_cid is not None:
                cut_slices.append({
                    'startRaw': int(level_start_raw) + int(c_ms),
                    'clientId': int(c_cid),
                })
    if not cut_slices:
        cut_slices = infer_fallback_matchdemo_cuts(synth_map, level_start_raw)

    cut_slices.sort(key=lambda s: s['startRaw'])

    def active_pov_at(tr):
        if not cut_slices:
            return None
        chosen = cut_slices[0]['clientId']
        for sl in cut_slices:
            if sl['startRaw'] <= tr + 100:
                chosen = sl['clientId']
            else:
                break
        return chosen

    duration_ms = max(0, int(end_raw) - int(start_raw))
    cuts_out = []
    for sl in cut_slices:
        t_off = max(0, int(sl['startRaw']) - int(start_raw))
        if t_off <= duration_ms:
            cid = sl['clientId']
            nm = resolve_player_name_at(names_dict, cid, time_raw=sl['startRaw']) or ('Player %d' % cid)
            cuts_out.append({
                't': t_off,
                'clientId': cid,
                'player': nm,
                'playerClean': strip_q3_colors(nm),
            })

    # Include Nice Shots across the match that happened on-camera
    all_ns, _ = query_session_highlights(match_id, rel_demo_path=None)
    demo_cid_by_rel = {d.get('id'): d.get('c') for d in (match_doc.get('d') or []) if d.get('id')}
    filtered_ns = []
    for shot in all_ns:
        shot_cid = demo_cid_by_rel.get(shot.get('demo'))
        shot_raw = int(level_start_raw) + int(shot['time'])
        if shot_cid is None or active_pov_at(shot_raw) == shot_cid:
            filtered_ns.append(shot)

    return build_timeline_from_map_data(
        map_obj=synth_map,
        start_raw=start_raw,
        end_raw=end_raw,
        mode='matchdemo',
        active_pov_fn=active_pov_at,
        pov_client_id=None,
        pov_name='Match Highlight',
        ns_shots=filtered_ns,
        extra_bookmarks=[],
        cuts_out=cuts_out,
    )


def enrich_mme_meta_bytes(raw_meta_bytes, cache_key, context=None, cache_dir=DEFAULT_CACHE_DIR):
    """
    Given raw JSON bytes from `<cache_key>.meta.json`, attach `"timeline"` metadata
    (caching `<cache_key>.timeline.json` on disk) and return UTF-8 JSON bytes.
    Never raises: on any error returns `raw_meta_bytes` unchanged so demo playback
    cannot fail due to timeline metadata.
    """
    if not context or not raw_meta_bytes:
        return raw_meta_bytes

    try:
        if isinstance(raw_meta_bytes, bytes):
            meta_str = raw_meta_bytes.decode('utf-8', 'replace')
        else:
            meta_str = raw_meta_bytes
        meta_obj = json.loads(meta_str)
    except Exception:
        return raw_meta_bytes

    if not isinstance(meta_obj, dict):
        return raw_meta_bytes

    start_raw = int(meta_obj.get('startTime') or 0)
    end_raw = int(meta_obj.get('endTime') or 0)
    map_name = meta_obj.get('mapName') or ''
    if end_raw <= start_raw:
        return raw_meta_bytes

    tl_cache_path = None
    if cache_dir and cache_key and os.path.isdir(cache_dir):
        tl_cache_path = os.path.join(cache_dir, cache_key + '.timeline.json')
        cached_tl = load_json_file(tl_cache_path)
        if isinstance(cached_tl, dict) and 'timeline' in cached_tl:
            meta_obj['timeline'] = cached_tl['timeline']
            out_str = json.dumps(meta_obj, separators=(',', ':'))
            return out_str.encode('utf-8') if isinstance(out_str, text_type) else out_str

    timeline = None
    try:
        kind = context.get('kind')
        if kind == 'single':
            timeline = build_single_or_trim_timeline(
                demo_path=context.get('demo_path', ''),
                start_raw=start_raw,
                end_raw=end_raw,
                map_name=map_name,
            )
        elif kind == 'trim':
            timeline = build_single_or_trim_timeline(
                demo_path=context.get('demo_path', ''),
                start_raw=start_raw,
                end_raw=end_raw,
                map_name=map_name,
                trim_focus={
                    'focus_time': context.get('focus_time'),
                    'before': context.get('before'),
                    'after': context.get('after'),
                    'prefix': context.get('prefix'),
                },
            )
        elif kind == 'merged':
            timeline = build_merged_pov_timeline(
                match_id=context.get('matchid', ''),
                client_id=context.get('clientid', 0),
                start_raw=start_raw,
                end_raw=end_raw,
            )
        elif kind == 'matchdemo':
            timeline = build_matchdemo_timeline(
                match_id=context.get('matchid', ''),
                start_raw=start_raw,
                end_raw=end_raw,
            )
    except Exception:
        timeline = None

    if tl_cache_path:
        try:
            tmp_path = '%s.tmp.%d' % (tl_cache_path, os.getpid())
            payload = json.dumps({'timeline': timeline}, separators=(',', ':'))
            if isinstance(payload, text_type):
                payload = payload.encode('utf-8')
            with open(tmp_path, 'wb') as f:
                f.write(payload)
            os.rename(tmp_path, tl_cache_path)
        except Exception:
            try:
                if os.path.isfile(tmp_path):
                    os.unlink(tmp_path)
            except Exception:
                pass

    meta_obj['timeline'] = timeline
    try:
        out_str = json.dumps(meta_obj, separators=(',', ':'))
        return out_str.encode('utf-8') if isinstance(out_str, text_type) else out_str
    except Exception:
        return raw_meta_bytes
