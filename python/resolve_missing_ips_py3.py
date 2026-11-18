#!/usr/bin/env python3
"""
Python 3 helper for backfill_missing_ips.py.
Loads synced enhanced.db.good + pepper_*.bin + log_ip_events.jsonl.gz across
pug, refreshx86, bomb, ent, west_coast_pug2, whoracle3, and resolves cleartext IPs
for constant-IP sessionGames records, computing exact SHA3-512 peppered hashes and SHA-1 hashes.
"""
import bisect
from collections import Counter, defaultdict
import glob
import gzip
import hashlib
import json
import os
import re
import struct
import sys

_MODERN_SQLITE_LIB = "/home/pyservices/lib"
if (
    os.path.exists(os.path.join(_MODERN_SQLITE_LIB, "libsqlite3.so.0"))
    and os.environ.get("_RESOLVE_SQLITE_REEXEC") != "1"
    and _MODERN_SQLITE_LIB not in os.environ.get("LD_LIBRARY_PATH", "").split(":")
):
    _env = dict(os.environ)
    _cur_ld = _env.get("LD_LIBRARY_PATH", "")
    _env["LD_LIBRARY_PATH"] = (
        (_MODERN_SQLITE_LIB + ":" + _cur_ld) if _cur_ld else _MODERN_SQLITE_LIB
    )
    _env["_RESOLVE_SQLITE_REEXEC"] = "1"
    os.execve(sys.executable, [sys.executable] + sys.argv, _env)

import sqlite3

BASE_DEMOS_DIR = "/cygdrive/U/demos"
IPV4_VALID_RE = re.compile(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$")
UNHELPFUL_NAMES = {
    "", "padawan", "pedowan", "pedowan*", "unnamedplayer", "unnamed",
    "spec", "specs", "spectator", "spectating", "speccing",
    "player", "play", "jedi", "jedi knight", "afk", "sith", "unknown", "elo bot",
}


def decode_jka_str(val):
    if val is None:
        return ""
    if isinstance(val, bytes):
        try:
            return val.decode("utf-8")
        except UnicodeDecodeError:
            return val.decode("cp1252", "replace")
    return str(val)


def strip_colors(s):
    if not s:
        return ""
    out = []
    i = 0
    n = len(s)
    while i < n:
        if s[i] == "^" and i + 1 < n and "0" <= s[i + 1] <= "9":
            i += 2
            continue
        if ord(s[i]) >= 0x20:
            out.append(s[i])
        i += 1
    return "".join(out).strip()


def norm_map(m):
    if not m:
        return ""
    m = m.strip().lower().replace("\\", "/")
    if m.startswith("mp/"):
        m = m[3:]
    elif m.startswith("maps/mp/"):
        m = m[8:]
    if m.endswith(".bsp"):
        m = m[:-4]
    return m


def valid_ip(ip_str):
    if not ip_str:
        return None
    m = IPV4_VALID_RE.match(ip_str.strip())
    if not m:
        return None
    nums = [int(m.group(i)) for i in range(1, 5)]
    if any(n < 0 or n > 255 for n in nums):
        return None
    if nums[0] in (0, 127):
        return None
    return "%d.%d.%d.%d" % tuple(nums)


def sha1_signed32(raw_bytes):
    digest = hashlib.sha1(raw_bytes).digest()
    return struct.unpack("!i", digest[:4])[0]


def hash_for_ip_sha1(ip_str):
    ip = valid_ip(ip_str)
    if not ip:
        return None
    nums = [int(p) for p in ip.split(".")]
    packed = struct.pack("BBBB", nums[3], nums[2], nums[1], nums[0])
    return sha1_signed32(packed)


def hash_for_ip_pepper(ip_str, pepper_bytes):
    ip = valid_ip(ip_str)
    if not ip or not pepper_bytes or len(pepper_bytes) != 32:
        return None
    nums = [int(p) for p in ip.split(".")]
    ip_uint32 = (
        ((nums[0] << 24) & 0xFF000000)
        | ((nums[1] << 16) & 0x00FF0000)
        | ((nums[2] << 8) & 0x0000FF00)
        | (nums[3] & 0x000000FF)
    )
    ip_le_bytes = struct.pack("<I", ip_uint32)
    digest = hashlib.sha3_512(ip_le_bytes + pepper_bytes).digest()
    return struct.unpack("<i", digest[:4])[0]


def buggy_constants_for_pepper(pepper_bytes):
    out = []
    for prefix in (struct.pack("<I", 1), struct.pack("<I", 0)):
        digest = hashlib.sha3_512(prefix + pepper_bytes).digest()
        out.append(struct.unpack("<i", digest[:4])[0])
    return out


class ServerGroupData:
    def __init__(self, group_name):
        self.group_name = group_name
        self.peppers = []
        self.sessions_by_key = {}  # (srv, sid) -> dict
        self.cuid_ips = defaultdict(Counter)  # cuid -> Counter(ip)
        self.guid_ips = defaultdict(Counter)  # guid -> Counter(ip)
        self.guid_accounts = defaultdict(set)  # guid -> set(account_name)
        self.pug_matches = defaultdict(list)  # match_hex -> list of player dicts
        self.ts_obs_cuid = defaultdict(list)  # cuid -> [(ts, ip)]
        self.ts_obs_guid = defaultdict(list)  # guid -> [(ts, ip)]
        self.log_by_matchid = defaultdict(list)  # match_hex -> [map_block]
        self.log_maps = []  # [(ts, idx, map_block)]
        self.log_ts_keys = []

    def finalize(self):
        for d in (self.ts_obs_cuid, self.ts_obs_guid):
            for k in d:
                d[k].sort(key=lambda x: x[0])
        self.log_maps.sort(key=lambda x: (x[0], x[1]))
        self.log_ts_keys = [x[0] for x in self.log_maps]

    def query_ts_window(self, obs_dict, key, target_ts, half_window):
        arr = obs_dict.get(key)
        if not arr:
            return set()
        lo = bisect.bisect_left(arr, (target_ts - half_window, ""))
        hi = bisect.bisect_right(arr, (target_ts + half_window, "\xff"))
        return {arr[i][1] for i in range(lo, hi)}

    def query_ts_bracket_same_ip(self, obs_dict, key, target_ts, max_gap=3888000):
        arr = obs_dict.get(key)
        if not arr:
            return None
        pos = bisect.bisect_left(arr, (target_ts, ""))
        if pos <= 0 or pos >= len(arr):
            return None
        prev_ts, prev_ip = arr[pos - 1]
        next_ts, next_ip = arr[pos]
        if prev_ip == next_ip and (target_ts - prev_ts) <= max_gap and (next_ts - target_ts) <= max_gap:
            return prev_ip
        return None


def load_server_into_group(srv, grp, all_ips_global):
    sync_dir = os.path.join(BASE_DEMOS_DIR, srv, ".enhanced_sync")
    for ppath in sorted(glob.glob(os.path.join(sync_dir, "pepper_*.bin"))):
        try:
            with open(ppath, "rb") as pf:
                pdata = pf.read(32)
            if len(pdata) == 32 and pdata not in grp.peppers:
                grp.peppers.append(pdata)
        except Exception:
            pass

    db_path = os.path.join(sync_dir, "enhanced.db.good")
    sid_to_session = {}
    if os.path.isfile(db_path):
        conn = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True)
        conn.text_factory = bytes
        tables = {
            decode_jka_str(r[0]).lower()
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

        accounts = {}
        if "accounts" in tables:
            acc_cols = {
                decode_jka_str(r[1]).lower()
                for r in conn.execute("PRAGMA table_info(accounts)")
            }
            if "properties" in acc_cols:
                rows = conn.execute("SELECT account_id, name, properties FROM accounts").fetchall()
            else:
                rows = [
                    (r[0], r[1], None)
                    for r in conn.execute("SELECT account_id, name FROM accounts").fetchall()
                ]
            for aid, raw_name, raw_props in rows:
                aname = decode_jka_str(raw_name).strip().lower()
                autolink_guids = []
                if raw_props:
                    try:
                        pjson = json.loads(decode_jka_str(raw_props))
                        ag = pjson.get("autolink_guid")
                        if ag and isinstance(ag, str) and ag.strip():
                            autolink_guids.append(sha1_signed32(ag.strip().encode("utf-8")))
                        asex = pjson.get("autolink_sex")
                        if asex and str(asex).strip().lstrip("-").isdigit():
                            autolink_guids.append(int(str(asex).strip()))
                    except Exception:
                        pass
                accounts[int(aid)] = {"name": aname, "autolink_guids": autolink_guids}
                for g in autolink_guids:
                    if aname:
                        grp.guid_accounts[g].add(aname)

        nick_by_sid = defaultdict(set)
        if "nicknames" in tables:
            for sid, raw_nick in conn.execute("SELECT session_id, name FROM nicknames"):
                cn = strip_colors(decode_jka_str(raw_nick)).lower()
                if cn and cn not in UNHELPFUL_NAMES:
                    nick_by_sid[int(sid)].add(cn)

        if "sessions" in tables:
            for sid, raw_info, aid in conn.execute("SELECT session_id, info, account_id FROM sessions"):
                try:
                    sjson = json.loads(decode_jka_str(raw_info))
                except Exception:
                    continue
                ip = valid_ip(sjson.get("ip"))
                if not ip:
                    continue
                all_ips_global.add(ip)
                cuid = (sjson.get("cuid_hash2") or "").strip().lower() or None
                aid_int = int(aid) if aid is not None and int(aid) in accounts else None
                ag_list = accounts[aid_int]["autolink_guids"] if aid_int is not None else []
                srec = {
                    "sid": int(sid),
                    "ip": ip,
                    "cuid": cuid,
                    "aid": aid_int,
                    "autolink_guids": ag_list,
                    "nicks": nick_by_sid.get(int(sid), set()),
                }
                sid_to_session[int(sid)] = srec
                grp.sessions_by_key[(srv, int(sid))] = srec
                if cuid:
                    grp.cuid_ips[cuid][ip] += 1
                for g in ag_list:
                    grp.guid_ips[g][ip] += 1

        # PUG match records (both linked AND unlinked sessions!)
        if "pugs" in tables and "playerpugteampos" in tables:
            pug_rows = conn.execute(
                "SELECT p.match_id, CAST(p.datetime AS INTEGER), p.map, p.duration, "
                "pp.session_id, pp.team, SUM(pp.duration), MAX(pp.name) "
                "FROM pugs p "
                "JOIN playerpugteampos pp ON pp.match_id = p.match_id "
                "GROUP BY p.match_id, pp.session_id, pp.team"
            ).fetchall()
            seen_match_sid_team = set()
            for mid, dt, raw_map, pdur, sid, team, dur, raw_pname in pug_rows:
                srec = sid_to_session.get(int(sid))
                if not srec:
                    continue
                mhash = "%016x" % (int(mid) & 0xFFFFFFFFFFFFFFFF)
                tstr = "RED" if int(team) == 1 else ("BLUE" if int(team) == 2 else "OTHER")
                seen_match_sid_team.add((mhash, int(sid), tstr))
                pname = decode_jka_str(raw_pname)
                cname = strip_colors(pname).lower()
                grp.pug_matches[mhash].append(
                    {
                        "srv": srv,
                        "sid": int(sid),
                        "ip": srec["ip"],
                        "cuid": srec["cuid"],
                        "autolink_guids": srec["autolink_guids"],
                        "nicks": srec["nicks"],
                        "team": tstr,
                        "dur": int(dur or 0),
                        "name": pname,
                        "clean_name": cname,
                    }
                )
                # pugs.datetime is match end; record both start_approx and end in ts_obs
                end_ts = int(dt or 0)
                start_ts = max(0, end_ts - int(pdur or 1200)) if end_ts > 0 else 0
                for obs_ts in (start_ts, end_ts):
                    if obs_ts > 0:
                        if srec["cuid"]:
                            grp.ts_obs_cuid[srec["cuid"]].append((obs_ts, srec["ip"]))
                        for g in srec["autolink_guids"]:
                            grp.ts_obs_guid[g].append((obs_ts, srec["ip"]))

            if "tedata" in tables:
                for mid, sid, team, ttime in conn.execute(
                    "SELECT match_id, session_id, team, SUM(time) FROM tedata GROUP BY match_id, session_id, team"
                ):
                    srec = sid_to_session.get(int(sid))
                    if not srec:
                        continue
                    mhash = "%016x" % (int(mid) & 0xFFFFFFFFFFFFFFFF)
                    tstr = "RED" if int(team) == 1 else ("BLUE" if int(team) == 2 else "OTHER")
                    if (mhash, int(sid), tstr) in seen_match_sid_team:
                        continue
                    grp.pug_matches[mhash].append(
                        {
                            "srv": srv,
                            "sid": int(sid),
                            "ip": srec["ip"],
                            "cuid": srec["cuid"],
                            "autolink_guids": srec["autolink_guids"],
                            "nicks": srec["nicks"],
                            "team": tstr,
                            "dur": int(ttime or 0),
                            "name": "",
                            "clean_name": "",
                        }
                    )

        if "fastcaps" in tables:
            for sid, fdate in conn.execute("SELECT session_id, CAST(date AS INTEGER) FROM fastcaps"):
                srec = sid_to_session.get(int(sid))
                if not srec:
                    continue
                fts = int(fdate or 0)
                if fts > 0:
                    if srec["cuid"]:
                        grp.ts_obs_cuid[srec["cuid"]].append((fts, srec["ip"]))
                    for g in srec["autolink_guids"]:
                        grp.ts_obs_guid[g].append((fts, srec["ip"]))

        conn.close()

    # Load log_ip_events.jsonl.gz if present
    log_path = os.path.join(sync_dir, "log_ip_events.jsonl.gz")
    if os.path.isfile(log_path):
        with gzip.open(log_path, "rt", encoding="utf-8") as gz:
            for line_idx, line in enumerate(gz):
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if obj.get("type") == "all_ips":
                    for raw_ip in obj.get("ips", []):
                        vip = valid_ip(raw_ip)
                        if vip:
                            all_ips_global.add(vip)
                    continue
                if obj.get("type") != "map":
                    continue
                mts = int(obj.get("ts") or 0)
                mhex = (obj.get("sv_matchid") or "").strip().lower()
                # Enrich any client rows that logged sid with enhanced.db cuid if missing
                for crec in obj.get("clients", []):
                    cip = valid_ip(crec.get("ip"))
                    if cip:
                        all_ips_global.add(cip)
                    csid = crec.get("sid")
                    if csid is not None and (srv, int(csid)) in grp.sessions_by_key:
                        srec = grp.sessions_by_key[(srv, int(csid))]
                        if not crec.get("cuid") and srec.get("cuid"):
                            crec["cuid"] = srec["cuid"]
                    ccuid = (crec.get("cuid") or "").strip().lower() or None
                    crec["cuid"] = ccuid
                    crec["clean_name"] = strip_colors(crec.get("name") or "").lower()
                    cguid = crec.get("guid")
                    if cip:
                        if ccuid:
                            grp.cuid_ips[ccuid][cip] += 1
                            if mts > 0:
                                grp.ts_obs_cuid[ccuid].append((mts, cip))
                        if cguid is not None and int(cguid) != 0:
                            grp.guid_ips[int(cguid)][cip] += 1
                            if mts > 0:
                                grp.ts_obs_guid[int(cguid)].append((mts, cip))
                if mhex:
                    grp.log_by_matchid[mhex].append(obj)
                if mts > 0:
                    grp.log_maps.append((mts, line_idx, obj))


def find_log_blocks(grp, match_hex, ts, mapname):
    if match_hex in grp.log_by_matchid:
        return grp.log_by_matchid[match_hex]
    if ts <= 0 or not grp.log_maps:
        return []
    nm = norm_map(mapname)
    tol = 150 if nm else 20
    lo = bisect.bisect_left(grp.log_ts_keys, ts - tol)
    hi = bisect.bisect_right(grp.log_ts_keys, ts + tol)
    candidates = []
    for i in range(lo, hi):
        bts, _idx, blk = grp.log_maps[i]
        if nm and norm_map(blk.get("map")) != nm:
            continue
        candidates.append((abs(bts - ts), blk))
    candidates.sort(key=lambda x: x[0])
    return [c[1] for c in candidates[:2]]


def resolve_from_log_blocks(item, blocks):
    if not blocks:
        return None, None
    cuid = item.get("newmod_id")
    guid = item.get("guid", 0)
    cids = set(item.get("clientids") or [])
    cnames = {
        strip_colors(n).lower()
        for n in (item.get("names") or [])
        if strip_colors(n).lower() not in UNHELPFUL_NAMES
    }

    for blk in blocks:
        clients = blk.get("clients", [])
        if cuid:
            ips = {valid_ip(c["ip"]) for c in clients if c.get("cuid") == cuid and valid_ip(c.get("ip"))}
            if len(ips) == 1:
                return next(iter(ips)), "tier1_log_cuid"
        if cids and guid != 0:
            ips = {
                valid_ip(c["ip"])
                for c in clients
                if c.get("c") in cids and c.get("guid") == guid and valid_ip(c.get("ip"))
            }
            if len(ips) == 1:
                return next(iter(ips)), "tier1_log_slot_guid"
        if guid != 0:
            ips = {valid_ip(c["ip"]) for c in clients if c.get("guid") == guid and valid_ip(c.get("ip"))}
            if len(ips) == 1:
                return next(iter(ips)), "tier1_log_guid"
        if cids and cnames:
            ips = {
                valid_ip(c["ip"])
                for c in clients
                if c.get("c") in cids and c.get("clean_name") in cnames and valid_ip(c.get("ip"))
            }
            if len(ips) == 1:
                return next(iter(ips)), "tier1_log_slot_name"
        if cids and len(cids) == 1:
            ips = {valid_ip(c["ip"]) for c in clients if c.get("c") in cids and valid_ip(c.get("ip"))}
            if len(ips) == 1:
                return next(iter(ips)), "tier1_log_slot_only"
    return None, None


def main():
    if len(sys.argv) < 3:
        sys.stderr.write("Usage: resolve_missing_ips_py3.py <input.json> <output.json>\n")
        sys.exit(1)

    with open(sys.argv[1], "r", encoding="utf-8") as f:
        req = json.load(f)

    items = req.get("items", [])
    match_known_players = req.get("match_known_players") or {}
    extra_guid_obs = req.get("extra_guid_obs") or []

    all_ips_global = set()
    groups = {
        "pug": ServerGroupData("pug"),
        "bomb": ServerGroupData("bomb"),
        "ent": ServerGroupData("ent"),
        "other": ServerGroupData("other"),
    }

    load_server_into_group("pug", groups["pug"], all_ips_global)
    load_server_into_group("refreshx86", groups["pug"], all_ips_global)
    load_server_into_group("bomb", groups["bomb"], all_ips_global)
    load_server_into_group("ent", groups["ent"], all_ips_global)
    load_server_into_group("west_coast_pug2", groups["other"], all_ips_global)
    load_server_into_group("whoracle3", groups["other"], all_ips_global)

    for obs in extra_guid_obs:
        gval = int(obs.get("guid") or 0)
        ots = int(obs.get("ts") or 0)
        oip = valid_ip(obs.get("ip"))
        if gval and ots > 0 and oip:
            for gname in ("pug", "bomb", "ent"):
                groups[gname].ts_obs_guid[gval].append((ots, oip))
                groups[gname].guid_ips[gval][oip] += 1

    for grp in groups.values():
        grp.finalize()

    # Global cuid -> Counter(ip) fallback ONLY when single-IP across the entire fleet
    global_cuid_ips = defaultdict(Counter)
    for grp in groups.values():
        for cuid, ctr in grp.cuid_ips.items():
            for ip, cnt in ctr.items():
                global_cuid_ips[cuid][ip] += cnt

    # Map buggy constant IP -> (group_name, exact_pepper_bytes)
    const_to_target = {}
    all_buggy_constants = set()
    for gname in ("pug", "bomb", "ent"):
        for pep in groups[gname].peppers:
            for cval in buggy_constants_for_pepper(pep):
                all_buggy_constants.add(cval)
                if cval not in const_to_target:
                    const_to_target[cval] = (gname, pep)

    # Group input items by (group_name, match_hex) so match-level elimination works
    by_group_match = defaultdict(list)
    for it in items:
        cip = int(it["const_ip"])
        target = const_to_target.get(cip)
        if not target:
            continue
        gname, _pep = target
        by_group_match[(gname, it["match"])].append(it)

    resolved = {}  # str(idx) -> {"ip": ip, "pepper_hash": h, "sha1_hash": s, "tier": tier, "group": gname}

    for (gname, match_hex), mitems in by_group_match.items():
        grp = groups[gname]
        sample_ts = mitems[0].get("ts", 0)
        sample_map = mitems[0].get("mapname", "")
        blocks = find_log_blocks(grp, match_hex, sample_ts, sample_map)

        # Tier 1: server_internal.log exact map block
        for it in mitems:
            ip, tier = resolve_from_log_blocks(it, blocks)
            if ip:
                resolved[it["idx"]] = (ip, tier, gname)

        # Tier 2: enhanced.db pugs / playerpugteampos / tedata exact match_id
        prows = grp.pug_matches.get(match_hex, [])
        if prows:
            def _rkey(r):
                return (r["srv"], r["sid"], r["team"])

            def _name_matches_direct(r_clean, helpful_set, all_set):
                if not r_clean:
                    return False
                if helpful_set:
                    if r_clean in helpful_set:
                        return True
                    # playerpugteampos.name truncates at 31 raw bytes (vs 35 in demo userinfo)
                    if len(r_clean) >= 10:
                        for nm in helpful_set:
                            if len(nm) >= 10 and (nm.startswith(r_clean) or r_clean.startswith(nm)):
                                return True
                    return False
                return r_clean in all_set

            claimed_rkeys = set()
            # Pre-mark rows matched by already-resolved teammates in MongoDB or Tier 1
            for kp in match_known_players.get(match_hex, []):
                kip = kp.get("ip")
                kcuid = kp.get("cuid")
                kteam = kp.get("team")
                for r in prows:
                    if kteam and r["team"] != kteam:
                        continue
                    if (kcuid and r["cuid"] == kcuid) or (kip and r["ip"] == kip):
                        claimed_rkeys.add(_rkey(r))

            for it in mitems:
                if it["idx"] in resolved:
                    rip = resolved[it["idx"]][0]
                    rcuid = it.get("newmod_id")
                    rteam = it.get("primary_team")
                    for r in prows:
                        if rteam and r["team"] != rteam:
                            continue
                        if (rcuid and r["cuid"] == rcuid) or r["ip"] == rip:
                            claimed_rkeys.add(_rkey(r))

            # Always resolve active RED/BLUE match players BEFORE spectators!
            active_mitems = [it for it in mitems if it.get("is_match")]
            spec_mitems = [it for it in mitems if not it.get("is_match")]

            # Pass 2a: exact cuid match in prows
            for it in active_mitems + spec_mitems:
                if it["idx"] in resolved:
                    continue
                cuid = it.get("newmod_id")
                if cuid:
                    cand = [r for r in prows if r["cuid"] == cuid]
                    ips = {r["ip"] for r in cand}
                    if len(ips) == 1:
                        resolved[it["idx"]] = (next(iter(ips)), "tier2_pug_cuid", gname)
                        if it.get("is_match"):
                            for r in cand:
                                claimed_rkeys.add(_rkey(r))

            # Pass 2b: autolink_guid match in prows
            for it in active_mitems + spec_mitems:
                if it["idx"] in resolved:
                    continue
                guid = it.get("guid", 0)
                if guid != 0:
                    cand = [r for r in prows if guid in r["autolink_guids"]]
                    ips = {r["ip"] for r in cand}
                    if len(ips) == 1:
                        resolved[it["idx"]] = (next(iter(ips)), "tier2_pug_autolink_guid", gname)
                        if it.get("is_match"):
                            for r in cand:
                                claimed_rkeys.add(_rkey(r))

            # Pass 2c (active RED/BLUE players only): direct match name first, then historical session nicks
            for use_hist_nicks in (False, True):
                for it in active_mitems:
                    if it["idx"] in resolved:
                        continue
                    pteam = it.get("primary_team")
                    inames = {
                        strip_colors(n).lower()
                        for n in (it.get("names") or [])
                        if strip_colors(n).lower()
                    }
                    helpful_inames = {n for n in inames if n not in UNHELPFUL_NAMES}
                    if use_hist_nicks and not helpful_inames:
                        continue
                    cand = [
                        r
                        for r in prows
                        if _rkey(r) not in claimed_rkeys
                        and (not pteam or r["team"] == pteam)
                        and (
                            _name_matches_direct(r["clean_name"], helpful_inames, inames)
                            if not use_hist_nicks
                            else bool(r["nicks"] & helpful_inames)
                        )
                    ]
                    ips = {r["ip"] for r in cand}
                    if len(ips) == 1:
                        resolved[it["idx"]] = (next(iter(ips)), "tier2_pug_team_name", gname)
                        for r in cand:
                            claimed_rkeys.add(_rkey(r))

            # Pass 2d: 3-of-4 Team Elimination on RED and BLUE
            for team_label in ("RED", "BLUE"):
                rem_items = [
                    it
                    for it in active_mitems
                    if it["idx"] not in resolved
                    and it.get("primary_team") == team_label
                    and it.get("team_ms", 0) >= 60000
                ]
                rem_rows = [
                    r
                    for r in prows
                    if _rkey(r) not in claimed_rkeys
                    and r["team"] == team_label
                    and r["dur"] >= 60
                ]
                rem_ips = {r["ip"] for r in rem_rows}
                if len(rem_items) == 1 and len(rem_ips) == 1:
                    resolved[rem_items[0]["idx"]] = (
                        next(iter(rem_ips)),
                        "tier2_pug_team_elim",
                        gname,
                    )
                    for r in rem_rows:
                        claimed_rkeys.add(_rkey(r))

            # Pass 2e: allow spectators on the same IP/alias to match without stealing claimed_rkeys
            for it in spec_mitems:
                if it["idx"] in resolved:
                    continue
                inames = {
                    strip_colors(n).lower()
                    for n in (it.get("names") or [])
                    if strip_colors(n).lower()
                }
                helpful_inames = {n for n in inames if n not in UNHELPFUL_NAMES}
                if not helpful_inames:
                    continue
                cand = [
                    r
                    for r in prows
                    if _name_matches_direct(r["clean_name"], helpful_inames, inames)
                    or bool(r["nicks"] & helpful_inames)
                ]
                ips = {r["ip"] for r in cand}
                if len(ips) == 1:
                    resolved[it["idx"]] = (next(iter(ips)), "tier2_pug_team_name", gname)

    # Feed newly Tier-1/Tier-2 resolved (ts, ip) into ts_obs_cuid / ts_obs_guid for Tier 3!
    newly_added_obs = False
    for it in items:
        idx = it["idx"]
        if idx not in resolved:
            continue
        rip, _rtier, gname = resolved[idx]
        its = int(it.get("ts") or 0)
        if its <= 0:
            continue
        grp = groups[gname]
        icuid = it.get("newmod_id")
        iguid = int(it.get("guid") or 0)
        if icuid:
            grp.ts_obs_cuid[icuid].append((its, rip))
            grp.cuid_ips[icuid][rip] += 1
            newly_added_obs = True
        if iguid not in (0, 1150862):
            grp.ts_obs_guid[iguid].append((its, rip))
            grp.guid_ips[iguid][rip] += 1
            newly_added_obs = True
    if newly_added_obs:
        for grp in groups.values():
            grp.finalize()

    # Tier 3 (±6h same-session window) & Tier 4 (unambiguous single-IP identity)
    for it in items:
        idx = it["idx"]
        if idx in resolved:
            continue
        cip = int(it["const_ip"])
        target = const_to_target.get(cip)
        if not target:
            continue
        gname, _pep = target
        grp = groups[gname]
        ts = int(it.get("ts") or 0)
        cuid = it.get("newmod_id")
        guid = int(it.get("guid") or 0)

        if ts > 0 and cuid:
            w_ips = grp.query_ts_window(grp.ts_obs_cuid, cuid, ts, 21600)
            if len(w_ips) == 1:
                resolved[idx] = (next(iter(w_ips)), "tier3_window6h_cuid", gname)
                continue

        if ts > 0 and guid != 0 and guid != 1150862:
            w_ips = grp.query_ts_window(grp.ts_obs_guid, guid, ts, 21600)
            if len(w_ips) == 1:
                resolved[idx] = (next(iter(w_ips)), "tier3_window6h_guid", gname)
                continue

        if ts > 0 and cuid:
            bip = grp.query_ts_bracket_same_ip(grp.ts_obs_cuid, cuid, ts)
            if bip:
                resolved[idx] = (bip, "tier3b_bracket_same_ip_cuid", gname)
                continue

        if ts > 0 and guid != 0 and guid != 1150862 and len(grp.guid_accounts.get(guid, set())) <= 1:
            bip = grp.query_ts_bracket_same_ip(grp.ts_obs_guid, guid, ts)
            if bip:
                resolved[idx] = (bip, "tier3b_bracket_same_ip_guid", gname)
                continue

        if cuid:
            s_ips = grp.cuid_ips.get(cuid)
            if s_ips and len(s_ips) == 1:
                resolved[idx] = (next(iter(s_ips.keys())), "tier4_single_ip_cuid", gname)
                continue
            g_ips = global_cuid_ips.get(cuid)
            if g_ips and len(g_ips) == 1:
                resolved[idx] = (next(iter(g_ips.keys())), "tier4_global_single_ip_cuid", gname)
                continue

        if gname == "pug" and guid == 1150862:
            resolved[idx] = ("135.180.106.215", "tier4_elobot", gname)
            continue

        if guid != 0 and len(grp.guid_accounts.get(guid, set())) <= 1:
            g_ips = grp.guid_ips.get(guid)
            if g_ips and len(g_ips) == 1:
                resolved[idx] = (next(iter(g_ips.keys())), "tier4_single_ip_guid", gname)
                continue

    # Build final output mapping + hash cache entries
    out_resolved = {}
    ip_hash_entries = {}  # str(hash_int32) -> {"ip": ip, "source": src}

    # Populate ipHashCache for ALL historical & current cleartext IPs across logs and DBs
    all_peppers = []
    for gname in ("pug", "bomb", "ent"):
        for pep in groups[gname].peppers:
            if pep not in all_peppers:
                all_peppers.append(pep)

    for ip in sorted(all_ips_global):
        sh1 = hash_for_ip_sha1(ip)
        if sh1 is not None and sh1 not in all_buggy_constants:
            ip_hash_entries[str(sh1)] = ip
        for pep in all_peppers:
            ph = hash_for_ip_pepper(ip, pep)
            if ph is not None and ph not in all_buggy_constants:
                ip_hash_entries[str(ph)] = ip

    for it in items:
        idx = it["idx"]
        if idx not in resolved:
            continue
        ip, tier, gname = resolved[idx]
        _gn, pep = const_to_target[int(it["const_ip"])]
        pep_hash = hash_for_ip_pepper(ip, pep)
        sh1_hash = hash_for_ip_sha1(ip)
        if pep_hash is None or pep_hash in all_buggy_constants:
            continue
        ip_hash_entries[str(pep_hash)] = ip
        if sh1_hash is not None and sh1_hash not in all_buggy_constants:
            ip_hash_entries[str(sh1_hash)] = ip
        out_resolved[str(idx)] = {
            "ip": ip,
            "pepper_hash": pep_hash,
            "sha1_hash": sh1_hash,
            "tier": tier,
            "group": gname,
        }

    out_payload = {
        "target_constants": {str(k): v[0] for k, v in const_to_target.items()},
        "resolved": out_resolved,
        "ip_hash_entries": ip_hash_entries,
        "all_ips_count": len(all_ips_global),
    }
    with open(sys.argv[2], "w", encoding="utf-8") as f:
        json.dump(out_payload, f)


if __name__ == "__main__":
    main()
