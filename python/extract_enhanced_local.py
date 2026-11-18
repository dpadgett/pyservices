#!/usr/bin/env python3
"""
Local standardized SQLite + SHA3-512/SHA1 reader for synced enhanced.db files.
Runs inside demos-web-pyservices-1 (/usr/bin/python3 3.6+) and outputs JSON
consumed by enhanced_profile_lib.py (Python 2.7).
"""
import glob
import hashlib
import json
import os
import re
import shutil
import struct
import sys

_MODERN_SQLITE_LIB = "/home/pyservices/lib"
if (
    os.path.exists(os.path.join(_MODERN_SQLITE_LIB, "libsqlite3.so.0"))
    and os.environ.get("_ENHANCED_SQLITE_REEXEC") != "1"
    and _MODERN_SQLITE_LIB not in os.environ.get("LD_LIBRARY_PATH", "").split(":")
):
    _env = dict(os.environ)
    _cur_ld = _env.get("LD_LIBRARY_PATH", "")
    _env["LD_LIBRARY_PATH"] = (
        (_MODERN_SQLITE_LIB + ":" + _cur_ld) if _cur_ld else _MODERN_SQLITE_LIB
    )
    _env["_ENHANCED_SQLITE_REEXEC"] = "1"
    os.execve(sys.executable, [sys.executable] + sys.argv, _env)

import sqlite3

BASE_DEMOS_DIR = "/cygdrive/U/demos"
SERVER_DIRS = [
    "bomb",
    "ent",
    "pug",
    "refreshx86",
    "west_coast_pug2",
    "whoracle3",
]

IP_LIKE_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){2,3}")


def decode_jka_str(val):
    if val is None:
        return ""
    if isinstance(val, bytes):
        try:
            return val.decode("utf-8")
        except UnicodeDecodeError:
            return val.decode("cp1252", "replace")
    return str(val)


def sha1_signed32(raw_bytes):
    digest = hashlib.sha1(raw_bytes).digest()
    return struct.unpack("!i", digest[:4])[0]


def hash_for_ip_sha1(ip_str):
    parts = ip_str.strip().split(".")
    if len(parts) != 4:
        return None
    try:
        nums = [int(p) for p in parts]
        if any(n < 0 or n > 255 for n in nums):
            return None
    except ValueError:
        return None
    packed = struct.pack("BBBB", nums[3], nums[2], nums[1], nums[0])
    return sha1_signed32(packed)


def hash_for_ip_peppers(ip_str, peppers):
    parts = ip_str.strip().split(".")
    if len(parts) != 4 or not peppers:
        return []
    try:
        nums = [int(p) for p in parts]
        if any(n < 0 or n > 255 for n in nums):
            return []
    except ValueError:
        return []
    ip_uint32 = ((nums[0] << 24) & 0xFF000000) | ((nums[1] << 16) & 0x00FF0000) | ((nums[2] << 8) & 0x0000FF00) | (nums[3] & 0x000000FF)
    ip_le_bytes = struct.pack("<I", ip_uint32)
    out = []
    for pep in peppers:
        digest = hashlib.sha3_512(ip_le_bytes + pep).digest()
        val = struct.unpack("<i", digest[:4])[0]
        if val not in out:
            out.append(val)
    return out


def buggy_pepper_constants(peppers):
    """
    In base_enhanced g_client.c:2559:
      uint32_t ip = getIpFromString(client->sess.ipString, &ip);
    getIpFromString returns qtrue (1), overwriting ip with 1 before sha3_Update!
    We compute those exact server-constant IP hashes so we never treat them as unique client IPs.
    """
    out = []
    one_le = struct.pack("<I", 1)
    zero_le = struct.pack("<I", 0)
    for pep in peppers:
        for prefix in (one_le, zero_le):
            digest = hashlib.sha3_512(prefix + pep).digest()
            val = struct.unpack("<i", digest[:4])[0]
            if val not in out:
                out.append(val)
    return out


def promote_if_valid(sync_dir):
    staging = os.path.join(sync_dir, "enhanced.db.staging")
    staging_wal = os.path.join(sync_dir, "enhanced.db.staging-wal")
    good = os.path.join(sync_dir, "enhanced.db.good")
    good_wal = os.path.join(sync_dir, "enhanced.db.good-wal")
    meta_file = os.path.join(sync_dir, "enhanced.db.good.meta.json")

    if not os.path.isfile(staging):
        if os.path.isfile(good):
            return good, True, "using_existing_good"
        return None, False, "missing"

    st = os.stat(staging)
    wal_mtime = int(os.stat(staging_wal).st_mtime) if os.path.isfile(staging_wal) else 0
    wal_size = int(os.stat(staging_wal).st_size) if os.path.isfile(staging_wal) else 0
    sig = {"mtime": int(st.st_mtime), "size": int(st.st_size), "wal_mtime": wal_mtime, "wal_size": wal_size}

    if os.path.isfile(good) and os.path.isfile(meta_file):
        try:
            with open(meta_file, "r") as f:
                old_sig = json.load(f)
            if old_sig == sig:
                return good, True, "unchanged_good"
        except Exception:
            pass

    # Validate staging DB with PRAGMA quick_check(1)
    try:
        conn = sqlite3.connect("file:%s?mode=ro" % staging, uri=True)
        res = conn.execute("PRAGMA quick_check(1)").fetchall()
        conn.close()
        ok = (res == [("ok",)])
    except Exception as exc:
        ok = False
        err = str(exc)
    else:
        err = "quick_check_failed"

    if ok:
        tmp_good = good + ".tmp"
        shutil.copy2(staging, tmp_good)
        os.chmod(tmp_good, 0o600)
        if os.path.isfile(staging_wal):
            shutil.copy2(staging_wal, good_wal)
            os.chmod(good_wal, 0o600)
        elif os.path.isfile(good_wal):
            os.remove(good_wal)
        os.rename(tmp_good, good)
        with open(meta_file, "w") as f:
            json.dump(sig, f)
        return good, True, "promoted_ok"

    if os.path.isfile(good):
        return good, False, "staging_invalid_fallback_good:%s" % err
    return None, False, "staging_invalid:%s" % err


def extract_server(server):
    sync_dir = os.path.join(BASE_DEMOS_DIR, server, ".enhanced_sync")
    last_sync_ts = None
    ts_path = os.path.join(sync_dir, "last_sync_ts.txt")
    if os.path.isfile(ts_path):
        try:
            with open(ts_path, "r") as f:
                last_sync_ts = int(f.read().strip())
        except Exception:
            pass

    db_path, check_ok, status_msg = promote_if_valid(sync_dir)
    info = {
        "server": server,
        "has_db": bool(db_path),
        "quick_check_ok": check_ok,
        "status": status_msg,
        "last_sync_ts": last_sync_ts,
        "db_mtime": int(os.stat(db_path).st_mtime) if db_path and os.path.isfile(db_path) else None,
        "db_size": int(os.stat(db_path).st_size) if db_path and os.path.isfile(db_path) else 0,
        "accounts_count": 0,
        "linked_sessions_count": 0,
        "pugs_count": 0,
        "fastcaps_count": 0,
        "topaims_count": 0,
    }
    if not db_path:
        return info, None

    peppers = []
    for ppath in sorted(glob.glob(os.path.join(sync_dir, "pepper_*.bin"))):
        try:
            with open(ppath, "rb") as pf:
                pdata = pf.read(32)
                if len(pdata) == 32 and pdata not in peppers:
                    peppers.append(pdata)
        except Exception:
            pass

    const_ips = buggy_pepper_constants(peppers)

    conn = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True)
    conn.text_factory = bytes
    tables = {decode_jka_str(r[0]).lower() for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    acc_cols = {decode_jka_str(r[1]).lower() for r in conn.execute("PRAGMA table_info(accounts)")}
    has_props = "properties" in acc_cols

    accounts = {}
    if has_props:
        rows = conn.execute("SELECT account_id, name, properties FROM accounts").fetchall()
    else:
        rows = [(r[0], r[1], None) for r in conn.execute("SELECT account_id, name FROM accounts").fetchall()]

    for aid, raw_name, raw_props in rows:
        aname = decode_jka_str(raw_name).strip().lower()
        if not aname:
            continue
        autolink_guids = []
        autolink_details = []
        if raw_props:
            try:
                pjson = json.loads(decode_jka_str(raw_props))
                ag = pjson.get("autolink_guid")
                if ag and isinstance(ag, str) and ag.strip():
                    gval = sha1_signed32(ag.strip().encode("utf-8"))
                    autolink_guids.append(gval)
                    autolink_details.append({"guid": gval, "prop": "autolink_guid"})
                asex = pjson.get("autolink_sex")
                if asex and str(asex).strip().lstrip("-").isdigit():
                    gval = int(str(asex).strip())
                    autolink_guids.append(gval)
                    autolink_details.append({"guid": gval, "prop": "autolink_sex"})
            except Exception:
                pass
        accounts[int(aid)] = {
            "account_id": int(aid),
            "name": aname,
            "autolink_guids": autolink_guids,
            "autolink_details": autolink_details,
        }

    info["accounts_count"] = len(accounts)

    # Nicknames per linked session
    nick_by_sid = {}
    if "nicknames" in tables:
        for sid, raw_nick, dur in conn.execute(
            "SELECT n.session_id, n.name, n.duration FROM nicknames n "
            "JOIN sessions s ON s.session_id = n.session_id WHERE s.account_id IS NOT NULL"
        ):
            nstr = decode_jka_str(raw_nick)
            if not nstr or IP_LIKE_RE.match(nstr.strip()):
                continue
            lst = nick_by_sid.setdefault(int(sid), [])
            lst.append((int(dur or 0), nstr))
        for sid in list(nick_by_sid.keys()):
            nick_by_sid[sid] = [
                {"name": n, "duration": d}
                for d, n in sorted(nick_by_sid[sid], key=lambda x: x[0], reverse=True)[:8]
            ]

    sessions = []
    cleartext_ips = {}  # str(hash_int32) -> ip_str (across both linked and unlinked sessions!)
    for sid, raw_info, aid in conn.execute("SELECT session_id, info, account_id FROM sessions"):
        try:
            sjson = json.loads(decode_jka_str(raw_info))
        except Exception:
            continue
        ip_str = sjson.get("ip")
        sha1_ip = hash_for_ip_sha1(ip_str) if ip_str else None
        pep_ips = [h for h in hash_for_ip_peppers(ip_str, peppers) if h not in const_ips] if ip_str else []
        if ip_str and sha1_ip is not None:
            cleartext_ips[str(sha1_ip)] = ip_str
            for ph in pep_ips:
                cleartext_ips[str(ph)] = ip_str
        if aid is None:
            continue
        aid = int(aid)
        if aid not in accounts:
            continue
        cuid = sjson.get("cuid_hash2")
        sessions.append({
            "session_id": int(sid),
            "account_id": aid,
            "account_name": accounts[aid]["name"],
            "cuid_hash2": cuid if cuid else None,
            "ip_str": ip_str,
            "sha1_ip": sha1_ip,
            "pepper_ips": pep_ips,
            "nicknames": nick_by_sid.get(int(sid), []),
        })

    info["linked_sessions_count"] = len(sessions)

    # PUG match records (exact 64-bit match_id == minmatches._id!)
    pug_entries = []
    if "pugs" in tables and "playerpugteampos" in tables:
        pug_rows = conn.execute(
            "SELECT p.match_id, CAST(p.datetime AS INTEGER), p.map, p.red_score, p.blue_score, "
            "pp.session_id, s.account_id, pp.team, SUM(pp.duration), MAX(pp.name), "
            "SUM(COALESCE(pp.cap, 0)), SUM(COALESCE(pp.ass, 0)), SUM(COALESCE(pp.def, 0)), "
            "SUM(COALESCE(pp.dmg, 0)), SUM(COALESCE(pp.fckil, 0)), SUM(COALESCE(pp.ret, 0)), SUM(COALESCE(pp.ttlhold, 0)) "
            "FROM pugs p "
            "JOIN playerpugteampos pp ON pp.match_id = p.match_id "
            "JOIN sessions s ON s.session_id = pp.session_id "
            "WHERE s.account_id IS NOT NULL "
            "GROUP BY p.match_id, pp.session_id, pp.team"
        ).fetchall()
        by_match = {}
        for (mid, dt, raw_map, rs, bs, sid, aid, team, dur, raw_pname, cap, ass, dfn, dmg, fck, ret, hold) in pug_rows:
            aid = int(aid)
            if aid not in accounts:
                continue
            mhash = "%016x" % (int(mid) & 0xFFFFFFFFFFFFFFFF)
            mobj = by_match.setdefault(mhash, {
                "match_hash": mhash,
                "datetime": int(dt or 0),
                "map": decode_jka_str(raw_map).lower(),
                "red_score": int(rs or 0),
                "blue_score": int(bs or 0),
                "players": [],
            })
            mobj["players"].append({
                "account_id": aid,
                "account_name": accounts[aid]["name"],
                "team": "RED" if int(team) == 1 else ("BLUE" if int(team) == 2 else "OTHER"),
                "duration": int(dur or 0),
                "name": decode_jka_str(raw_pname),
                "captures": int(cap or 0),
                "assists": int(ass or 0),
                "defends": int(dfn or 0),
                "dmg": int(dmg or 0),
                "fckil": int(fck or 0),
                "rets": int(ret or 0),
                "ttlhold": int(hold or 0),
            })
        pug_entries = list(by_match.values())
        info["pugs_count"] = len(pug_entries)

    # Exact (match_id, client_id, time) records from fastcaps & topaims
    direct_client_matches = []
    if "fastcaps" in tables:
        fc_cnt = 0
        for aid, raw_extra in conn.execute(
            "SELECT s.account_id, f.extra FROM fastcaps f "
            "JOIN sessions s ON s.session_id = f.session_id "
            "WHERE s.account_id IS NOT NULL AND f.extra IS NOT NULL"
        ):
            aid = int(aid)
            if aid not in accounts:
                continue
            try:
                ex = json.loads(decode_jka_str(raw_extra))
                mhash = ex.get("match_id")
                cid = ex.get("client_id")
                t_ms = ex.get("pickup_time")
                if mhash and cid is not None:
                    direct_client_matches.append({
                        "kind": "fastcap",
                        "match_hash": str(mhash).lower(),
                        "client_id": int(cid),
                        "time_ms": int(t_ms) if t_ms is not None else None,
                        "player_name": ex.get("player_name", ""),
                        "account_id": aid,
                        "account_name": accounts[aid]["name"],
                    })
                    fc_cnt += 1
            except Exception:
                pass
        info["fastcaps_count"] = fc_cnt

    if "topaims" in tables:
        ta_cnt = 0
        for aid, raw_extra in conn.execute(
            "SELECT s.account_id, t.extra FROM topaims t "
            "JOIN sessions s ON s.session_id = t.session_id "
            "WHERE s.account_id IS NOT NULL AND t.extra IS NOT NULL"
        ):
            aid = int(aid)
            if aid not in accounts:
                continue
            try:
                ex = json.loads(decode_jka_str(raw_extra))
                mhash = ex.get("match_id")
                cid = ex.get("client_id")
                t_ms = ex.get("start_time")
                if mhash and cid is not None:
                    direct_client_matches.append({
                        "kind": "topaim",
                        "match_hash": str(mhash).lower(),
                        "client_id": int(cid),
                        "time_ms": int(t_ms) if t_ms is not None else None,
                        "player_name": ex.get("player_name", ""),
                        "account_id": aid,
                        "account_name": accounts[aid]["name"],
                    })
                    ta_cnt += 1
            except Exception:
                pass
        info["topaims_count"] = ta_cnt

    conn.close()
    payload = {
        "server": server,
        "buggy_pepper_constants": const_ips,
        "accounts": list(accounts.values()),
        "sessions": sessions,
        "cleartext_ips": cleartext_ips,
        "pugs": pug_entries,
        "direct_client_matches": direct_client_matches,
    }
    return info, payload


def main():
    only_status = ("--status-only" in sys.argv)
    sync_statuses = []
    servers_data = []
    for srv in SERVER_DIRS:
        st, payload = extract_server(srv)
        sync_statuses.append(st)
        if payload and not only_status:
            servers_data.append(payload)
    json.dump({"sync_statuses": sync_statuses, "servers": servers_data}, sys.stdout)


if __name__ == "__main__":
    main()
