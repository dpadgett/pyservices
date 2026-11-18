#!/usr/bin/env python3
"""
Standalone DNS BGP/ASN + PTR + Parallel Unprivileged UDP Traceroute (IP_RECVERR)
enrichment engine for demos-web-pyservices-1 (Python 3.6+, zero external deps).
"""
from concurrent.futures import ThreadPoolExecutor
import json
import random
import re
import select
import socket
import struct
import sys
import time

SOL_IP = 0
IP_TTL = 2
IP_RECVERR = 11
MSG_ERRQUEUE = 0x2000

US_TRANSIT_SUBSTRS = (
    ".sonic.net",
    "sjo-", "palo-", "den-", "nyk-", "chi-", "ewr-", "kanc-", "ash-", "lax-", "dal-", "mia-", "sea-",
    ".sfo0", ".slc0", ".ord0", ".cle0", ".jfk0", ".iad0",
)


def get_nameservers():
    ns = []
    try:
        with open("/etc/resolv.conf", "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("nameserver"):
                    parts = line.split()
                    if len(parts) >= 2 and ":" not in parts[1]:
                        ns.append(parts[1])
    except Exception:
        pass
    if "8.8.8.8" not in ns:
        ns.append("8.8.8.8")
    if "1.1.1.1" not in ns:
        ns.append("1.1.1.1")
    return ns


NAMESERVERS = get_nameservers()


def _encode_qname(domain):
    out = b""
    for lbl in domain.strip(".").split("."):
        b = lbl.encode("ascii", "ignore")
        out += struct.pack("B", len(b)) + b
    return out + b"\x00"


def _skip_dns_name(buf, offset):
    n = len(buf)
    while offset < n:
        length = buf[offset]
        if length == 0:
            return offset + 1
        if (length & 0xC0) == 0xC0:
            return offset + 2
        offset += 1 + length
    return offset


def query_dns_txt(domain, timeout=1.3):
    txid = random.randint(1, 65534)
    pkt = struct.pack("!HHHHHH", txid, 0x0100, 1, 0, 0, 0) + _encode_qname(domain) + struct.pack("!HH", 16, 1)
    for ns in NAMESERVERS[:2]:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(timeout)
        try:
            s.sendto(pkt, (ns, 53))
            resp, _ = s.recvfrom(2048)
            if len(resp) < 12:
                continue
            rxid, flags, qdcount, ancount, _, _ = struct.unpack("!HHHHHH", resp[:12])
            if rxid != txid:
                continue
            rcode = flags & 0x000F
            if rcode == 3:  # NXDOMAIN
                return []
            if ancount == 0:
                continue
            off = 12
            for _ in range(qdcount):
                off = _skip_dns_name(resp, off) + 4
            txts = []
            for _ in range(ancount):
                if off >= len(resp):
                    break
                off = _skip_dns_name(resp, off)
                if off + 10 > len(resp):
                    break
                rtype, rclass, ttl, rdlen = struct.unpack("!HHIH", resp[off:off + 10])
                off += 10
                rdata = resp[off:off + rdlen]
                off += rdlen
                if rtype == 16 and rdlen > 0:
                    pos = 0
                    chunks = []
                    while pos < len(rdata):
                        clen = rdata[pos]
                        pos += 1
                        chunks.append(rdata[pos:pos + clen].decode("utf-8", "replace"))
                        pos += clen
                    txts.append("".join(chunks))
            return txts
        except Exception:
            continue
        finally:
            s.close()
    return []


def lookup_ptr(ip_str):
    try:
        host, _, _ = socket.gethostbyaddr(ip_str)
        return host.rstrip(".")
    except Exception:
        return ""


def extract_ptr_domain(ptr):
    if not ptr:
        return ""
    parts = ptr.lower().split(".")
    if len(parts) >= 3 and len(parts[-1]) == 2 and parts[-2] in ("net", "co", "com", "org", "ne", "or"):
        return ".".join(parts[-3:])
    if len(parts) >= 3 and parts[-2] in ("otenet", "hol", "forthnet", "ertelecom", "bell", "chello"):
        return ".".join(parts[-3:])
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return ptr.lower()


def lookup_bgp_origin(ip_str):
    parts = ip_str.strip().split(".")
    if len(parts) != 4:
        return {}
    q = "%s.%s.%s.%s.origin.asn.cymru.com" % (parts[3], parts[2], parts[1], parts[0])
    txts = query_dns_txt(q)
    if not txts:
        return {}
    # Pick most specific prefix if multiple returned
    best = None
    best_plen = -1
    for t in txts:
        cols = [c.strip() for c in t.split("|")]
        if len(cols) >= 3:
            asn_raw = cols[0].split()[0] if cols[0] else ""
            pfx = cols[1]
            plen = int(pfx.split("/")[1]) if "/" in pfx and pfx.split("/")[1].isdigit() else 0
            if asn_raw.isdigit() and plen > best_plen:
                best_plen = plen
                best = {
                    "asn": int(asn_raw),
                    "bgp_prefix": pfx,
                    "country": cols[2] if len(cols) > 2 else "",
                    "rir": cols[3] if len(cols) > 3 else "",
                    "alloc_date": cols[4] if len(cols) > 4 else "",
                }
    return best or {}


def lookup_asn_name(asn):
    txts = query_dns_txt("AS%d.asn.cymru.com" % int(asn))
    if not txts:
        return ""
    cols = [c.strip() for c in txts[0].split("|")]
    if len(cols) >= 5:
        raw_desc = cols[4]
        if raw_desc.endswith(", " + cols[1]):
            raw_desc = raw_desc[:-len(", " + cols[1])].strip()
        return raw_desc[:48]
    return ""


def parallel_udp_traceroutes(target_ips, min_ttl=12, max_ttl=26, timeout=1.45, batch_targets=10):
    """
    Multiplexed unprivileged UDP traceroute using Linux IP_RECVERR across multiple target IPs.
    Returns {ip_str: [(ttl, hop_ip), ...]}.
    """
    results = {ip: [] for ip in target_ips}
    for bstart in range(0, len(target_ips), batch_targets):
        batch = target_ips[bstart:bstart + batch_targets]
        socks = {}  # sock -> (target_ip, ttl)
        for t_idx, dest_ip in enumerate(batch):
            for ttl in range(min_ttl, max_ttl + 1):
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.setsockopt(SOL_IP, IP_RECVERR, 1)
                    s.setsockopt(SOL_IP, IP_TTL, ttl)
                    s.setblocking(False)
                    port = 33434 + (t_idx * 32) + (ttl - min_ttl)
                    s.connect((dest_ip, port))
                    s.send(b"\x00" * 24)
                    socks[s] = (dest_ip, ttl)
                except Exception:
                    try:
                        s.close()
                    except Exception:
                        pass

        hops_map = {ip: {} for ip in batch}
        deadline = time.time() + timeout
        while socks and time.time() < deadline:
            try:
                select.select(list(socks.keys()), [], list(socks.keys()), 0.04)
            except Exception:
                pass
            for s in list(socks.keys()):
                dest_ip, ttl = socks[s]
                try:
                    data, ancdata, msg_flags, addr = s.recvmsg(512, 512, MSG_ERRQUEUE)
                    hop_ip = addr[0] if addr else None
                    for cmsg_lvl, cmsg_type, cmsg_data in ancdata:
                        if len(cmsg_data) >= 24:
                            fam = struct.unpack_from("<H", cmsg_data, 16)[0]
                            if fam == socket.AF_INET:
                                hop_ip = socket.inet_ntoa(cmsg_data[20:24])
                    if hop_ip:
                        hops_map[dest_ip][ttl] = hop_ip
                        s.close()
                        del socks[s]
                except BlockingIOError:
                    pass
                except Exception:
                    s.close()
                    del socks[s]

        for s in list(socks.keys()):
            try:
                s.close()
            except Exception:
                pass

        for ip in batch:
            # Trim duplicate trailing destination/gateway echoes while preserving distinct hops
            ordered = sorted(hops_map[ip].items())
            dedup = []
            for ttl, hip in ordered:
                if not dedup or dedup[-1][1] != hip:
                    dedup.append((ttl, hip))
                if hip == ip:
                    break
            results[ip] = dedup
    return results


def summarize_far_hops(hops_with_ptr, dest_ip):
    """Extract the most informative far-end ISP / regional PoP hops."""
    far = []
    for h in hops_with_ptr:
        ttl = h["ttl"]
        hip = h["ip"]
        host = (h.get("host") or "").lower()
        if hip.startswith("192.168.") or hip.startswith("10."):
            continue
        if ttl < 13 and any(sub in host for sub in (".sonic.net",)):
            continue
        if any(sub in host for sub in US_TRANSIT_SUBSTRS):
            continue
        far.append(h)
    tail = far[-3:] if far else hops_with_ptr[-2:]
    labels = []
    for item in tail:
        lbl = item.get("host") or item["ip"]
        if lbl not in labels:
            labels.append(lbl)
    return tail, " -> ".join(labels[-2:])


def mode_single_trace(ip_str):
    t0 = time.time()
    bgp = lookup_bgp_origin(ip_str)
    asn_name = lookup_asn_name(bgp["asn"]) if bgp.get("asn") else ""
    ptr = lookup_ptr(ip_str)
    raw_hops = parallel_udp_traceroutes([ip_str], min_ttl=1, max_ttl=28, timeout=1.6, batch_targets=1)[ip_str]
    hop_ips = list({hip for _, hip in raw_hops})
    ptr_map = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        for hip, hptr in zip(hop_ips, ex.map(lookup_ptr, hop_ips)):
            ptr_map[hip] = hptr
    hops_out = [{"ttl": ttl, "ip": hip, "host": ptr_map.get(hip, "")} for ttl, hip in raw_hops]
    far_tail, pop_sig = summarize_far_hops(hops_out, ip_str)
    return {
        "ip": ip_str,
        "ptr": ptr,
        "ptr_domain": extract_ptr_domain(ptr),
        "bgp": bgp,
        "asn_name": asn_name,
        "hops": hops_out,
        "pop_sig": pop_sig,
        "elapsed_ms": int((time.time() - t0) * 1000),
    }


def mode_batch_enrich(req):
    ips = req.get("ips") or []
    trace_subnets = set(req.get("trace_subnets") or [])

    # Group by /24
    by_s24 = {}
    for ip in ips:
        parts = ip.strip().split(".")
        if len(parts) == 4:
            s24 = "%s.%s.%s.0/24" % (parts[0], parts[1], parts[2])
            by_s24.setdefault(s24, ip)

    s24_list = sorted(by_s24.keys())
    rep_ips = [by_s24[s24] for s24 in s24_list]

    bgp_list = []
    ptr_list = []
    with ThreadPoolExecutor(max_workers=24) as ex:
        bgp_futs = [ex.submit(lookup_bgp_origin, ip) for ip in rep_ips]
        ptr_futs = [ex.submit(lookup_ptr, ip) for ip in rep_ips]
        bgp_list = [f.result() for f in bgp_futs]
        ptr_list = [f.result() for f in ptr_futs]

    distinct_asns = sorted({b["asn"] for b in bgp_list if b.get("asn")})
    asn_names = {}
    if distinct_asns:
        with ThreadPoolExecutor(max_workers=16) as ex:
            for asn, aname in zip(distinct_asns, ex.map(lookup_asn_name, distinct_asns)):
                asn_names[asn] = aname

    # Run parallel traceroutes for requested /24s
    trace_rep_ips = [by_s24[s24] for s24 in s24_list if s24 in trace_subnets]
    trace_raw = {}
    hop_ptr_map = {}
    if trace_rep_ips:
        trace_raw = parallel_udp_traceroutes(trace_rep_ips, min_ttl=12, max_ttl=26, timeout=1.45, batch_targets=10)
        all_hop_ips = sorted({hip for hlist in trace_raw.values() for _, hip in hlist})
        if all_hop_ips:
            with ThreadPoolExecutor(max_workers=24) as ex:
                for hip, hptr in zip(all_hop_ips, ex.map(lookup_ptr, all_hop_ips)):
                    hop_ptr_map[hip] = hptr

    subnets_out = {}
    for s24, rep_ip, bgp, ptr in zip(s24_list, rep_ips, bgp_list, ptr_list):
        asn = bgp.get("asn")
        entry = {
            "subnet24": s24,
            "sample_ip": rep_ip,
            "asn": asn,
            "asn_name": asn_names.get(asn, "") if asn else "",
            "bgp_prefix": bgp.get("bgp_prefix", ""),
            "country": bgp.get("country", ""),
            "alloc_date": bgp.get("alloc_date", ""),
            "ptr": ptr,
            "ptr_domain": extract_ptr_domain(ptr),
        }
        if rep_ip in trace_raw:
            hops_full = [{"ttl": ttl, "ip": hip, "host": hop_ptr_map.get(hip, "")} for ttl, hip in trace_raw[rep_ip]]
            far_tail, pop_sig = summarize_far_hops(hops_full, rep_ip)
            entry["traced"] = True
            entry["far_hops"] = far_tail
            entry["pop_sig"] = pop_sig
        subnets_out[s24] = entry

    return {"subnets": subnets_out}


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "--single-trace":
        json.dump(mode_single_trace(sys.argv[2]), sys.stdout)
        return
    req = json.loads(sys.stdin.read() or "{}")
    json.dump(mode_batch_enrich(req), sys.stdout)


if __name__ == "__main__":
    main()
