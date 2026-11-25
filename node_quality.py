#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clash node quality auditing, IP origin (Native vs Broadcast) detection,
composite scoring/ranking, and rolling-update Git push gating.

Designed for low-memory ARM64 OpenStick (512MB RAM, Python 3.9+) with zero
hardcoded secrets. All HTTP probes use strict timeouts, bounded response reads,
and per-node session reuse (never reusing a single HTTP proxy session across
Mihomo WORKER_i node switches).
"""

import hashlib
import json
import logging
import math
import os
import re
import threading
import time
import urllib.parse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

import requests
from requests.adapters import HTTPAdapter

log = logging.getLogger("node_quality")

# ======================== Constants & Defaults ========================

DEFAULT_PUSH_STATE_FILE = Path("/opt/clash-filter/push_state.json")
DEFAULT_IP_CACHE_FILE = Path("/opt/clash-filter/ip_intel_cache.json")
DEFAULT_IP_CACHE_TTL_SEC = 48.0 * 3600.0
DEFAULT_IP_CACHE_MAX_ENTRIES = 2000
IPAPI_QUOTA_COOLDOWN_SEC = 900.0
DEFAULT_SPEEDTEST_BYTES = 131072  # 128 KiB: 0.2-0.8s on stick, safe for 2.4G Wi-Fi
MAX_SPEEDTEST_BYTES = 524288      # Hard safety cap (512 KiB)
MAX_INSPECT_BODY_BYTES = 32768    # Cap HTML inspection reads to protect RAM

_IP_CACHE_LOCK = threading.RLock()
_IP_CACHE_STORE: Dict[str, Dict[str, Any]] = {}
_IPAPI_QUOTA_UNTIL: float = 0.0
_IPAPI_QUOTA_LOGGED: bool = False

USER_AGENT_BROWSER = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
USER_AGENT_CLI = "curl/8.5.0"

DEFAULT_SPEEDTEST_URLS: Tuple[str, ...] = (
    "https://speed.cloudflare.com/__down?bytes={nbytes}",
    "https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js",
)

# Regional/placeholder codes returned by RDAP or GeoIP that are not a real country
NON_COUNTRY_CODES = frozenset({"", "EU", "AP", "ZZ", "XX", "T1", "A1", "A2"})

DATACENTER_KEYWORDS: Tuple[str, ...] = (
    "amazon", "aws", "google", "gcp", "microsoft", "azure", "alibaba", "aliyun",
    "tencent", "oracle", "digitalocean", "vultr", "linode", "akamai", "hetzner",
    "ovh", "cloudflare", "choopa", "leaseweb", "zenlayer", "racknerd",
    "colocrossing", "datacamp", "cdn77", "m247", "hosting", "datacenter",
    "data center", "cloud", "vps", "server", "dedicated", "contabo", "scaleway",
    "kamatera", "ucloud", "huawei", "bytedance", "dmit", "bandwagon", "it7",
    "sharktech", "psychz", "quadranet", "cogent", "hostwinds", "ionos",
    "b2 net", "xserver", "sakura internet", "conoha", "gmo internet", "idc",
)

TIER_PRIORITY: Dict[str, int] = {"纯净": 1, "优选": 2, "普通": 3}
ORIGIN_PRIORITY: Dict[str, int] = {"原生": 1, "未知": 2, "广播": 3}

PROTOCOL_BONUS: Dict[str, float] = {
    "vless": 4.0,
    "hysteria2": 4.0,
    "hy2": 4.0,
    "tuic": 4.0,
    "anytls": 4.0,
    "hysteria": 2.5,
    "trojan": 2.5,
    "ss": 1.5,
    "snell": 1.5,
    "wireguard": 1.5,
    "vmess": 0.5,
    "ssr": 0.0,
}

GOOGLE_BLOCK_MARKERS: Tuple[str, ...] = (
    "/sorry/",
    "unusual traffic",
    "captcha",
    "recaptcha",
    "automated queries",
    "not available in your country",
    "not supported in your country",
)

GEMINI_URL_BLOCK_MARKERS: Tuple[str, ...] = (
    "/sorry/",
    "unsupported",
    "restricted",
    "unavailable",
)

DEFAULT_CC_REGION_NAMES: Dict[str, str] = {
    "HK": "香港", "TW": "台湾", "MO": "澳门", "JP": "日本", "SG": "新加坡",
    "US": "美国", "KR": "韩国", "DE": "德国", "GB": "英国", "UK": "英国",
    "FR": "法国", "NL": "荷兰", "RU": "俄罗斯", "IT": "意大利", "CA": "加拿大",
    "AU": "澳大利亚", "IN": "印度", "TR": "土耳其", "VN": "越南", "MY": "马来西亚",
    "TH": "泰国", "PH": "菲律宾", "ID": "印尼", "BR": "巴西", "ES": "西班牙",
    "SE": "瑞典", "CH": "瑞士", "AT": "奥地利", "BG": "保加利亚", "IE": "爱尔兰",
    "FI": "芬兰", "NO": "挪威", "PL": "波兰", "UA": "乌克兰", "AR": "阿根廷",
    "CL": "智利", "CO": "哥伦比亚", "MX": "墨西哥", "ZA": "南非", "AE": "阿联酋",
    "IL": "以色列", "NZ": "新西兰", "KZ": "哈萨克斯坦", "IS": "冰岛", "CZ": "捷克",
    "RO": "罗马尼亚", "MD": "摩尔多瓦", "RS": "塞尔维亚", "HU": "匈牙利", "GR": "希腊",
    "PT": "葡萄牙", "BE": "比利时", "DK": "丹麦", "LU": "卢森堡", "EE": "爱沙尼亚",
    "LV": "拉脱维亚", "LT": "立陶宛", "GE": "格鲁吉亚", "AM": "亚美尼亚", "AZ": "阿塞拜疆",
    "UZ": "乌兹别克斯坦", "KG": "吉尔吉斯斯坦", "TJ": "塔吉克斯坦", "TM": "土库曼斯坦",
    "MN": "蒙古", "PK": "巴基斯坦", "BD": "孟加拉国", "LK": "斯里兰卡", "NP": "尼泊尔",
    "KH": "柬埔寨", "LA": "老挝", "MM": "缅甸", "OTHER": "其他", "UN": "其他",
}


# ======================== Session & Fingerprint Helpers ========================

def proxy_fingerprint(proxy: Dict[str, Any]) -> str:
    """Extract canonical node fingerprint independent of node display name."""
    server = str(proxy.get("server", "")).strip().lower()
    port = str(proxy.get("port", "")).strip()
    p_type = str(proxy.get("type", "")).strip().lower()
    if p_type == "hy2":
        p_type = "hysteria2"
    cred = str(
        proxy.get("uuid")
        or proxy.get("password")
        or proxy.get("auth")
        or proxy.get("auth-str")
        or proxy.get("token")
        or ""
    ).strip()
    return f"{p_type}://{server}:{port}:{cred}"


def create_probe_session(
    proxy_url: Optional[str] = None,
    *,
    pool_size: int = 4,
    user_agent: str = USER_AGENT_BROWSER,
) -> requests.Session:
    """
    Create a pooled requests.Session isolated from environment proxies.

    IMPORTANT: For Mihomo WORKER_i listeners, create one Session per tested node
    and close it before switching the group to the next node so HTTP CONNECT
    tunnels are never reused across nodes.
    """
    s = requests.Session()
    s.trust_env = False
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    if proxy_url:
        s.proxies = {"http": proxy_url, "https": proxy_url}
    if user_agent:
        s.headers.update({"User-Agent": user_agent})
    return s


def _normalize_cc(val: Any) -> str:
    cc = str(val or "").strip().upper()
    if cc == "UK":
        cc = "GB"
    if re.fullmatch(r"[A-Z]{2}", cc):
        return cc
    return ""


def _atomic_write_json(path: Path, payload: Any) -> Path:
    """Atomically persist JSON using a sibling temp file, fsync, and os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(str(tmp_path), str(path))
        return path
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def reset_ipapi_quota_cooldown() -> None:
    """Clear any active ipapi.is quota cooldown (mainly for tests)."""
    global _IPAPI_QUOTA_UNTIL, _IPAPI_QUOTA_LOGGED
    with _IP_CACHE_LOCK:
        _IPAPI_QUOTA_UNTIL = 0.0
        _IPAPI_QUOTA_LOGGED = False


def _is_ipapi_quota_blocked(now_ts: Optional[float] = None) -> bool:
    curr = time.monotonic() if now_ts is None else now_ts
    with _IP_CACHE_LOCK:
        return curr < _IPAPI_QUOTA_UNTIL


def _mark_ipapi_quota_blocked(reason: str, cooldown_sec: float = IPAPI_QUOTA_COOLDOWN_SEC) -> None:
    global _IPAPI_QUOTA_UNTIL, _IPAPI_QUOTA_LOGGED
    with _IP_CACHE_LOCK:
        _IPAPI_QUOTA_UNTIL = time.monotonic() + max(60.0, float(cooldown_sec))
        if not _IPAPI_QUOTA_LOGGED:
            _IPAPI_QUOTA_LOGGED = True
            log.warning(
                "ipapi.is quota/auth limit reached (%s); cooling down for %ds and using free fallbacks.",
                reason, int(cooldown_sec),
            )


def _get_cache_bucket_locked(cache_file: Path) -> Dict[str, Any]:
    key = str(cache_file.resolve()) if cache_file.exists() else str(cache_file)
    bucket = _IP_CACHE_STORE.get(key)
    disk_mtime = None
    if cache_file.exists():
        try:
            disk_mtime = cache_file.stat().st_mtime
        except OSError:
            disk_mtime = None

    if bucket is None or (disk_mtime is not None and bucket.get("mtime") != disk_mtime):
        entries: Dict[str, Any] = {}
        if cache_file.exists():
            try:
                with cache_file.open("r", encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, dict):
                    raw_entries = raw.get("entries", raw)
                    if isinstance(raw_entries, dict):
                        for ip, item in raw_entries.items():
                            if isinstance(item, dict) and isinstance(item.get("data"), dict):
                                entries[str(ip)] = item
            except Exception as exc:
                log.warning("Failed to load IP intel cache (%s): %s", cache_file, exc)
        bucket = {"entries": entries, "mtime": disk_mtime}
        _IP_CACHE_STORE[key] = bucket
    return bucket


def load_ip_intel_cache(cache_file: Path = DEFAULT_IP_CACHE_FILE) -> Dict[str, Any]:
    """Load persisted IP intelligence cache entries mapped by IP address."""
    with _IP_CACHE_LOCK:
        bucket = _get_cache_bucket_locked(cache_file)
        return dict(bucket["entries"])


def get_cached_ip_intel(
    ip: str,
    *,
    cache_file: Path = DEFAULT_IP_CACHE_FILE,
    ttl_sec: float = DEFAULT_IP_CACHE_TTL_SEC,
    now_ts: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    """Return a fresh cached IP intelligence dict for `ip`, or None on miss/expiry."""
    ip_clean = str(ip or "").strip()
    if not ip_clean:
        return None
    curr_ts = float(time.time() if now_ts is None else now_ts)

    with _IP_CACHE_LOCK:
        bucket = _get_cache_bucket_locked(cache_file)
        entries = bucket["entries"]
        entry = entries.get(ip_clean)
        if not isinstance(entry, dict):
            return None
        try:
            cached_ts = float(entry.get("cached_ts", 0.0))
        except (TypeError, ValueError):
            entries.pop(ip_clean, None)
            return None

        if curr_ts < cached_ts:
            # Clock moved backward (e.g. pre-NTP boot); treat cautiously by age=0
            age = 0.0
        else:
            age = curr_ts - cached_ts
        if age >= ttl_sec:
            entries.pop(ip_clean, None)
            return None

        entry["last_seen_ts"] = curr_ts
        data = entry.get("data")
        return dict(data) if isinstance(data, dict) else None


def put_cached_ip_intel(
    ip: str,
    ip_info: Dict[str, Any],
    *,
    cache_file: Path = DEFAULT_IP_CACHE_FILE,
    ttl_sec: float = DEFAULT_IP_CACHE_TTL_SEC,
    max_entries: int = DEFAULT_IP_CACHE_MAX_ENTRIES,
    now_ts: Optional[float] = None,
) -> None:
    """Insert/refresh `ip` in the persistent TTL+LRU cache and write atomically."""
    ip_clean = str(ip or "").strip()
    if not ip_clean or not isinstance(ip_info, dict):
        return
    curr_ts = float(time.time() if now_ts is None else now_ts)

    clean_data = {
        k: v for k, v in ip_info.items()
        if k not in ("cache_hit",)
    }

    with _IP_CACHE_LOCK:
        bucket = _get_cache_bucket_locked(cache_file)
        entries: Dict[str, Any] = bucket["entries"]

        # Purge expired entries first
        expired = [
            k for k, v in entries.items()
            if not isinstance(v, dict)
            or (curr_ts - float(v.get("cached_ts", 0.0) or 0.0)) >= ttl_sec
        ]
        for k in expired:
            entries.pop(k, None)

        entries[ip_clean] = {
            "cached_ts": curr_ts,
            "last_seen_ts": curr_ts,
            "data": clean_data,
        }

        # LRU eviction by last_seen_ts when exceeding max_entries
        cap = max(1, int(max_entries))
        if len(entries) > cap:
            ordered = sorted(
                entries.items(),
                key=lambda kv: float(kv[1].get("last_seen_ts", kv[1].get("cached_ts", 0.0)) or 0.0),
            )
            for k, _ in ordered[: len(entries) - cap]:
                entries.pop(k, None)

        try:
            _atomic_write_json(cache_file, {"version": 1, "entries": entries})
            try:
                bucket["mtime"] = cache_file.stat().st_mtime
            except OSError:
                pass
        except Exception as exc:
            log.warning("Failed to persist IP intel cache (%s): %s", cache_file, exc)


def _read_bounded_text(resp: requests.Response, max_bytes: int = MAX_INSPECT_BODY_BYTES) -> str:
    """Read at most max_bytes from a streaming response to protect stick RAM."""
    chunks: List[bytes] = []
    total = 0
    try:
        for chunk in resp.iter_content(chunk_size=8192):
            if not chunk:
                continue
            remaining = max_bytes - total
            if remaining <= 0:
                break
            chunks.append(chunk[:remaining])
            total += min(len(chunk), remaining)
            if total >= max_bytes:
                break
    except Exception:
        pass
    return b"".join(chunks).decode("utf-8", errors="ignore")


# ======================== 1. IP Origin Detection (Native vs Broadcast) ========================

def _parse_ipapi_is_payload(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize keyed or unkeyed api.ipapi.is JSON into a unified dict."""
    if not isinstance(d, dict) or d.get("error"):
        return None

    loc = d.get("location") if isinstance(d.get("location"), dict) else {}
    asn_raw = d.get("asn")
    asn_info = asn_raw if isinstance(asn_raw, dict) else {}
    comp_raw = d.get("company")
    company = comp_raw if isinstance(comp_raw, dict) else {}

    # Keyed response carries location.country_code; unkeyed falls back to country name/code
    cc = _normalize_cc(loc.get("country_code"))
    if not cc and isinstance(d.get("country_code"), str):
        cc = _normalize_cc(d.get("country_code"))
    if not cc:
        return None

    asn_cc = _normalize_cc(asn_info.get("country"))
    company_name = str(company.get("name") or (comp_raw if isinstance(comp_raw, str) else "")).strip()
    company_domain = str(company.get("domain", "")).strip().lower()
    company_type = str(company.get("type", "")).strip().lower()
    asn_org = str(asn_info.get("org") or (asn_raw if isinstance(asn_raw, str) else "")).strip()
    asn_num = asn_info.get("asn", "")
    asn_str = f"AS{asn_num}" if asn_num else (asn_org if asn_org.upper().startswith("AS") else "")

    is_dc = d.get("is_datacenter")
    is_proxy_flag = (
        bool(d.get("is_proxy", False))
        or bool(d.get("is_vpn", False))
        or bool(d.get("is_tor", False))
        if any(k in d for k in ("is_proxy", "is_vpn", "is_tor"))
        else None
    )

    return {
        "ip": str(d.get("ip", "")).strip(),
        "cc": cc,
        "asn_country": asn_cc,
        "rir": str(d.get("rir", "")).strip().upper(),
        "asn_rir": str(asn_info.get("rir", "")).strip().upper(),
        "asn_type": str(asn_info.get("type", "")).strip().lower(),
        "company_name": company_name,
        "company_domain": company_domain,
        "company_type": company_type,
        "isp": company_name or asn_org,
        "org": asn_org,
        "as": asn_str,
        "hosting": bool(is_dc) if is_dc is not None else None,
        "proxy": is_proxy_flag,
        "is_datacenter": bool(is_dc) if is_dc is not None else None,
        "is_proxy": bool(d.get("is_proxy", False)) if "is_proxy" in d else None,
        "is_vpn": bool(d.get("is_vpn", False)) if "is_vpn" in d else None,
        "is_tor": bool(d.get("is_tor", False)) if "is_tor" in d else None,
        "is_abuser": bool(d.get("is_abuser", False)) if "is_abuser" in d else False,
        "is_anycast": bool(d.get("is_anycast", False)),
        "source": "ipapi.is",
    }


def lookup_rdap_country(
    ip: str,
    *,
    session: Optional[requests.Session] = None,
    proxy_url: Optional[str] = None,
    timeout: float = 3.5,
    rdap_base_url: str = "https://rdap.org/ip",
) -> str:
    """Query authoritative RIR RDAP for the registered country code of an IP."""
    ip_clean = str(ip or "").strip()
    if not ip_clean:
        return ""

    owns_session = session is None
    sess = session or create_probe_session(proxy_url, user_agent=USER_AGENT_CLI)
    try:
        url = f"{rdap_base_url.rstrip('/')}/{urllib.parse.quote(ip_clean, safe=':.')}"
        r = sess.get(url, timeout=timeout, headers={"Accept": "application/rdap+json, application/json"})
        if r.status_code != 200:
            return ""
        data = r.json()
        if not isinstance(data, dict):
            return ""
        cc = _normalize_cc(data.get("country"))
        if cc in NON_COUNTRY_CODES:
            return ""
        return cc
    except Exception:
        return ""
    finally:
        if owns_session:
            sess.close()


def detect_ip_origin_type(
    ip_info: Optional[Dict[str, Any]],
    *,
    rdap_country: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Classify an exit IP as Native ("原生"), Broadcast/Anycast ("广播"), or Unknown ("未知").

    Accepts either normalized dicts from query_exit_ip_info() or raw ipapi.is JSON payloads.
    Uses exit geolocation country vs ASN/RIR registration country (never top-level
    ipapi.is `rir`, which reflects ARIN-allocated global cloud ranges).
    """
    if not isinstance(ip_info, dict) or not ip_info:
        return ("未知", "empty_ip_info")

    # Support raw ipapi.is payload as well as normalized dict
    if "location" in ip_info and "cc" not in ip_info:
        parsed = _parse_ipapi_is_payload(ip_info)
        if parsed:
            ip_info = parsed

    geo_cc = _normalize_cc(ip_info.get("cc"))
    asn_raw = ip_info.get("asn")
    asn_cc = _normalize_cc(
        ip_info.get("asn_country")
        or (asn_raw.get("country") if isinstance(asn_raw, dict) else "")
    )
    rdap_cc = _normalize_cc(rdap_country or ip_info.get("rdap_country"))
    if rdap_cc in NON_COUNTRY_CODES:
        rdap_cc = ""
    if asn_cc in NON_COUNTRY_CODES:
        asn_cc = ""

    is_anycast = bool(ip_info.get("is_anycast", False))
    if is_anycast:
        return ("广播", "is_anycast=True")

    if not geo_cc or geo_cc in NON_COUNTRY_CODES:
        return ("未知", "missing_geo_country")

    domain_note = f", domain={ip_info['company_domain']}" if ip_info.get("company_domain") else ""
    if asn_cc:
        if asn_cc == geo_cc:
            return ("原生", f"asn_country={asn_cc} == geo={geo_cc}{domain_note}")
        return ("广播", f"asn_country={asn_cc} != geo={geo_cc}{domain_note}")

    if rdap_cc:
        if rdap_cc == geo_cc:
            return ("原生", f"rdap_country={rdap_cc} == geo={geo_cc}{domain_note}")
        return ("广播", f"rdap_country={rdap_cc} != geo={geo_cc}{domain_note}")

    return ("未知", f"missing_asn_and_rdap_country(geo={geo_cc})")


def _query_ipapi_is(
    sess: requests.Session,
    ipapi_url: str,
    ipapi_key: str,
    headers: Dict[str, str],
    timeout: float,
    target_ip: str = "",
) -> Optional[Dict[str, Any]]:
    """Query api.ipapi.is with quota/auth error handling (401/402/403/429)."""
    if not ipapi_key or _is_ipapi_quota_blocked():
        return None
    try:
        sep = "&" if "?" in ipapi_url else "?"
        if target_ip:
            q_ip = urllib.parse.quote(target_ip, safe=".:")
            url = f"{ipapi_url}{sep}q={q_ip}&key={ipapi_key}"
        else:
            url = f"{ipapi_url}{sep}key={ipapi_key}"
        r = sess.get(url, headers=headers, timeout=timeout)
        if r.status_code in (401, 402, 403, 429):
            _mark_ipapi_quota_blocked(f"http_{r.status_code}")
            return None
        if r.status_code == 200:
            d = r.json()
            if isinstance(d, dict) and d.get("error"):
                err_msg = str(d.get("error", ""))
                if any(tok in err_msg.lower() for tok in ("limit", "quota", "key", "unauthorized", "exceeded")):
                    _mark_ipapi_quota_blocked(err_msg[:64])
                return None
            return _parse_ipapi_is_payload(d)
    except Exception:
        pass
    return None


def _query_ip_api_com(
    sess: requests.Session,
    ip_api_url: str,
    headers: Dict[str, str],
    timeout: float,
) -> Optional[Dict[str, Any]]:
    try:
        r = sess.get(ip_api_url, headers=headers, timeout=timeout)
        if r.status_code == 200:
            d = r.json()
            if isinstance(d, dict) and d.get("status") == "success":
                cc = _normalize_cc(d.get("countryCode"))
                if cc:
                    return {
                        "ip": str(d.get("query", "")).strip(),
                        "cc": cc,
                        "asn_country": "",
                        "isp": str(d.get("isp", "")).strip(),
                        "org": str(d.get("org", "")).strip(),
                        "as": str(d.get("as", "")).strip(),
                        "hosting": d.get("hosting"),
                        "proxy": d.get("proxy"),
                        "is_datacenter": d.get("hosting"),
                        "is_proxy": d.get("proxy"),
                        "is_abuser": False,
                        "is_anycast": False,
                        "source": "ip-api.com",
                    }
    except Exception:
        pass
    return None


def _query_ipwho_is(
    sess: requests.Session,
    ipwho_url: str,
    headers: Dict[str, str],
    timeout: float,
) -> Optional[Dict[str, Any]]:
    try:
        r = sess.get(ipwho_url, headers=headers, timeout=timeout)
        if r.status_code == 200:
            d = r.json()
            if isinstance(d, dict) and d.get("success"):
                cc = _normalize_cc(d.get("country_code"))
                if cc:
                    conn = d.get("connection") if isinstance(d.get("connection"), dict) else {}
                    asn_val = conn.get("asn", "")
                    return {
                        "ip": str(d.get("ip", "")).strip(),
                        "cc": cc,
                        "asn_country": "",
                        "isp": str(conn.get("isp", "")).strip(),
                        "org": str(conn.get("org", "")).strip(),
                        "as": f"AS{asn_val}" if asn_val else "",
                        "hosting": None,
                        "proxy": None,
                        "is_datacenter": None,
                        "is_proxy": None,
                        "is_abuser": False,
                        "is_anycast": False,
                        "source": "ipwho.is",
                    }
    except Exception:
        pass
    return None


def query_exit_ip_info(
    proxy_url: str,
    *,
    ipapi_key: str = "",
    session: Optional[requests.Session] = None,
    relay_proxy_url: Optional[str] = None,
    enable_rdap_fallback: bool = True,
    timeout: float = 6.0,
    rdap_timeout: float = 3.5,
    ip_cache_file: Optional[Path] = DEFAULT_IP_CACHE_FILE,
    ip_cache_ttl_sec: float = DEFAULT_IP_CACHE_TTL_SEC,
    ip_cache_max_entries: int = DEFAULT_IP_CACHE_MAX_ENTRIES,
    ipapi_url: str = "https://api.ipapi.is/",
    ip_api_url: str = "http://ip-api.com/json/?fields=status,countryCode,isp,org,as,hosting,proxy,mobile,query",
    ipwho_url: str = "http://ipwho.is/",
    rdap_base_url: str = "https://rdap.org/ip",
) -> Optional[Dict[str, Any]]:
    """
    Probe exit IP geolocation, ISP/datacenter risk flags, and Native vs Broadcast origin.

    When `ip_cache_file` is enabled, resolves the exit IP via free ip-api.com/ipwho.is
    first and checks the persistent 48h TTL cache before consuming ipapi.is quota.
    Handles ipapi.is 401/402/403/429 quota exhaustion gracefully via free fallbacks.
    """
    owns_session = session is None
    sess = session or create_probe_session(proxy_url, user_agent=USER_AGENT_CLI)
    req_headers = {"User-Agent": USER_AGENT_CLI}
    probe_timeout = min(float(timeout), 4.5)
    info: Optional[Dict[str, Any]] = None

    try:
        if ip_cache_file is not None:
            # Cache-first mode: obtain exit IP via free endpoint, then check 48h cache
            info = _query_ip_api_com(sess, ip_api_url, req_headers, probe_timeout)
            if info is None:
                info = _query_ipwho_is(sess, ipwho_url, req_headers, probe_timeout)

            exit_ip = str(info.get("ip", "")).strip() if info else ""
            if exit_ip:
                cached = get_cached_ip_intel(
                    exit_ip, cache_file=ip_cache_file, ttl_sec=ip_cache_ttl_sec
                )
                if cached is not None and (
                    cached.get("source") == "ipapi.is"
                    or not ipapi_key
                    or _is_ipapi_quota_blocked()
                ):
                    origin_type, origin_reason = detect_ip_origin_type(
                        cached, rdap_country=cached.get("rdap_country")
                    )
                    cached["ip_origin"] = origin_type
                    cached["ip_origin_reason"] = origin_reason
                    cached["cache_hit"] = True
                    return cached

            # Cache miss: enrich with ipapi.is if key is valid & not in cooldown
            if ipapi_key and not _is_ipapi_quota_blocked():
                enriched: Optional[Dict[str, Any]] = None
                if exit_ip and relay_proxy_url:
                    with create_probe_session(relay_proxy_url, user_agent=USER_AGENT_CLI) as relay_s:
                        enriched = _query_ipapi_is(
                            relay_s, ipapi_url, ipapi_key, req_headers, float(timeout), target_ip=exit_ip
                        )
                if enriched is None:
                    enriched = _query_ipapi_is(
                        sess, ipapi_url, ipapi_key, req_headers, float(timeout), target_ip=exit_ip
                    )
                if enriched is not None:
                    if not enriched.get("cc") and info and info.get("cc"):
                        enriched["cc"] = info["cc"]
                    if not enriched.get("ip") and exit_ip:
                        enriched["ip"] = exit_ip
                    info = enriched
        else:
            # Direct mode (no persistent cache)
            info = _query_ipapi_is(sess, ipapi_url, ipapi_key, req_headers, float(timeout))
            if info is None:
                info = _query_ip_api_com(sess, ip_api_url, req_headers, probe_timeout)
            if info is None:
                info = _query_ipwho_is(sess, ipwho_url, req_headers, probe_timeout)

            if info is not None and ipapi_key and info.get("source") != "ipapi.is" and info.get("ip") and relay_proxy_url:
                with create_probe_session(relay_proxy_url, user_agent=USER_AGENT_CLI) as relay_s:
                    enriched = _query_ipapi_is(
                        relay_s, ipapi_url, ipapi_key, req_headers, float(timeout), target_ip=info["ip"]
                    )
                    if enriched:
                        if not enriched.get("cc") and info.get("cc"):
                            enriched["cc"] = info["cc"]
                        enriched["source"] = f"{info['source']}+ipapi.is_relay"
                        info = enriched

        if info is None:
            return None

        # Rare RDAP fallback only if asn_country is missing
        rdap_cc = str(info.get("rdap_country", "")).strip()
        if enable_rdap_fallback and info.get("ip") and not info.get("asn_country") and not rdap_cc:
            rdap_sess = sess
            owns_rdap_sess = False
            if relay_proxy_url:
                rdap_sess = create_probe_session(relay_proxy_url, user_agent=USER_AGENT_CLI)
                owns_rdap_sess = True
            try:
                rdap_cc = lookup_rdap_country(
                    info["ip"],
                    session=rdap_sess,
                    timeout=rdap_timeout,
                    rdap_base_url=rdap_base_url,
                )
            finally:
                if owns_rdap_sess:
                    rdap_sess.close()
            if rdap_cc:
                info["rdap_country"] = rdap_cc

        origin_type, origin_reason = detect_ip_origin_type(info, rdap_country=rdap_cc)
        info["ip_origin"] = origin_type
        info["ip_origin_reason"] = origin_reason
        info["cache_hit"] = False

        if (
            ip_cache_file is not None
            and info.get("ip")
            and (info.get("source") == "ipapi.is" or origin_type != "未知")
        ):
            put_cached_ip_intel(
                info["ip"],
                info,
                cache_file=ip_cache_file,
                ttl_sec=ip_cache_ttl_sec,
                max_entries=ip_cache_max_entries,
            )

        return info
    finally:
        if owns_session:
            sess.close()


# ======================== 2. Stage 1.5 Audit, Speedtest, Google/Gemini & Scoring ========================

def audit_stage1_node(
    proxy: Dict[str, Any],
    initial_delay_ms: int,
    *,
    api_port: int = 19999,
    timeout_ms: int = 3500,
    probe_url: str = "https://www.google.com/generate_204",
    samples: int = 3,
    min_success: int = 2,
    session: Optional[requests.Session] = None,
    sample_interval_s: float = 0.25,
) -> Optional[Dict[str, Any]]:
    """
    Perform multi-sample Stage 1.5 delay + jitter audit against Mihomo REST API.

    Reuses the caller's REST API Session per worker thread to avoid socket churn.
    Penalizes jitter spikes and packet loss without dropping slightly jittery good nodes.
    """
    name = str(proxy.get("name", "")).strip()
    if not name:
        return None

    encoded_name = urllib.parse.quote(name, safe="")
    encoded_probe = urllib.parse.quote(probe_url, safe="")
    url = (
        f"http://127.0.0.1:{api_port}/proxies/{encoded_name}/delay"
        f"?timeout={timeout_ms}&url={encoded_probe}"
    )

    success_delays: List[int] = []
    attempts = 0
    if 0 < int(initial_delay_ms) <= timeout_ms:
        success_delays.append(int(initial_delay_ms))
        attempts = 1

    owns_session = session is None
    sess = session or create_probe_session(None)
    req_timeout = (timeout_ms / 1000.0) + 0.8

    try:
        while attempts < samples:
            # Early exit if remaining attempts cannot achieve min_success
            remaining = samples - attempts
            if len(success_delays) + remaining < min_success:
                break
            if attempts > 0 and sample_interval_s > 0:
                time.sleep(sample_interval_s)
            attempts += 1
            try:
                resp = sess.get(url, timeout=req_timeout)
                if resp.status_code == 200:
                    d = int(resp.json().get("delay", 9999))
                    if 0 < d <= timeout_ms:
                        success_delays.append(d)
            except Exception:
                pass
    finally:
        if owns_session:
            sess.close()

    if len(success_delays) < min_success:
        return None

    d_min = min(success_delays)
    d_max = max(success_delays)
    d_avg = sum(success_delays) / len(success_delays)
    jitter_ms = d_max - d_min
    loss_rate = max(0.0, 1.0 - (len(success_delays) / max(1, attempts)))

    # Weighted effective latency penalizing jitter spikes and partial probe loss
    effective_delay_ms = int(
        round(0.45 * d_avg + 0.25 * d_min + 0.30 * jitter_ms + 400.0 * loss_rate)
    )

    return {
        "proxy": proxy,
        "delay": effective_delay_ms,
        "delay_ms": effective_delay_ms,
        "min_delay_ms": d_min,
        "max_delay_ms": d_max,
        "avg_delay_ms": int(round(d_avg)),
        "jitter_ms": jitter_ms,
        "samples": success_delays,
        "attempts": attempts,
        "loss_rate": round(loss_rate, 3),
    }


def _has_block_marker(text: str, markers: Tuple[str, ...]) -> Optional[str]:
    lowered = text.lower()
    for m in markers:
        if m in lowered:
            return m
    return None


def check_google_and_connectivity(
    session: requests.Session,
    *,
    timeout: float = 6.0,
    check_openai: bool = False,
    google_204_url: str = "https://www.google.com/generate_204",
    google_search_url: str = "https://www.google.com/search?q=cloudflare&hl=en&gl=us",
    gemini_url: str = "https://gemini.google.com/app",
    cloudflare_url: str = "https://cp.cloudflare.com/generate_204",
    youtube_url: str = "https://www.youtube.com/generate_204",
    openai_url: str = "https://api.openai.com/v1/models",
) -> Dict[str, Any]:
    """
    Verify general HTTPS connectivity and distinguish clean Google/Gemini access
    from HTTP 429/403, /sorry/, CAPTCHA, or geo-restriction blocks.
    """
    headers = {
        "User-Agent": USER_AGENT_BROWSER,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    google_204_ok = False
    cloudflare_ok = False
    youtube_ok = False
    conn_timeout = min(float(timeout), 4.5)
    hard_net_failures = 0

    for attempt in range(2):
        if attempt == 0:
            try:
                r = session.get(google_204_url, headers=headers, timeout=conn_timeout)
                if r.status_code in (200, 204):
                    google_204_ok = True
                    break
            except requests.exceptions.RequestException:
                hard_net_failures += 1
            except Exception:
                pass

            try:
                r = session.get(cloudflare_url, headers=headers, timeout=conn_timeout)
                if r.status_code in (200, 204):
                    cloudflare_ok = True
                    break
            except requests.exceptions.RequestException:
                hard_net_failures += 1
            except Exception:
                pass

            # Fast-fail if both Google and Cloudflare raised transport/TLS/proxy errors
            if hard_net_failures >= 2:
                break
            time.sleep(0.25)
        else:
            try:
                r = session.get(youtube_url, headers=headers, timeout=conn_timeout)
                if r.status_code in (200, 204):
                    youtube_ok = True
                    break
            except Exception:
                pass

            try:
                r = session.get(cloudflare_url, headers=headers, timeout=conn_timeout)
                if r.status_code in (200, 204):
                    cloudflare_ok = True
                    break
            except Exception:
                pass

    https_ok = google_204_ok or youtube_ok or cloudflare_ok
    if not https_ok:
        return {
            "https_ok": False,
            "google_204_ok": False,
            "cloudflare_ok": False,
            "google_clean": False,
            "gemini_clean": False,
            "openai_reachable": None,
            "reason": "all_connectivity_probes_failed",
        }

    # Check Google Search cleanliness (/sorry/, 429, 403, CAPTCHA)
    google_search_clean = False
    google_blocked = False
    reasons: List[str] = []

    if google_204_ok:
        try:
            with session.get(
                google_search_url, headers=headers, timeout=timeout, allow_redirects=True, stream=True
            ) as r:
                final_url = str(getattr(r, "url", "") or "")
                body_snip = _read_bounded_text(r)
                marker = _has_block_marker(final_url, GOOGLE_BLOCK_MARKERS) or _has_block_marker(
                    body_snip, GOOGLE_BLOCK_MARKERS
                )
                if r.status_code in (403, 429):
                    google_blocked = True
                    reasons.append(f"google_search_http_{r.status_code}")
                elif marker:
                    google_blocked = True
                    reasons.append(f"google_search_marker:{marker}")
                elif r.status_code == 200:
                    google_search_clean = True
                else:
                    reasons.append(f"google_search_http_{r.status_code}")
        except Exception as exc:
            reasons.append(f"google_search_err:{type(exc).__name__}")
    else:
        reasons.append("google_204_failed")

    # Check Gemini cleanliness (200 or accounts.google.com login redirect vs 429/403/geo-block)
    gemini_clean = False
    try:
        with session.get(
            gemini_url, headers=headers, timeout=timeout, allow_redirects=True, stream=True
        ) as r:
            final_url = str(getattr(r, "url", "") or "")
            body_snip = _read_bounded_text(r)
            url_marker = _has_block_marker(final_url, GEMINI_URL_BLOCK_MARKERS)
            body_marker = _has_block_marker(body_snip, GOOGLE_BLOCK_MARKERS)

            if r.status_code in (403, 429):
                google_blocked = True
                reasons.append(f"gemini_http_{r.status_code}")
            elif url_marker or body_marker:
                if (url_marker == "/sorry/") or (body_marker in ("/sorry/", "unusual traffic", "captcha", "recaptcha")):
                    google_blocked = True
                reasons.append(f"gemini_blocked:{url_marker or body_marker}")
            elif r.status_code == 200 or "accounts.google.com" in final_url.lower():
                gemini_clean = True
            else:
                reasons.append(f"gemini_http_{r.status_code}")
    except Exception as exc:
        reasons.append(f"gemini_err:{type(exc).__name__}")

    openai_reachable: Optional[bool] = None
    if check_openai:
        try:
            r_oa = session.get(openai_url, headers=headers, timeout=min(timeout, 4.0))
            # OpenAI returns 401/403 JSON on reachable IPs without auth, or 403/429 CF block;
            # 200/401 means API endpoint reached cleanly.
            openai_reachable = r_oa.status_code in (200, 401)
        except Exception:
            openai_reachable = False

    google_clean = bool(google_204_ok and google_search_clean and not google_blocked)
    if not reasons and google_clean and gemini_clean:
        reasons.append("google_and_gemini_clean")

    return {
        "https_ok": True,
        "google_204_ok": google_204_ok,
        "cloudflare_ok": cloudflare_ok or youtube_ok,
        "google_clean": google_clean,
        "gemini_clean": gemini_clean,
        "openai_reachable": openai_reachable,
        "reason": ";".join(reasons),
    }


def measure_throughput_kbps(
    session: requests.Session,
    *,
    nbytes: int = DEFAULT_SPEEDTEST_BYTES,
    timeout: float = 5.0,
    urls: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """
    Run a capped micro-payload download to measure practical throughput in kbps.
    Strictly caps read bytes and streams chunks to minimize RAM usage.
    """
    capped_bytes = max(16384, min(int(nbytes), MAX_SPEEDTEST_BYTES))
    target_urls = [u.format(nbytes=capped_bytes) for u in (urls or DEFAULT_SPEEDTEST_URLS)]

    req_timeout = (min(float(timeout), 3.5), float(timeout))
    for url in target_urls:
        t0 = time.monotonic()
        downloaded = 0
        try:
            with session.get(url, timeout=req_timeout, stream=True) as r:
                if r.status_code != 200:
                    continue
                for chunk in r.iter_content(chunk_size=16384):
                    if not chunk:
                        continue
                    needed = capped_bytes - downloaded
                    if needed <= 0:
                        break
                    downloaded += min(len(chunk), needed)
                    if downloaded >= capped_bytes:
                        break
            elapsed_s = max(time.monotonic() - t0, 0.01)
            if downloaded >= 8192:
                speed_kbps = (downloaded * 8.0) / (elapsed_s * 1000.0)
                return {
                    "speed_kbps": round(speed_kbps, 1),
                    "bytes_downloaded": downloaded,
                    "duration_ms": int(round(elapsed_s * 1000.0)),
                    "url": url,
                }
        except Exception:
            if downloaded > 0:
                break
            continue

    return {"speed_kbps": 0.0, "bytes_downloaded": 0, "duration_ms": 0, "url": ""}


def classify_node_tier(
    ip_info: Dict[str, Any],
    latency_ms: int,
    *,
    jitter_ms: int = 0,
    ip_origin: Optional[str] = None,
    google_clean: Optional[bool] = None,
    speed_kbps: float = 0.0,
    premium_max_latency_ms: int = 800,
) -> str:
    """
    Classify node tier into "纯净" (Pure Residential Native), "优选" (Preferred),
    or "普通" (Standard). Backwards-compatible with 2-arg calls.
    """
    isp_blob = (
        f"{ip_info.get('isp', '')} {ip_info.get('org', '')} "
        f"{ip_info.get('as', '')} {ip_info.get('company_name', '')}"
    ).lower()
    is_hosting_kw = any(kw in isp_blob for kw in DATACENTER_KEYWORDS)
    api_hosting = ip_info.get("hosting")
    if api_hosting is None:
        api_hosting = ip_info.get("is_datacenter")
    api_proxy = ip_info.get("proxy")
    if api_proxy is None and any(k in ip_info for k in ("is_proxy", "is_vpn", "is_tor")):
        api_proxy = bool(ip_info.get("is_proxy") or ip_info.get("is_vpn") or ip_info.get("is_tor"))
    is_abuser = bool(ip_info.get("is_abuser", False))

    origin = ip_origin if ip_origin is not None else ip_info.get("ip_origin")

    is_residential = (
        bool(isp_blob.strip())
        and not is_hosting_kw
        and api_hosting is False
        and api_proxy is False
        and not is_abuser
    )
    if is_residential and origin != "广播" and google_clean is not False:
        return "纯净"

    if (
        (latency_ms <= premium_max_latency_ms and jitter_ms <= 350 and google_clean is not False)
        or (
            origin == "原生"
            and google_clean is True
            and latency_ms <= int(premium_max_latency_ms * 1.35)
            and speed_kbps >= 500.0
        )
        or (
            speed_kbps >= 2000.0
            and latency_ms <= int(premium_max_latency_ms * 1.25)
            and google_clean is not False
        )
    ):
        return "优选"

    return "普通"


def compute_quality_score(
    *,
    tier: str,
    ip_origin: str,
    delay_ms: int,
    jitter_ms: int = 0,
    loss_rate: float = 0.0,
    speed_kbps: float = 0.0,
    google_clean: Optional[bool] = None,
    gemini_clean: Optional[bool] = None,
    proxy_type: str = "",
    ip_info: Optional[Dict[str, Any]] = None,
) -> float:
    """
    Compute composite quality score (higher is better) combining cleanliness,
    Native vs Broadcast IP, Google/Gemini health, throughput, latency/jitter, and protocol.
    """
    score = 0.0

    # 1. Tier & abuse/proxy posture
    if tier == "纯净":
        score += 30.0
    elif tier == "优选":
        score += 18.0
    else:
        score += 8.0

    if isinstance(ip_info, dict):
        if ip_info.get("is_abuser"):
            score -= 10.0
        if ip_info.get("proxy") or ip_info.get("is_proxy") or ip_info.get("is_vpn") or ip_info.get("is_tor"):
            score -= 6.0

    # 2. Native vs Broadcast IP
    if ip_origin == "原生":
        score += 18.0
    elif ip_origin == "未知":
        score += 4.0
    elif ip_origin == "广播":
        score -= 8.0

    # 3. Google & Gemini cleanliness
    if google_clean is True:
        score += 12.0
    elif google_clean is False:
        score -= 15.0
    if gemini_clean is True:
        score += 6.0

    # 4. Throughput (saturating curve up to ~22 pts at 8 Mbps)
    if speed_kbps > 0:
        score += min(22.0, (math.log1p(speed_kbps / 250.0) / math.log1p(32.0)) * 22.0)

    # 5. Latency, jitter, and loss
    clamped_delay = max(0, min(int(delay_ms), 2500))
    score += 20.0 * (1.0 - clamped_delay / 2500.0)
    score -= min(12.0, max(0, int(jitter_ms)) / 50.0)
    score -= max(0.0, float(loss_rate)) * 15.0

    # 6. Protocol bonus
    score += PROTOCOL_BONUS.get(str(proxy_type or "").strip().lower(), 0.0)

    return round(score, 2)


def evaluate_node_quality(
    proxy: Dict[str, Any],
    proxy_url: str,
    delay_ms: int,
    *,
    jitter_ms: int = 0,
    loss_rate: float = 0.0,
    ipapi_key: str = "",
    relay_proxy_url: Optional[str] = None,
    enable_speedtest: bool = True,
    speedtest_bytes: int = DEFAULT_SPEEDTEST_BYTES,
    speedtest_timeout: float = 5.0,
    probe_timeout: float = 6.0,
    enable_rdap_fallback: bool = True,
    check_openai: bool = False,
    ip_cache_file: Optional[Path] = DEFAULT_IP_CACHE_FILE,
    ip_cache_ttl_sec: float = DEFAULT_IP_CACHE_TTL_SEC,
    ip_cache_max_entries: int = DEFAULT_IP_CACHE_MAX_ENTRIES,
    fallback_cc_fn: Optional[Callable[[str], str]] = None,
    ipapi_url: str = "https://api.ipapi.is/",
    ip_api_url: str = "http://ip-api.com/json/?fields=status,countryCode,isp,org,as,hosting,proxy,mobile,query",
    ipwho_url: str = "http://ipwho.is/",
    rdap_base_url: str = "https://rdap.org/ip",
    google_204_url: str = "https://www.google.com/generate_204",
    google_search_url: str = "https://www.google.com/search?q=cloudflare&hl=en&gl=us",
    gemini_url: str = "https://gemini.google.com/app",
    cloudflare_url: str = "https://cp.cloudflare.com/generate_204",
    youtube_url: str = "https://www.youtube.com/generate_204",
    speedtest_urls: Optional[Iterable[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Run full Stage 2 quality audit for a single node using one dedicated Session.

    Measures exit IP + Native/Broadcast origin, Google/Gemini/Cloudflare health,
    optional micro-payload throughput, tier classification, and composite score.
    Returns None if exit is CN or HTTPS connectivity fails.
    """
    with create_probe_session(proxy_url) as node_s:
        ip_info = query_exit_ip_info(
            proxy_url,
            ipapi_key=ipapi_key,
            session=node_s,
            relay_proxy_url=relay_proxy_url,
            enable_rdap_fallback=enable_rdap_fallback,
            timeout=probe_timeout,
            ip_cache_file=ip_cache_file,
            ip_cache_ttl_sec=ip_cache_ttl_sec,
            ip_cache_max_entries=ip_cache_max_entries,
            ipapi_url=ipapi_url,
            ip_api_url=ip_api_url,
            ipwho_url=ipwho_url,
            rdap_base_url=rdap_base_url,
        )

        if not ip_info:
            inferred = fallback_cc_fn(str(proxy.get("name", ""))) if fallback_cc_fn else "OTHER"
            cc = _normalize_cc(inferred) or "OTHER"
            ip_info = {
                "cc": cc,
                "isp": "Unknown",
                "org": "",
                "as": "",
                "hosting": None,
                "proxy": None,
                "ip_origin": "未知",
                "ip_origin_reason": "ip_lookup_failed",
            }
        else:
            cc = _normalize_cc(ip_info.get("cc")) or "OTHER"

        if cc == "CN":
            return None

        conn = check_google_and_connectivity(
            node_s,
            timeout=probe_timeout,
            check_openai=check_openai,
            google_204_url=google_204_url,
            google_search_url=google_search_url,
            gemini_url=gemini_url,
            cloudflare_url=cloudflare_url,
            youtube_url=youtube_url,
        )
        if not conn["https_ok"]:
            return None

        speed_kbps = 0.0
        if enable_speedtest:
            sp = measure_throughput_kbps(
                node_s,
                nbytes=speedtest_bytes,
                timeout=speedtest_timeout,
                urls=speedtest_urls,
            )
            speed_kbps = float(sp["speed_kbps"])

    ip_origin = str(ip_info.get("ip_origin", "未知"))
    tier = classify_node_tier(
        ip_info,
        int(delay_ms),
        jitter_ms=int(jitter_ms),
        ip_origin=ip_origin,
        google_clean=conn["google_clean"],
        speed_kbps=speed_kbps,
    )
    quality_score = compute_quality_score(
        tier=tier,
        ip_origin=ip_origin,
        delay_ms=int(delay_ms),
        jitter_ms=int(jitter_ms),
        loss_rate=float(loss_rate),
        speed_kbps=speed_kbps,
        google_clean=conn["google_clean"],
        gemini_clean=conn["gemini_clean"],
        proxy_type=str(proxy.get("type", "")),
        ip_info=ip_info,
    )

    return {
        "cc": cc,
        "tier": tier,
        "ip_origin": ip_origin,
        "ip_origin_reason": str(ip_info.get("ip_origin_reason", "")),
        "delay": int(delay_ms),
        "delay_ms": int(delay_ms),
        "jitter_ms": int(jitter_ms),
        "loss_rate": float(loss_rate),
        "speed_kbps": round(speed_kbps, 1),
        "google_clean": bool(conn["google_clean"]),
        "gemini_clean": bool(conn["gemini_clean"]),
        "cloudflare_ok": bool(conn["cloudflare_ok"]),
        "openai_reachable": conn.get("openai_reachable"),
        "google_reason": str(conn.get("reason", "")),
        "quality_score": quality_score,
        "ip_info": ip_info,
        "proxy": proxy,
    }


def score_and_rank_nodes(
    verified_records: List[Dict[str, Any]],
    *,
    max_nodes: Optional[int] = None,
    cc_region_names: Optional[Dict[str, str]] = None,
    include_origin_in_name: bool = True,
) -> List[Dict[str, Any]]:
    """
    Score, globally rank, optionally cap, and deterministically rename qualified
    nodes for a single flat PROXY + 漏网之鱼 Clash configuration.
    """
    region_map = cc_region_names or DEFAULT_CC_REGION_NAMES
    valid = [dict(r) for r in verified_records if r.get("cc") != "CN"]

    for rec in valid:
        if "quality_score" not in rec:
            rec["quality_score"] = compute_quality_score(
                tier=str(rec.get("tier", "普通")),
                ip_origin=str(rec.get("ip_origin", "未知")),
                delay_ms=int(rec.get("delay_ms", rec.get("delay", 9999))),
                jitter_ms=int(rec.get("jitter_ms", 0)),
                loss_rate=float(rec.get("loss_rate", 0.0)),
                speed_kbps=float(rec.get("speed_kbps", 0.0)),
                google_clean=rec.get("google_clean"),
                gemini_clean=rec.get("gemini_clean"),
                proxy_type=str((rec.get("proxy") or {}).get("type", "")),
                ip_info=rec.get("ip_info"),
            )

    valid.sort(
        key=lambda x: (
            -float(x.get("quality_score", 0.0)),
            TIER_PRIORITY.get(str(x.get("tier", "普通")), 9),
            ORIGIN_PRIORITY.get(str(x.get("ip_origin", "未知")), 9),
            int(x.get("delay_ms", x.get("delay", 9999))),
            int(x.get("jitter_ms", 0)),
        )
    )

    if max_nodes is not None and max_nodes > 0:
        valid = valid[:max_nodes]

    final_proxies: List[Dict[str, Any]] = []
    naming_counter: Counter = Counter()

    for item in valid:
        cc = str(item.get("cc", "OTHER"))
        tier = str(item.get("tier", "普通"))
        origin = str(item.get("ip_origin", "未知"))
        tier_label = f"{tier}·{origin}" if (include_origin_in_name and origin in ("原生", "广播")) else tier

        naming_counter[(cc, tier_label)] += 1
        seq = naming_counter[(cc, tier_label)]
        cc_cn = region_map.get(cc, cc)
        new_name = f"{cc_cn} | {tier_label} | {seq:02d}"

        node_dict = dict(item["proxy"])
        node_dict["name"] = new_name

        out = dict(item)
        out["delay"] = int(item.get("delay", item.get("delay_ms", 0)))
        out["proxy"] = node_dict
        final_proxies.append(out)

    return final_proxies


# ======================== 3. Rolling Update Push Gate ========================

def _extract_state_snapshot(
    final_records: List[Dict[str, Any]],
    *,
    top_n: int = 5,
    get_fingerprint_fn: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> Dict[str, Any]:
    fp_fn = get_fingerprint_fn or proxy_fingerprint
    fps: List[str] = []
    seen: Set[str] = set()
    high_tier_fps: List[str] = []

    for rec in final_records:
        proxy = rec.get("proxy") if "proxy" in rec else rec
        if not isinstance(proxy, dict):
            continue
        fp = fp_fn(proxy)
        if fp not in seen:
            seen.add(fp)
            fps.append(fp)
        if rec.get("tier") == "纯净" or rec.get("ip_origin") == "原生":
            if fp not in high_tier_fps:
                high_tier_fps.append(fp)

    canonical = json.dumps(sorted(fps), ensure_ascii=True, separators=(",", ":"))
    proxies_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]

    return {
        "node_count": len(fps),
        "fingerprints": fps,
        "top_fingerprints": fps[:max(1, top_n)],
        "high_tier_fingerprints": high_tier_fps,
        "proxies_hash": proxies_hash,
    }


def load_push_state(state_file: Path = DEFAULT_PUSH_STATE_FILE) -> Optional[Dict[str, Any]]:
    """Load persisted Git push state JSON, returning None if absent or invalid."""
    if not state_file.exists():
        return None
    try:
        with state_file.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception as exc:
        log.warning("Failed to read push state (%s): %s", state_file, exc)
    return None


def should_push_rolling_update(
    final_records: List[Dict[str, Any]],
    *,
    state_file: Path = DEFAULT_PUSH_STATE_FILE,
    now_ts: Optional[float] = None,
    max_staleness_sec: float = 21600.0,
    min_count_delta: int = 3,
    count_delta_ratio: float = 0.15,
    turnover_threshold: float = 0.20,
    top_n: int = 5,
    high_tier_delta_threshold: int = 2,
    get_fingerprint_fn: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Decide whether the latest run warrants a Git commit/push + CDN purge.

    Local historical state (saved_working_nodes.yaml & historical_meta.json)
    should still be updated every run regardless of this gate's decision.
    """
    if not final_records:
        return (False, "empty_final_records", {"node_count": 0})

    curr_ts = float(time.time() if now_ts is None else now_ts)
    curr = _extract_state_snapshot(final_records, top_n=top_n, get_fingerprint_fn=get_fingerprint_fn)

    prev = load_push_state(state_file)
    initial_summary = {
        "prev_count": 0,
        "curr_count": curr["node_count"],
        "turnover_ratio": 1.0,
        "high_tier_count": len(curr["high_tier_fingerprints"]),
        "proxies_hash": curr["proxies_hash"],
    }
    if (
        not prev
        or not isinstance(prev.get("fingerprints"), list)
        or not prev.get("fingerprints")
        or not prev.get("last_push_ts")
    ):
        return (True, "initial_push_no_state", initial_summary)

    try:
        prev_ts = float(prev.get("last_push_ts", 0.0))
        if not math.isfinite(prev_ts) or prev_ts <= 0.0:
            raise ValueError("invalid_prev_ts")
    except (TypeError, ValueError):
        return (True, "initial_push_invalid_state", initial_summary)

    elapsed = max(0.0, curr_ts - prev_ts)

    prev_fps = {str(fp) for fp in prev.get("fingerprints", []) if fp}
    curr_fps = set(curr["fingerprints"])
    prev_count = len(prev_fps)
    curr_count = len(curr_fps)
    if prev_count == 0:
        return (True, "initial_push_empty_fps", initial_summary)

    union_cnt = len(curr_fps | prev_fps)
    sym_diff_cnt = len(curr_fps ^ prev_fps)
    turnover = (sym_diff_cnt / union_cnt) if union_cnt > 0 else 0.0

    prev_top_raw = prev.get("top_fingerprints", [])
    prev_top = list(prev_top_raw) if isinstance(prev_top_raw, list) else []
    curr_top = list(curr["top_fingerprints"])
    prev_high_raw = prev.get("high_tier_fingerprints", [])
    prev_high = {str(fp) for fp in prev_high_raw if fp} if isinstance(prev_high_raw, list) else set()
    curr_high = set(curr["high_tier_fingerprints"])
    high_churn = len(curr_high ^ prev_high)

    metrics: Dict[str, Any] = {
        "prev_count": prev_count,
        "curr_count": curr_count,
        "turnover_ratio": round(turnover, 4),
        "sym_diff_count": sym_diff_cnt,
        "high_tier_churn": high_churn,
        "elapsed_sec": int(round(elapsed)),
        "proxies_hash": curr["proxies_hash"],
    }

    if elapsed >= max_staleness_sec:
        return (True, f"max_staleness_exceeded({int(elapsed)}s>={int(max_staleness_sec)}s)", metrics)

    count_threshold = max(min_count_delta, int(math.floor(count_delta_ratio * prev_count)))
    if abs(curr_count - prev_count) >= count_threshold:
        return (True, f"node_count_delta({prev_count}->{curr_count}>={count_threshold})", metrics)

    if turnover >= turnover_threshold:
        return (True, f"turnover_ratio({turnover:.1%}>={turnover_threshold:.0%})", metrics)

    if set(curr_top) != set(prev_top) or (prev_top and curr_top and curr_top[0] != prev_top[0]):
        return (True, f"top{top_n}_quality_set_changed", metrics)

    if high_churn >= high_tier_delta_threshold:
        return (True, f"high_tier_churn({high_churn}>={high_tier_delta_threshold})", metrics)

    return (
        False,
        f"below_push_threshold(turnover={turnover:.1%},count={prev_count}->{curr_count},age={int(elapsed)}s)",
        metrics,
    )


def save_push_state(
    final_records: List[Dict[str, Any]],
    *,
    state_file: Path = DEFAULT_PUSH_STATE_FILE,
    now_ts: Optional[float] = None,
    top_n: int = 5,
    get_fingerprint_fn: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> Path:
    """Atomically persist the latest pushed node snapshot to push_state.json."""
    curr_ts = float(time.time() if now_ts is None else now_ts)
    snap = _extract_state_snapshot(final_records, top_n=top_n, get_fingerprint_fn=get_fingerprint_fn)
    payload = {
        "last_push_ts": curr_ts,
        "last_push_utc": datetime.fromtimestamp(curr_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        **snap,
    }

    return _atomic_write_json(state_file, payload)
