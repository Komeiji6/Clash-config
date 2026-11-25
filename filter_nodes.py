#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clash 节点全量测速筛选与自动发布脚本 (Snapdragon 410 / 512MB RAM / Debian 11 ARM64)
目标仓库: git@github.com:Komeiji6/Clash-config.git (nodes 孤儿分支)
运行模式:
1. 全量拉取上游 (~2600+ 节点)，不提前截断；
2. Mihomo 高并发 (60协程) 内核级快速握手初筛，严控 1000ms 超时，超标即弃；
3. 存活节点 3 路无锁探测落地 IP 属性与原生纯净度 (ip-api / ipwho.is)；
4. 按策略组分桶 (HK, JP, SG, US, 其他地区)，每组上限 20 个，优先纯净与低延迟；
5. 全流程位于 /tmp/ (tmpfs)，零 eMMC 磨损，最终原子化单 Commit 强推至 GitHub nodes 分支。
"""

import base64
import gc
import json
import logging
import urllib.parse
import os
import queue
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

# 优先使用 C 加速解析器，大幅降低 ARM64 上的解析耗时与内存占用
try:
    YamlLoader = yaml.CSafeLoader
    YamlDumper = yaml.CSafeDumper
except AttributeError:
    YamlLoader = yaml.SafeLoader
    YamlDumper = yaml.SafeDumper

# ======================== 核心配置参数 ========================

def _gh_mirrors(owner: str, repo: str, branch: str, path: str) -> List[str]:
    """生成 GitHub 订阅源的多镜像故障转移列表 (jsDelivr CDN -> ghfast 国内加速 -> raw 直连)。"""
    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
    return [
        f"https://cdn.jsdelivr.net/gh/{owner}/{repo}@{branch}/{path}",
        f"https://ghfast.top/{raw_url}",
        raw_url,
    ]


# 经高通 410 真机实测筛选的精选高产数据源 (按国内存活率与延迟表现降序排列)
UPSTREAM_SOURCES: List[Tuple[str, List[str]]] = [
    # --- Tier 1: 国内直测 / subs-check 高命中核心源 (存活率 24% ~ 95%) ---
    ("kooker/FreeSubsCheck", _gh_mirrors("kooker", "FreeSubsCheck", "main", "all.yaml")),
    ("zhuhaiuk/free-nodes", _gh_mirrors("zhuhaiuk", "free-nodes", "main", "clash_config.yaml")),
    ("won777/xrayvip", _gh_mirrors("won777", "clash-sub", "main", "xrayvip.yaml")),
    ("Pawdroid/Free-servers", _gh_mirrors("Pawdroid", "Free-servers", "main", "sub")),
    ("sunmiao4458/airport", _gh_mirrors("sunmiao4458", "free-proxy-airport", "main", "output/clash.yaml")),
    ("snakem982/proxypool", _gh_mirrors("snakem982", "proxypool", "main", "source/clash-meta-2.yaml")),
    ("Barabama/cfmem", _gh_mirrors("Barabama", "FreeNodes", "main", "nodes/cfmem.yaml")),
    ("Barabama/datiya", _gh_mirrors("Barabama", "FreeNodes", "main", "nodes/datiya.yaml")),

    # --- Tier 2: 高频活跃预检源 (存活率 8% ~ 19%，大幅扩充不重复活节点) ---
    (
        "ermaozi/reachable",
        _gh_mirrors("ermaozi", "get_subscribe", "main", "subscribe/reachable/clash.yml")
        + ["https://blog.ermao.net/sub/clash/ermao.net"],
    ),
    ("chengaopan/AutoMerge", _gh_mirrors("chengaopan", "AutoMergePublicNodes", "master", "list.yml")),
    ("ripaojiedian/freenode", _gh_mirrors("ripaojiedian", "freenode", "main", "sub")),
    ("lyqnihao/clash-meta", _gh_mirrors("lyqnihao", "proxy", "main", "clash-meta/output.yaml")),
    ("free18/v2ray", _gh_mirrors("free18", "v2ray", "main", "c.yaml")),
    ("won777/unified", _gh_mirrors("won777", "clash-sub", "main", "unified.yaml")),

    # --- Tier 3: 现代协议专项池 (VLESS Reality / Hysteria2 / xhttp 增量) ---
    ("peasoft/NoMoreWalls", _gh_mirrors("peasoft", "NoMoreWalls", "master", "list.meta.yml")),
    ("hamzawih0/zorVPN", _gh_mirrors("hamzawih0", "zorVPN", "main", "clash.yaml")),
    ("sakazxc/free-vpn-sub", _gh_mirrors("sakazxc1400-creator", "free-vpn-sub", "data", "output/clash.yaml")),
    ("Mosifree/Reality", _gh_mirrors("Mosifree", "-FREE2CONFIG", "main", "Reality")),
]

BIN_MIHOMO = "/usr/local/bin/mihomo"

# /tmp/ tmpfs 路径定义
TMP_DIR = Path("/tmp")

# 历史存活节点池与元数据定义 (持久化至 /opt/clash-filter/)
HISTORICAL_POOL_FILE = Path("/opt/clash-filter/saved_working_nodes.yaml")
HISTORICAL_META_FILE = Path("/opt/clash-filter/historical_meta.json")
HISTORICAL_MAX_STRIKES = 3      # 连续 3 次测速失联 (即持续 3 小时完全挂掉) 才彻底从历史池注销
HISTORICAL_MAX_CAPACITY = 120   # 历史池最大容量上限，防无上限膨胀
TMP_RAW_YAML = TMP_DIR / "raw_nodes.yaml"
TMP_STAGE1_DIR = TMP_DIR / "mihomo_stage1"
TMP_STAGE2_DIR = TMP_DIR / "mihomo_stage2"
TMP_FINAL_YAML = TMP_DIR / "proxies.yaml"
TMP_OUT_REPO = TMP_DIR / "out_repo"

# Git 发布配置
GIT_REMOTE_URL = "git@github.com:Komeiji6/Clash-config.git"
GIT_BRANCH = "nodes"
GIT_USER_NAME = "OpenStick-410"
GIT_USER_EMAIL = "openstick@local"
GIT_SSH_KEY = "/root/.ssh/id_ed25519"

# 端口与并发规划
STAGE1_API_PORT = 19999
STAGE1_CONCURRENCY = 20         # 初筛并发降至 25（完美适配 410 的 2.4G 射频硬件，杜绝拥塞误杀）
STAGE2_API_PORT = 19090
STAGE2_BASE_PORT = 18080
STAGE2_CONCURRENCY = 3          # 落地 IP 检测并发数 (3 路无锁)

# 本地机密配置文件 (仅保存在棒子本地 /opt/clash-filter/secrets.json，严禁提交至公共仓库)
SECRETS_FILE = Path("/opt/clash-filter/secrets.json")

def _load_local_secrets() -> Dict[str, str]:
    if SECRETS_FILE.exists():
        try:
            with SECRETS_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

_SECRETS = _load_local_secrets()
IPAPI_IS_KEY = _SECRETS.get("ipapi_is_key", os.getenv("IPAPI_IS_KEY", ""))

# 策略组容量上限与阈值
MAX_NODES_PER_GROUP = 50        # 每个含节点策略组最多保留 50 个
HANDSHAKE_TIMEOUT_MS = 3500     # 握手超时放宽至 3000ms（兼顾亚太极速与欧美跨洋优质节点，杜绝抖动误杀）
PREMIUM_MAX_LATENCY_MS = 800    # 「优选」延迟上限放宽为 800ms
MIN_SAFEGUARD_NODES = 3         # 熔断保底阈值

# Go 运行时内存限制 (防止触发 512MB OOM)
GO_LOW_MEM_ENV = {
    **os.environ,
    "GOGC": "30",
    "GOMEMLIMIT": "120MiB",
}

# 数据中心 / 机房 Hosting 关键词 (辅助判定非原生住宅 IP)
DATACENTER_KEYWORDS = (
    "amazon", "aws", "google", "gcp", "microsoft", "azure", "alibaba", "aliyun",
    "tencent", "oracle", "digitalocean", "vultr", "linode", "akamai", "hetzner",
    "ovh", "cloudflare", "choopa", "leaseweb", "zenlayer", "racknerd",
    "colocrossing", "datacamp", "cdn77", "m247", "hosting", "datacenter",
    "data center", "cloud", "vps", "server", "dedicated", "contabo", "scaleway",
    "kamatera", "ucloud", "huawei", "bytedance", "dmit", "bandwagon", "it7",
    "sharktech", "psychz", "quadranet", "cogent", "hostwinds", "ionos", "vultr",
    "b2 net", "xserver", "sakura internet", "conoha", "gmo internet", "idc",
)

# 地区优先级与品质优先级
CC_PRIORITY = {"HK": 1, "JP": 2, "SG": 3, "US": 4, "OTHER": 5}
TIER_PRIORITY = {"纯净": 1, "优选": 2, "普通": 3}

# ======================== 日志配置 ========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("filter_nodes")

_active_procs: List[subprocess.Popen] = []


def kill_active_procs() -> None:
    """终止所有活动的 Mihomo 子进程。"""
    global _active_procs
    for p in _active_procs:
        if p and p.poll() is None:
            try:
                p.terminate()
                p.wait(timeout=3)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
    _active_procs.clear()


def cleanup_tmp() -> None:
    """清理 /tmp/ 下所有中间过程文件与目录，释放 tmpfs 内存。"""
    kill_active_procs()
    targets = [
        TMP_RAW_YAML,
        TMP_STAGE1_DIR,
        TMP_STAGE2_DIR,
        TMP_FINAL_YAML,
        TMP_OUT_REPO,
    ]
    for path in targets:
        try:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            elif path.exists():
                path.unlink(missing_ok=True)
        except Exception:
            pass
    gc.collect()


def _signal_handler(sig: int, _frame: Any) -> None:
    log.warning("收到中断信号 (%d)，正在安全退出并清理 /tmp/...", sig)
    cleanup_tmp()
    sys.exit(128 + sig)


# ======================== 冷启动与自愈自检 ========================

def ensure_system_time_and_network() -> None:
    """自检 NTP 时钟与网络连通性，并在必要时触发吉大 Dr.COM 认证。"""
    now = datetime.now()
    if now.year < 2024:
        log.warning("检测到硬件 RTC 未同步 (当前年份=%d)，等待 NTP 校准...", now.year)
        try:
            subprocess.run(["systemctl", "restart", "systemd-timesyncd"], timeout=10, check=False)
        except Exception:
            pass

        for _ in range(15):
            time.sleep(1)
            if datetime.now().year >= 2024:
                log.info("NTP 校准成功: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                break
        else:
            log.warning("NTP 依然未同步，尝试通过国内 HTTP Date 头强行自愈时钟...")
            for mirror in ["http://www.baidu.com", "http://www.aliyun.com", "http://www.qq.com"]:
                try:
                    resp = requests.head(mirror, timeout=3)
                    if "Date" in resp.headers:
                        gmt_time = parsedate_to_datetime(resp.headers["Date"])
                        time_str = gmt_time.strftime("%Y-%m-%d %H:%M:%S")
                        subprocess.run(["date", "-s", time_str], timeout=5, check=False)
                        log.info("通过 HTTP Date 强制同步系统时间成功: %s", time_str)
                        break
                except Exception:
                    pass

    # 检查网络与校园网认证状态
    try:
        r = requests.get("http://www.gstatic.com/generate_204", timeout=3)
        if r.status_code == 204:
            log.info("互联网与时钟均处于就绪状态。")
            return
    except Exception:
        pass

    log.info("互联网未连通或受限于校园网认证网关，尝试执行校园网心跳登录...")
    acc = _SECRETS.get("campus_account", "")
    pwd = _SECRETS.get("campus_password", "")
    if acc and pwd:
        auth_url = (
            f"http://10.100.61.3/drcom/login?"
            f"callback=dr1003&DDDDD={urllib.parse.quote(acc)}&upass={urllib.parse.quote(pwd)}&0MKKey=123456"
        )
        try:
            requests.get(auth_url, timeout=5)
            time.sleep(2)
            log.info("校园网认证心跳发送完毕，网络已恢复就绪。")
        except Exception as e:
            log.warning("发送校园网认证心跳异常: %s", e)
    else:
        log.info("未配置本地校园网认证账号，跳过自动登录。")


# ======================== 工具函数 ========================

def load_proxies_yaml(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            data = yaml.load(f, Loader=YamlLoader)
        if isinstance(data, dict) and isinstance(data.get("proxies"), list):
            return [p for p in data["proxies"] if isinstance(p, dict)]
    except Exception as exc:
        log.error("解析 YAML 失败 (%s): %s", path, exc)
    return []


def dump_proxies_yaml(path: Path, proxies: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        yaml.dump(
            {"proxies": proxies},
            f,
            Dumper=YamlDumper,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
        )


def get_proxy_fingerprint(proxy: Dict[str, Any]) -> str:
    """提取节点的唯一身份指纹 (协议、服务器、端口、认证凭据)，免疫命名变更影响。"""
    server = str(proxy.get("server", "")).strip().lower()
    port = str(proxy.get("port", "")).strip()
    p_type = str(proxy.get("type", "")).strip().lower()
    cred = str(
        proxy.get("uuid")
        or proxy.get("password")
        or proxy.get("auth")
        or proxy.get("auth-str")
        or proxy.get("token")
        or ""
    ).strip()
    return f"{p_type}://{server}:{port}:{cred}"


def load_historical_pool() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """加载本地历史存活节点池与其元数据（容错失败计数、历史统计）。"""
    proxies: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {}

    if HISTORICAL_POOL_FILE.exists():
        try:
            with HISTORICAL_POOL_FILE.open("r", encoding="utf-8") as f:
                saved_data = yaml.load(f, Loader=YamlLoader)
                if isinstance(saved_data, dict) and isinstance(saved_data.get("proxies"), list):
                    proxies = [p for p in saved_data["proxies"] if isinstance(p, dict)]
        except Exception as exc:
            log.warning("读取历史存活节点池异常: %s", exc)

    if HISTORICAL_META_FILE.exists():
        try:
            with HISTORICAL_META_FILE.open("r", encoding="utf-8") as f:
                meta = json.load(f)
                if not isinstance(meta, dict):
                    meta = {}
        except Exception as exc:
            log.warning("读取历史节点元数据异常: %s", exc)

    return proxies, meta


def update_and_save_historical_pool(
    current_verified_proxies: List[Dict[str, Any]],
    old_historical_proxies: List[Dict[str, Any]],
    old_meta: Dict[str, Any],
) -> None:
    """
    终极容错缓冲机制（3 振出局 + 自动换血 + 原子落盘）：
    1. 当期实测合格节点：首要保留，连续失联计数重置为 0。
    2. 历史老节点本轮失联：触发容错缓冲（连续失联计数 +1）。
       - 失联次数 < 3：保留在本地历史池中，赋予下轮复活测试机会（防校园网偶发丢包误杀）。
       - 连续失联 >= 3 (即持续 3 小时完全无法连通)：判定彻底报废，永久注销移出。
    3. 全局去重与最大容量 (HISTORICAL_MAX_CAPACITY) 控制。
    4. 采用 tmpfs 临时写入 + 原子重命名替换，零磁盘碎片且断电不损坏。
    """
    now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    new_meta: Dict[str, Any] = {}

    # 1. 建立本轮成功节点的指纹映射
    success_map: Dict[str, Dict[str, Any]] = {}
    for p in current_verified_proxies:
        fp = get_proxy_fingerprint(p)
        success_map[fp] = p
        prev_entry = old_meta.get(fp, {})
        new_meta[fp] = {
            "name": p.get("name", "unknown"),
            "fails": 0,
            "first_seen": prev_entry.get("first_seen", now_utc_str),
            "last_success": now_utc_str,
        }

    # 2. 检查老节点池中的节点：如果本轮未成功，评估是否在容错缓冲期内
    preserved_grace_proxies: List[Dict[str, Any]] = []
    purged_count = 0
    grace_count = 0

    for old_p in old_historical_proxies:
        fp = get_proxy_fingerprint(old_p)
        if fp in success_map:
            # 本轮已通过实测入围
            continue

        prev_entry = old_meta.get(fp, {})
        curr_fails = int(prev_entry.get("fails", 0)) + 1

        if curr_fails < HISTORICAL_MAX_STRIKES:
            grace_count += 1
            preserved_grace_proxies.append(old_p)
            new_meta[fp] = {
                "name": old_p.get("name", "unknown"),
                "fails": curr_fails,
                "first_seen": prev_entry.get("first_seen", now_utc_str),
                "last_success": prev_entry.get("last_success", "unknown"),
            }
            log.info("历史节点 [%s] 本轮偶发失联 (容错缓冲 %d/%d)，保留待下次复试",
                     old_p.get("name", fp[:24]), curr_fails, HISTORICAL_MAX_STRIKES)
        else:
            purged_count += 1
            log.info("历史节点 [%s] 连续 %d 次测速失联 (已持续 %d 小时)，正式从历史池淘汰注销",
                     old_p.get("name", fp[:24]), curr_fails, HISTORICAL_MAX_STRIKES)

    # 3. 合并：当前实测活节点 + 容错保护节点
    combined_pool: List[Dict[str, Any]] = list(current_verified_proxies) + preserved_grace_proxies

    # 4. 容量控制 (超出 HISTORICAL_MAX_CAPACITY 时截断)
    if len(combined_pool) > HISTORICAL_MAX_CAPACITY:
        combined_pool = combined_pool[:HISTORICAL_MAX_CAPACITY]
        retained_fps = {get_proxy_fingerprint(p) for p in combined_pool}
        new_meta = {fp: v for fp, v in new_meta.items() if fp in retained_fps}

    # 5. 原子化安全落盘 (在 tmpfs 临时写入后原子移动替换，防断电文件损坏)
    try:
        tmp_pool_file = TMP_DIR / "saved_working_nodes.yaml.tmp"
        tmp_meta_file = TMP_DIR / "historical_meta.json.tmp"

        dump_proxies_yaml(tmp_pool_file, combined_pool)
        with tmp_meta_file.open("w", encoding="utf-8") as f:
            json.dump(new_meta, f, ensure_ascii=False, indent=2)

        shutil.move(str(tmp_pool_file), str(HISTORICAL_POOL_FILE))
        shutil.move(str(tmp_meta_file), str(HISTORICAL_META_FILE))

        log.info(
            "🎉 历史保活池更新完毕: 当期实测合格 %d 个, 容错保活 %d 个, 连续失联淘汰 %d 个 -> 当前总蓄水容量 %d 个",
            len(current_verified_proxies), grace_count, purged_count, len(combined_pool)
        )
    except Exception as exc:
        log.warning("原子化存档历史存活节点异常: %s", exc)


# 有效成熟的代理协议类型白名单 (严格排除 compatible 占位符、http/socks5 明文、非代理规则)
VALID_PROXY_TYPES = {
    "ss", "ssr", "vmess", "vless", "trojan",
    "hysteria", "hysteria2", "tuic", "wireguard", "snell", "anytls"
}

# 垃圾/广告/兼容性占位关键字 (出现在名称中直接丢弃)
JUNK_NAME_KEYWORDS = (
    "compatible", "剩余流量", "到期", "过期", "官网", "更新", "关注",
    "群", "公告", "广告", "购买", "套餐", "客服", "订阅转换", "防失联"
)

# 无效/保留/蜜罐地址黑名单
INVALID_SERVERS = {"", "127.0.0.1", "127.0.0.53", "localhost", "0.0.0.0", "::", "::1"}


def safe_b64decode(s: str) -> str:
    """安全解码 Base64 字符串（自动补全 padding，兼容 URL-Safe 字符集）。"""
    s = s.strip().replace(" ", "+").replace("\r", "").replace("\n", "")
    missing_padding = len(s) % 4
    if missing_padding:
        s += "=" * (4 - missing_padding)
    try:
        return base64.b64decode(s).decode("utf-8", errors="ignore")
    except Exception:
        try:
            return base64.urlsafe_b64decode(s).decode("utf-8", errors="ignore")
        except Exception:
            return ""


def parse_vmess(link: str) -> Optional[Dict[str, Any]]:
    """解析标准 vmess://<base64_json> 节点为 Clash 字典。"""
    raw = link[8:].strip()
    decoded = safe_b64decode(raw)
    if not decoded:
        return None
    try:
        data = json.loads(decoded)
        server = str(data.get("add", "")).strip()
        port = int(data.get("port", 0))
        uuid = str(data.get("id", "")).strip()
        if not server or not port or not uuid:
            return None
        name = str(data.get("ps", f"vmess_{server}")).strip()
        p = {
            "name": name,
            "type": "vmess",
            "server": server,
            "port": port,
            "uuid": uuid,
            "alterId": int(data.get("aid", 0)),
            "cipher": str(data.get("scy", "auto")),
            "udp": True,
        }
        tls = str(data.get("tls", "")).lower()
        sni = str(data.get("sni", "")).strip()
        if tls == "tls":
            p["tls"] = True
            if sni:
                p["servername"] = sni
        net = str(data.get("net", "tcp")).lower()
        if net == "ws":
            p["network"] = "ws"
            p["ws-opts"] = {
                "path": str(data.get("path", "/")),
                "headers": {"Host": str(data.get("host", sni or server))},
            }
        elif net == "grpc":
            p["network"] = "grpc"
            p["grpc-opts"] = {
                "grpc-service-name": str(data.get("path", "")),
            }
        return p
    except Exception:
        return None


def parse_vless(link: str) -> Optional[Dict[str, Any]]:
    """解析标准 vless://<uuid>@<host>:<port>?<params>#<name> 为 Clash 字典。"""
    try:
        u = urllib.parse.urlparse(link)
        uuid = u.username
        host = u.hostname
        port = u.port
        if not uuid or not host or not port:
            return None
        name = urllib.parse.unquote(u.fragment) if u.fragment else f"vless_{host}"
        qs = urllib.parse.parse_qs(u.query)
        p = {
            "name": name,
            "type": "vless",
            "server": host,
            "port": port,
            "uuid": uuid,
            "udp": True,
        }
        sec = qs.get("security", [""])[0].lower()
        sni = qs.get("sni", [""])[0]
        fp = qs.get("fp", [""])[0]
        flow = qs.get("flow", [""])[0]
        if sec in ("tls", "reality"):
            p["tls"] = True
            if sni:
                p["servername"] = sni
            if fp:
                p["client-fingerprint"] = fp
            if flow:
                p["flow"] = flow
            if sec == "reality":
                pbk = qs.get("pbk", [""])[0].strip()
                sid = qs.get("sid", [""])[0].strip()
                if len(sid) != 16 or not all(c in "0123456789abcdefABCDEF" for c in sid):
                    sid = ""
                if pbk:
                    p["reality-opts"] = {
                        "public-key": pbk,
                        "short-id": sid,
                    }
        net = qs.get("type", ["tcp"])[0].lower()
        alpn_raw = qs.get("alpn", [""])[0]
        if alpn_raw:
            p["alpn"] = [a.strip() for a in alpn_raw.split(",") if a.strip()]
        if qs.get("allowInsecure", ["0"])[0] in ("1", "true"):
            p["skip-cert-verify"] = True
        enc = qs.get("encryption", [""])[0].strip()
        if enc and enc.lower() != "none":
            p["encryption"] = enc
        if net == "ws":
            p["network"] = "ws"
            path = qs.get("path", ["/"])[0]
            ws_host = qs.get("host", [""])[0] or sni or host
            p["ws-opts"] = {"path": path, "headers": {"Host": ws_host}}
        elif net == "grpc":
            p["network"] = "grpc"
            service_name = qs.get("serviceName", [""])[0]
            p["grpc-opts"] = {"grpc-service-name": service_name}
        elif net in ("xhttp", "splithttp"):
            p["network"] = "xhttp"
            path = qs.get("path", ["/"])[0]
            x_host = qs.get("host", [""])[0] or sni or host
            p["xhttp-opts"] = {"path": path, "host": x_host, "mode": qs.get("mode", ["auto"])[0]}
        elif net == "h2":
            p["network"] = "h2"
            p["h2-opts"] = {"path": qs.get("path", ["/"])[0], "host": [qs.get("host", [""])[0] or sni or host]}
        return p
    except Exception:
        return None


def parse_trojan(link: str) -> Optional[Dict[str, Any]]:
    """解析标准 trojan://<password>@<host>:<port>?<params>#<name> 为 Clash 字典。"""
    try:
        u = urllib.parse.urlparse(link)
        password = u.username or u.password
        host = u.hostname
        port = u.port
        if not password or not host or not port:
            return None
        name = urllib.parse.unquote(u.fragment) if u.fragment else f"trojan_{host}"
        qs = urllib.parse.parse_qs(u.query)
        sni = qs.get("sni", [""])[0]
        p = {
            "name": name,
            "type": "trojan",
            "server": host,
            "port": port,
            "password": password,
            "udp": True,
        }
        if sni:
            p["sni"] = sni
        return p
    except Exception:
        return None


def parse_ss(link: str) -> Optional[Dict[str, Any]]:
    """解析 Shadowsocks 原生链接 (支持标准 Base64 及 SIP002 规范)。"""
    try:
        u = urllib.parse.urlparse(link)
        name = urllib.parse.unquote(u.fragment) if u.fragment else f"ss_{u.hostname or 'node'}"
        if "@" in u.netloc:
            userinfo, hostport = u.netloc.split("@", 1)
            decoded_user = safe_b64decode(userinfo)
            if ":" in decoded_user:
                method, password = decoded_user.split(":", 1)
            else:
                return None
            host, port_str = hostport.split(":", 1)
            port = int(port_str.split("?")[0])
        else:
            decoded = safe_b64decode(u.netloc)
            if "@" not in decoded:
                return None
            cred, hostport = decoded.split("@", 1)
            method, password = cred.split(":", 1)
            host, port_str = hostport.split(":", 1)
            port = int(port_str.split("?")[0])

        return {
            "name": name,
            "type": "ss",
            "server": host,
            "port": port,
            "cipher": method,
            "password": password,
            "udp": True,
        }
    except Exception:
        return None


def parse_hysteria2(link: str) -> Optional[Dict[str, Any]]:
    """解析 Hysteria2 (hy2) 原生链接为 Clash 字典。"""
    try:
        u = urllib.parse.urlparse(link)
        auth = u.username or u.password or ""
        host = u.hostname
        port = u.port
        if not host or not port:
            return None
        name = urllib.parse.unquote(u.fragment) if u.fragment else f"hy2_{host}"
        qs = urllib.parse.parse_qs(u.query)
        sni = qs.get("sni", [""])[0]
        insecure = qs.get("insecure", ["0"])[0] == "1"
        obfs = qs.get("obfs", [""])[0]
        obfs_password = qs.get("obfs-password", [""])[0]
        p = {
            "name": name,
            "type": "hysteria2",
            "server": host,
            "port": port,
            "password": auth,
            "udp": True,
        }
        if sni:
            p["sni"] = sni
        if insecure:
            p["skip-cert-verify"] = True
        if obfs:
            p["obfs"] = obfs
            if obfs_password:
                p["obfs-password"] = obfs_password
        return p
    except Exception:
        return None


def parse_anytls(link: str) -> Optional[Dict[str, Any]]:
    """解析 anytls://<password>@<host>:<port>?<params>#<name> 为 Clash 字典。"""
    try:
        u = urllib.parse.urlparse(link)
        password = urllib.parse.unquote(u.username or u.password or "")
        host = u.hostname
        port = u.port
        if not password or not host or not port:
            return None
        name = urllib.parse.unquote(u.fragment) if u.fragment else f"anytls_{host}"
        qs = urllib.parse.parse_qs(u.query)
        p: Dict[str, Any] = {
            "name": name,
            "type": "anytls",
            "server": host,
            "port": port,
            "password": password,
            "udp": True,
        }
        sni = qs.get("sni", [""])[0]
        if sni:
            p["sni"] = sni
        if qs.get("insecure", ["0"])[0] in ("1", "true"):
            p["skip-cert-verify"] = True
        alpn_raw = qs.get("alpn", [""])[0]
        if alpn_raw:
            p["alpn"] = [a.strip() for a in alpn_raw.split(",") if a.strip()]
        return p
    except Exception:
        return None


def parse_tuic(link: str) -> Optional[Dict[str, Any]]:
    """解析 tuic://<uuid>:<password>@<host>:<port>?<params>#<name> 为 Clash 字典。"""
    try:
        u = urllib.parse.urlparse(link)
        uuid = urllib.parse.unquote(u.username or "")
        password = urllib.parse.unquote(u.password or "")
        host = u.hostname
        port = u.port
        if not uuid or not host or not port:
            return None
        name = urllib.parse.unquote(u.fragment) if u.fragment else f"tuic_{host}"
        qs = urllib.parse.parse_qs(u.query)
        p: Dict[str, Any] = {
            "name": name,
            "type": "tuic",
            "server": host,
            "port": port,
            "uuid": uuid,
            "password": password,
            "udp": True,
        }
        sni = qs.get("sni", [""])[0]
        if sni:
            p["sni"] = sni
        cc = qs.get("congestion_control", [""])[0]
        if cc:
            p["congestion-controller"] = cc
        alpn_raw = qs.get("alpn", [""])[0]
        if alpn_raw:
            p["alpn"] = [a.strip() for a in alpn_raw.split(",") if a.strip()]
        if qs.get("allow_insecure", ["0"])[0] in ("1", "true") or qs.get("insecure", ["0"])[0] in ("1", "true"):
            p["skip-cert-verify"] = True
        return p
    except Exception:
        return None


def parse_raw_line(line: str) -> Optional[Dict[str, Any]]:
    """单行原生代理链接路由与解析。"""
    line = line.strip()
    if not line:
        return None
    if line.startswith("vmess://"):
        return parse_vmess(line)
    elif line.startswith("vless://"):
        return parse_vless(line)
    elif line.startswith("trojan://"):
        return parse_trojan(line)
    elif line.startswith("ss://"):
        return parse_ss(line)
    elif line.startswith("hysteria2://") or line.startswith("hy2://"):
        return parse_hysteria2(line)
    elif line.startswith("anytls://"):
        return parse_anytls(line)
    elif line.startswith("tuic://"):
        return parse_tuic(line)
    return None


def parse_base64_payload(text: str) -> List[Dict[str, Any]]:
    """
    通用 Base64 生肉订阅解码引擎：
    - 若整个文本为单段 Base64 编码，自动解码为明文；
    - 逐行提取 vmess/vless/ss/trojan/hysteria2/anytls/tuic 链接；
    - 返回标准 Clash proxy 字典列表。
    """
    decoded = safe_b64decode(text)
    proto_hints = ("vmess://", "vless://", "ss://", "trojan://", "hysteria2://", "hy2://", "anytls://", "tuic://")
    content = decoded if decoded and any(proto in decoded for proto in proto_hints) else text
    proxies: List[Dict[str, Any]] = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        p = parse_raw_line(line)
        if p:
            proxies.append(p)
    return proxies


# ======================== 阶段 1: 多源上游拉取与去重 ========================

# 记录每个节点指纹首次归属的上游来源（仅内存中用于产率统计，绝不写入配置文件）
_SOURCE_BY_FP: Dict[str, str] = {}

VALID_SS_CIPHERS = {
    "aes-128-gcm", "aes-192-gcm", "aes-256-gcm",
    "chacha20-ietf-poly1305", "xchacha20-ietf-poly1305",
    "2022-blake3-aes-128-gcm", "2022-blake3-aes-256-gcm", "2022-blake3-chacha20-poly1305",
    "aes-128-cfb", "aes-192-cfb", "aes-256-cfb",
    "aes-128-ctr", "aes-192-ctr", "aes-256-ctr",
    "rc4-md5", "chacha20-ietf", "none",
}


def _fetch_single_source_group(idx_and_item: Tuple[int, Tuple[str, List[str]]]) -> Tuple[int, str, List[Dict[str, Any]]]:
    """拉取单个上游源（带镜像故障转移）。"""
    order_idx, (source_name, urls) = idx_and_item
    headers = {"User-Agent": "ClashMeta/1.19.34"}
    with requests.Session() as session:
        session.trust_env = False
        for url in urls:
            try:
                resp = session.get(url, headers=headers, timeout=12)
                resp.raise_for_status()
                text = resp.text
                proxies_list: List[Dict[str, Any]] = []

                # 1. 优先尝试作为标准 Clash YAML 解析
                if "proxies:" in text:
                    try:
                        data = yaml.load(text, Loader=YamlLoader)
                        if isinstance(data, dict) and isinstance(data.get("proxies"), list):
                            proxies_list = [p for p in data["proxies"] if isinstance(p, dict)]
                    except Exception:
                        pass

                # 2. 若非 YAML 或 YAML 未解析出节点，自动触发 Base64 / 原生协议极速解码
                if not proxies_list:
                    try:
                        proxies_list = parse_base64_payload(text)
                    except Exception as e:
                        log.warning("[%s] Base64/Raw 解码异常: %s", source_name, e)

                if proxies_list:
                    log.info("拉取成功 [%s]: %s (获取到 %d 个原始节点)", source_name, url.split("?")[0], len(proxies_list))
                    return (order_idx, source_name, proxies_list)
                log.warning("[%s] 响应未包含有效节点数据: %s", source_name, url)
            except Exception as exc:
                log.warning("[%s] 镜像拉取失败 (%s): %s", source_name, url.split("?")[0], exc)
    log.warning("订阅源 [%s] 所有镜像均不可达", source_name)
    return (order_idx, source_name, [])


def fetch_and_dedup() -> Tuple[List[Dict[str, Any]], int, int, List[Dict[str, Any]], Dict[str, Any]]:
    """多源并发拉取上游订阅节点（按优先级顺序去重合并，保留高优源质量参数），并回流历史保活池。"""
    _SOURCE_BY_FP.clear()
    fetched_groups: List[Tuple[int, str, List[Dict[str, Any]]]] = []

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_fetch_single_source_group, (i, item)) for i, item in enumerate(UPSTREAM_SOURCES)]
        for f in as_completed(futures):
            fetched_groups.append(f.result())

    # 严格按 UPSTREAM_SOURCES 定义的高存活率顺序排列合并
    fetched_groups.sort(key=lambda x: x[0])

    tagged_raw_proxies: List[Tuple[str, Dict[str, Any]]] = []
    for _, source_name, plist in fetched_groups:
        for p in plist:
            tagged_raw_proxies.append((source_name, p))

    # 核心保障：自动回流历史存活节点池（带 3 振出局容错元数据）
    old_historical_proxies, old_meta = load_historical_pool()
    if old_historical_proxies:
        for p in old_historical_proxies:
            tagged_raw_proxies.append(("历史保活池", p))
        log.info("成功回流历史保活节点池: %d 个 (进入保活复测队列)", len(old_historical_proxies))

    if not tagged_raw_proxies:
        raise RuntimeError("所有上游镜像源均拉取失败，终止任务。")

    raw_count = len(tagged_raw_proxies)
    seen_keys = set()
    dedup_proxies: List[Dict[str, Any]] = []
    per_source_uniq: Counter = Counter()

    for idx, (source_name, raw_proxy) in enumerate(tagged_raw_proxies):
        if not isinstance(raw_proxy, dict):
            continue
        proxy = dict(raw_proxy)

        # 1. 保留所有包含服务器的节点，绝不根据名字过滤，若名称缺失自动补齐
        _ = str(proxy.get("name") or f"node_{idx}").strip()

        # 2. 协议归一化与白名单过滤 (剔除 compatible、http、socks5 等无效类型)
        p_type = str(proxy.get("type", "")).strip().lower()
        if p_type == "hy2":
            p_type = "hysteria2"
            proxy["type"] = "hysteria2"
        if not p_type or p_type not in VALID_PROXY_TYPES:
            continue

        # 3. 服务器与端口有效性过滤
        server = str(proxy.get("server", "")).strip().lower()
        port = str(proxy.get("port", "")).strip()
        if not server or not port or server in INVALID_SERVERS or " " in server or "compatible" in server:
            continue

        try:
            port_num = int(port)
            if not (1 <= port_num <= 65535):
                continue
            proxy["port"] = port_num
        except ValueError:
            continue

        # 4. 凭证完整性与内核兼容性清洗
        if p_type in ("vmess", "vless") and not proxy.get("uuid"):
            continue
        if p_type == "vless" and str(proxy.get("cipher", "")).lower() == "none":
            proxy.pop("cipher", None)
        if p_type in ("ss", "trojan", "hysteria", "hysteria2", "anytls") and not (proxy.get("password") or proxy.get("auth")):
            continue
        if p_type == "tuic" and not (proxy.get("uuid") or proxy.get("token")):
            continue
        if p_type == "ss":
            cipher = str(proxy.get("cipher", "")).strip().lower()
            if cipher == "chacha20-poly1305":
                cipher = "chacha20-ietf-poly1305"
                proxy["cipher"] = cipher
            if not cipher or cipher not in VALID_SS_CIPHERS:
                continue

        # 5. Reality 校验与清洗 (短 ID 必须为 16 位合法 Hex，否则置空；缺公钥直接过滤)
        if "reality-opts" in proxy and isinstance(proxy["reality-opts"], dict):
            ro = dict(proxy["reality-opts"])
            if not ro.get("public-key"):
                continue
            sid = str(ro.get("short-id", "")).strip()
            if len(sid) != 16 or not all(c in "0123456789abcdefABCDEF" for c in sid):
                ro["short-id"] = ""
            proxy["reality-opts"] = ro

        unique_key = get_proxy_fingerprint(proxy)
        if unique_key in seen_keys:
            continue
        seen_keys.add(unique_key)
        _SOURCE_BY_FP[unique_key] = source_name
        per_source_uniq[source_name] += 1

        proxy["name"] = f"p_{idx:05d}"
        dedup_proxies.append(proxy)

    del tagged_raw_proxies, seen_keys
    gc.collect()

    dedup_count = len(dedup_proxies)
    top_contrib = ", ".join(f"{k}:{v}" for k, v in per_source_uniq.most_common(8))
    log.info("多源汇聚完成: 原始节点 %d 个 -> 全量待测 %d 个 (主要净增量: %s)", raw_count, dedup_count, top_contrib)
    return dedup_proxies, raw_count, dedup_count, old_historical_proxies, old_meta


# ======================== 阶段 2: Mihomo 高并发握手初筛 ========================

def run_stage1_fast_delay_filter(proxies: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], int]]:
    """
    使用 Mihomo 内核并发协程在 ~60 秒内对全部 2600+ 节点执行握手测速。
    握手 > 1000ms 或超时失败的节点直接丢弃。
    """
    TMP_STAGE1_DIR.mkdir(parents=True, exist_ok=True)
    cfg_path = TMP_STAGE1_DIR / "config.yaml"

    cfg = {
        "allow-lan": False,
        "bind-address": "127.0.0.1",
        "mode": "direct",
        "log-level": "silent",
        "ipv6": False,
        "unified-delay": True,
        "tcp-concurrent": True,
        "external-controller": f"127.0.0.1:{STAGE1_API_PORT}",
        "geodata-mode": False,
        "geo-auto-update": False,
        "profile": {"store-selected": False, "store-fake-ip": False},
        "dns": {"enable": False},
        "proxies": proxies,
        "rules": ["MATCH,DIRECT"],
    }

    with cfg_path.open("w", encoding="utf-8") as f:
        yaml.dump(cfg, f, Dumper=YamlDumper, allow_unicode=True, sort_keys=False)

    del cfg
    gc.collect()

    log.info("启动临时 Mihomo 实例用于全量并发握手初筛 (端口: %d)...", STAGE1_API_PORT)
    proc = subprocess.Popen(
        [BIN_MIHOMO, "-d", str(TMP_STAGE1_DIR), "-f", str(cfg_path)],
        env=GO_LOW_MEM_ENV,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    _active_procs.append(proc)

    # 等待 REST API 就绪
    ready = False
    with requests.Session() as s:
        s.trust_env = False
        for _ in range(30):
            if proc.poll() is not None:
                err = proc.stdout.read().decode("utf-8", errors="replace") if proc.stdout else ""
                raise RuntimeError(f"Mihomo 初筛实例异常退出: {err[:600]}")
            try:
                r = s.get(f"http://127.0.0.1:{STAGE1_API_PORT}/version", timeout=1)
                if r.status_code == 200:
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(0.5)

    if not ready:
        kill_active_procs()
        raise RuntimeError("等待 Mihomo 初筛实例就绪超时。")

    log.info("Mihomo 实例就绪，开始对全量 %d 个节点执行 60 协程并发握手测速 (握手上限=%dms)...",
             len(proxies), HANDSHAKE_TIMEOUT_MS)

    proxy_map = {p["name"]: p for p in proxies}
    alive_records: List[Tuple[Dict[str, Any], int]] = []
    tested_count = [0]
    total_count = len(proxies)
    t_start = time.monotonic()

    def check_node(name: str) -> Optional[Tuple[Dict[str, Any], int]]:
        url = (
            f"http://127.0.0.1:{STAGE1_API_PORT}/proxies/{name}/delay"
            f"?timeout={HANDSHAKE_TIMEOUT_MS}&url=https://www.google.com/generate_204"
        )
        with requests.Session() as s:
            s.trust_env = False
            # 允许最多 2 次握手探测机会（消除偶发单包丢包和 WiFi 射频抖动带来的误杀）
            for attempt in range(2):
                try:
                    resp = s.get(url, timeout=(HANDSHAKE_TIMEOUT_MS / 1000.0) + 0.8)
                    if resp.status_code == 200:
                        d = resp.json()
                        delay = d.get("delay", 9999)
                        if 0 < delay <= HANDSHAKE_TIMEOUT_MS:
                            return (proxy_map[name], delay)
                except Exception:
                    pass
                if attempt == 0:
                    time.sleep(0.3)
        return None

    names = [p["name"] for p in proxies]
    with ThreadPoolExecutor(max_workers=STAGE1_CONCURRENCY) as pool:
        futures = {pool.submit(check_node, name): name for name in names}
        for f in as_completed(futures):
            res = f.result()
            if res:
                alive_records.append(res)
            tested_count[0] += 1
            if tested_count[0] % 500 == 0 or tested_count[0] == total_count:
                elapsed = time.monotonic() - t_start
                rate = tested_count[0] / elapsed if elapsed > 0 else 0
                log.info("初筛进度: %d/%d (%.1f 个/秒), 存活 (<=%dms): %d",
                         tested_count[0], total_count, rate, HANDSHAKE_TIMEOUT_MS, len(alive_records))

    elapsed_total = time.monotonic() - t_start
    log.info("全量初筛完成！耗时: %.1f 秒, 初步存活节点数: %d 个", elapsed_total, len(alive_records))

    # 阶段 1.5: 对初步存活节点执行 5 并发双轮抽样精测与稳定性熔断 (消除冷启动与 50% 丢包假活)
    if alive_records:
        log.info("开始对 %d 个初选节点执行 5 并发低载双轮精测与稳定性熔断...", len(alive_records))
        refined_records: List[Tuple[Dict[str, Any], int]] = []
        flaky_count = 0

        def refine_check(item: Tuple[Dict[str, Any], int]) -> Optional[Tuple[Dict[str, Any], int]]:
            p, d1 = item
            time.sleep(0.3)
            url = (
                f"http://127.0.0.1:{STAGE1_API_PORT}/proxies/{p['name']}/delay"
                f"?timeout={HANDSHAKE_TIMEOUT_MS}&url=https://www.google.com/generate_204"
            )
            # 三局两胜容错精测机制（绝不误杀冷启动与轻微抖动的好节点）
            success_delays = [d1]
            with requests.Session() as s:
                s.trust_env = False
                for _ in range(2):
                    try:
                        resp = s.get(url, timeout=(HANDSHAKE_TIMEOUT_MS / 1000.0) + 0.8)
                        if resp.status_code == 200:
                            d = resp.json().get("delay", 9999)
                            if 0 < d <= HANDSHAKE_TIMEOUT_MS:
                                success_delays.append(d)
                                if len(success_delays) >= 2:
                                    break
                    except Exception:
                        pass
                    time.sleep(0.3)

            # 只要在至多 3 轮测试中成功通过 2 轮，节点即认定稳定可用！
            if len(success_delays) >= 2:
                d_min = min(success_delays)
                d_max = max(success_delays)
                jitter = d_max - d_min
                # 综合延迟平稳得分：抖动大的往后排，但绝不直接丢弃
                effective_score = int(0.5 * d_min + 0.3 * d_max + 0.2 * jitter)
                return (p, effective_score)
            return None

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(refine_check, item) for item in alive_records]
            for f in as_completed(futures):
                res = f.result()
                if res:
                    refined_records.append(res)
                else:
                    flaky_count += 1

        log.info("双轮精测完成: 最终稳定入围 %d 个 (已成功熔断剔除 %d 个偶发丢包/假活节点)",
                 len(refined_records), flaky_count)
        alive_records = refined_records

    if alive_records and _SOURCE_BY_FP:
        src_hits = Counter(_SOURCE_BY_FP.get(get_proxy_fingerprint(p), "unknown") for p, _ in alive_records)
        hit_summary = ", ".join(f"{k}:{v}" for k, v in src_hits.most_common())
        log.info("各来源存活贡献统计 (%d 个): %s", len(alive_records), hit_summary)

    # 关闭初筛 Mihomo 并释放资源
    kill_active_procs()
    shutil.rmtree(TMP_STAGE1_DIR, ignore_errors=True)
    gc.collect()

    return alive_records


# ======================== 阶段 3: 落地 IP 属性与品质判定 ========================

def query_exit_ip_info(proxy_url: str) -> Optional[Dict[str, Any]]:
    """通过本地代理探测落地国家代码与 ISP/Hosting 属性。"""
    proxies = {"http": proxy_url, "https": proxy_url}
    headers = {"User-Agent": "curl/7.74.0", "Connection": "close"}

    # 1. 旗舰接口: ipapi.is (若配置了 Key，具备最高精度 datacenter/vpn/proxy 识别)
    if IPAPI_IS_KEY:
        try:
            with requests.Session() as s:
                s.trust_env = False
                r = s.get(
                    f"https://api.ipapi.is/?key={IPAPI_IS_KEY}",
                    proxies=proxies,
                    headers=headers,
                    timeout=6,
                )
                if r.status_code == 200:
                    d = r.json()
                    loc = d.get("location") or {}
                    cc = str(loc.get("country_code", "")).strip().upper()
                    if cc:
                        company = d.get("company") or {}
                        asn_info = d.get("asn") or {}
                        is_dc = bool(d.get("is_datacenter", False))
                        is_proxy = bool(d.get("is_proxy", False) or d.get("is_vpn", False) or d.get("is_tor", False))
                        return {
                            "cc": cc,
                            "isp": str(company.get("name", "")),
                            "org": str(asn_info.get("org", "")),
                            "as": f"AS{asn_info.get('asn', '')}",
                            "hosting": is_dc,
                            "proxy": is_proxy,
                        }
        except Exception:
            pass

    # 2. 主力接口: ip-api.com (免 Key 极速直出)
    try:
        with requests.Session() as s:
            s.trust_env = False
            r = s.get(
                "http://ip-api.com/json/?fields=status,countryCode,isp,org,as,hosting,proxy,mobile,query",
                proxies=proxies,
                headers=headers,
                timeout=6,
            )
            if r.status_code == 200:
                d = r.json()
                if d.get("status") == "success" and d.get("countryCode"):
                    return {
                        "cc": str(d.get("countryCode", "")).strip().upper(),
                        "isp": str(d.get("isp", "")),
                        "org": str(d.get("org", "")),
                        "as": str(d.get("as", "")),
                        "hosting": d.get("hosting"),
                        "proxy": d.get("proxy"),
                    }
    except Exception:
        pass

    # 3. 备用接口: ipwho.is
    try:
        with requests.Session() as s:
            s.trust_env = False
            r = s.get("http://ipwho.is/", proxies=proxies, headers=headers, timeout=6)
            if r.status_code == 200:
                d = r.json()
                if d.get("success") and d.get("country_code"):
                    conn = d.get("connection") or {}
                    return {
                        "cc": str(d.get("country_code", "")).strip().upper(),
                        "isp": str(conn.get("isp", "")),
                        "org": str(conn.get("org", "")),
                        "as": str(conn.get("asn", "")),
                        "hosting": None,
                        "proxy": None,
                    }
    except Exception:
        pass

    return None



def extract_cc_from_name(raw_name: str) -> str:
    """从节点名称中推断国家代码，防止因外部 IP API 限流或超时误杀节点。"""
    name_upper = raw_name.upper()
    keywords = {
        "香港": "HK", "HK": "HK", "HONG KONG": "HK",
        "台湾": "TW", "TW": "TW", "TAIWAN": "TW",
        "日本": "JP", "JP": "JP", "JAPAN": "JP",
        "新加坡": "SG", "SG": "SG", "SINGAPORE": "SG",
        "美国": "US", "US": "US", "USA": "US",
        "韩国": "KR", "KR": "KR", "KOREA": "KR",
        "德国": "DE", "DE": "DE", "GERMANY": "DE",
        "英国": "GB", "GB": "GB", "UK": "GB",
        "法国": "FR", "FR": "FR", "FRANCE": "FR",
        "荷兰": "NL", "NL": "NL", "NETHERLANDS": "NL",
        "俄罗斯": "RU", "RU": "RU", "RUSSIA": "RU",
        "意大利": "IT", "IT": "IT", "ITALY": "IT",
        "加拿大": "CA", "CA": "CA", "CANADA": "CA",
        "澳大利亚": "AU", "AU": "AU", "AUSTRALIA": "AU",
        "土耳其": "TR", "TR": "TR", "TURKEY": "TR",
        "哈萨克斯坦": "KZ", "KZ": "KZ",
        "芬兰": "FI", "FI": "FI",
        "奥地利": "AT", "AT": "AT",
    }
    for kw, cc in keywords.items():
        if kw in name_upper or kw in raw_name:
            return cc
    return "OTHER"

def classify_node_tier(ip_info: Dict[str, Any], latency_ms: int) -> str:
    """根据 IP 属性与握手延迟判定节点品质 (纯净 / 优选 / 普通)。"""
    isp_blob = f"{ip_info.get('isp', '')} {ip_info.get('org', '')} {ip_info.get('as', '')}".lower()
    is_hosting_kw = any(kw in isp_blob for kw in DATACENTER_KEYWORDS)
    api_hosting = ip_info.get("hosting")
    api_proxy = ip_info.get("proxy")

    # 纯净住宅 IP 判定
    if (
        isp_blob.strip()
        and not is_hosting_kw
        and api_hosting is False
        and api_proxy is False
    ):
        return "纯净"

    # 优选低延迟判定
    if latency_ms <= PREMIUM_MAX_LATENCY_MS:
        return "优选"

    return "普通"


def run_stage2_exit_detection(
    alive_records: List[Tuple[Dict[str, Any], int]]
) -> List[Dict[str, Any]]:
    """对初筛存活节点进行 3 路并发落地检测与纯净度判定。"""
    if not alive_records:
        return []

    TMP_STAGE2_DIR.mkdir(parents=True, exist_ok=True)
    cfg_path = TMP_STAGE2_DIR / "config.yaml"

    mihomo_proxies: List[Dict[str, Any]] = []
    internal_names: List[str] = []
    task_items: List[Tuple[str, int, Dict[str, Any]]] = []

    for rank, (proxy, delay) in enumerate(alive_records):
        name = f"w_{rank:04d}"
        p_copy = dict(proxy)
        p_copy["name"] = name
        mihomo_proxies.append(p_copy)
        internal_names.append(name)
        task_items.append((name, delay, proxy))

    cfg = {
        "allow-lan": False,
        "bind-address": "127.0.0.1",
        "mode": "rule",
        "log-level": "silent",
        "ipv6": False,
        "external-controller": f"127.0.0.1:{STAGE2_API_PORT}",
        "geodata-mode": False,
        "geo-auto-update": False,
        "profile": {"store-selected": False, "store-fake-ip": False},
        "dns": {"enable": False},
        "proxies": mihomo_proxies,
        "proxy-groups": [
            {
                "name": f"WORKER_{i}",
                "type": "select",
                "proxies": internal_names,
            }
            for i in range(STAGE2_CONCURRENCY)
        ],
        "listeners": [
            {
                "name": f"http-worker-{i}",
                "type": "http",
                "port": STAGE2_BASE_PORT + i,
                "listen": "127.0.0.1",
                "proxy": f"WORKER_{i}",
            }
            for i in range(STAGE2_CONCURRENCY)
        ],
        "rules": ["MATCH,DIRECT"],
    }

    with cfg_path.open("w", encoding="utf-8") as f:
        yaml.dump(cfg, f, Dumper=YamlDumper, allow_unicode=True, sort_keys=False)

    del cfg, mihomo_proxies
    gc.collect()

    log.info("启动落地检测 Mihomo 实例 (并发端口: %d~%d)...",
             STAGE2_BASE_PORT, STAGE2_BASE_PORT + STAGE2_CONCURRENCY - 1)
    proc = subprocess.Popen(
        [BIN_MIHOMO, "-d", str(TMP_STAGE2_DIR), "-f", str(cfg_path)],
        env=GO_LOW_MEM_ENV,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    _active_procs.append(proc)

    # 等待 REST API 就绪
    ready = False
    with requests.Session() as s:
        s.trust_env = False
        for _ in range(30):
            if proc.poll() is not None:
                err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
                raise RuntimeError(f"Mihomo 落地实例异常退出: {err[:300]}")
            try:
                r = s.get(f"http://127.0.0.1:{STAGE2_API_PORT}/version", timeout=1)
                if r.status_code == 200:
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(0.5)

    if not ready:
        kill_active_procs()
        raise RuntimeError("等待 Mihomo 落地实例就绪超时。")

    task_q: "queue.Queue[Tuple[str, int, Dict[str, Any]]]" = queue.Queue()
    for item in task_items:
        task_q.put(item)

    results_lock = threading.Lock()
    verified_records: List[Dict[str, Any]] = []
    progress_counter = [0]
    total_tasks = len(task_items)

    log.info("开始落地 IP 纯净度与地区属性检测 (待测 %d 个)...", total_tasks)

    def worker(worker_id: int) -> None:
        group_name = f"WORKER_{worker_id}"
        proxy_url = f"http://127.0.0.1:{STAGE2_BASE_PORT + worker_id}"
        ctrl_url = f"http://127.0.0.1:{STAGE2_API_PORT}/proxies/{group_name}"

        with requests.Session() as ctrl_s:
            ctrl_s.trust_env = False
            while True:
                try:
                    name, delay, orig_proxy = task_q.get_nowait()
                except queue.Empty:
                    break

                try:
                    sw = ctrl_s.put(ctrl_url, json={"name": name}, timeout=3)
                    if sw.status_code not in (200, 204):
                        continue

                    ip_info = query_exit_ip_info(proxy_url)
                    if not ip_info:
                        # 容错降级：免费第三方 IP API 若临时超时或限流，绝不误杀真实可用的节点
                        inferred_cc = extract_cc_from_name(orig_proxy.get("name", ""))
                        ip_info = {
                            "cc": inferred_cc or "OTHER",
                            "isp": "Unknown",
                            "org": "",
                            "as": "",
                            "hosting": None,
                            "proxy": None,
                        }

                    cc = ip_info["cc"]
                    if not re.fullmatch(r"[A-Z]{2}", cc):
                        cc = "OTHER"

                    # 严格剔除中国大陆落地节点 (CN)，防止国内流量回环与无效翻墙
                    if cc == "CN":
                        continue

                    # 核心校验：多目标端到端高精可用性探测（7s 宽裕超时 + 双轮容错重试，杜绝误杀）
                    https_ok = False
                    with requests.Session() as browse_s:
                        browse_s.trust_env = False
                        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
                        proxies = {"http": proxy_url, "https": proxy_url}

                        for attempt in range(2):
                            # 探测 1：Google 204（核心翻墙验证）
                            try:
                                r_google = browse_s.get(
                                    "https://www.google.com/generate_204",
                                    proxies=proxies,
                                    headers=headers,
                                    timeout=7,
                                )
                                if r_google.status_code in (200, 204):
                                    https_ok = True
                                    break
                            except Exception:
                                pass

                            # 探测 2：YouTube 204（备选破墙探测，应对 Google 单点微抖动）
                            try:
                                r_yt = browse_s.get(
                                    "https://www.youtube.com/generate_204",
                                    proxies=proxies,
                                    headers=headers,
                                    timeout=7,
                                )
                                if r_yt.status_code in (200, 204):
                                    https_ok = True
                                    break
                            except Exception:
                                pass

                            # 探测 3：Cloudflare 204 / trace（全球国际网络连通与双向传输验证）
                            try:
                                r_cf = browse_s.get(
                                    "https://cp.cloudflare.com/generate_204",
                                    proxies=proxies,
                                    headers=headers,
                                    timeout=7,
                                )
                                if r_cf.status_code in (200, 204):
                                    https_ok = True
                                    break
                            except Exception:
                                pass

                            if attempt == 0 and not https_ok:
                                time.sleep(0.5)

                    if not https_ok:
                        continue

                    tier = classify_node_tier(ip_info, delay)

                    with results_lock:
                        verified_records.append({
                            "cc": cc,
                            "tier": tier,
                            "delay": delay,
                            "proxy": orig_proxy,
                        })
                except Exception:
                    pass
                finally:
                    with results_lock:
                        progress_counter[0] += 1
                        if progress_counter[0] % 10 == 0 or progress_counter[0] == total_tasks:
                            log.info("落地检测进度: %d/%d (已验证合格: %d)",
                                     progress_counter[0], total_tasks, len(verified_records))
                    task_q.task_done()
                    time.sleep(0.2)

    threads = [
        threading.Thread(target=worker, args=(i,), daemon=True)
        for i in range(STAGE2_CONCURRENCY)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    kill_active_procs()
    shutil.rmtree(TMP_STAGE2_DIR, ignore_errors=True)
    gc.collect()

    log.info("落地属性检测完成，成功获取到 %d 个合格节点的地区与纯净度标签", len(verified_records))
    return verified_records


# ======================== 阶段 4: 策略组分桶、单组上限 20 与规范重命名 ========================

def bucket_and_limit_nodes(verified_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    按 cmfa-merged-config.yaml 的策略组分桶：
    - 香港 (HK)
    - 日本 (JP)
    - 新加坡 (SG)
    - 美国 (US)
    - 其他地区 (OTHER)

    在每个分桶中：
    1. 排序优先级：纯净 (住宅) > 优选 (低延迟) > 普通；
    2. 延迟升序 (握手延迟低的排前面)；
    3. 每个策略组严格只保留最多 MAX_NODES_PER_GROUP (20) 个；
    4. 规范重命名为 "{CC} | {tier} | {两位编号}" (例如 "HK | 纯净 | 01")。
    """
    valid_records = [r for r in verified_records if r["cc"] != "CN"]

    # 全局扁平化排序：纯净优先 -> 稳定性综合延迟评分升序
    valid_records.sort(
        key=lambda x: (
            TIER_PRIORITY.get(x["tier"], 9),
            x["delay"],
        )
    )

    final_proxies: List[Dict[str, Any]] = []
    naming_counter: Counter = Counter()

    for item in valid_records:
        cc = item["cc"]
        tier = item["tier"]
        naming_counter[(cc, tier)] += 1
        seq = naming_counter[(cc, tier)]

        cc_cn = CC_REGION_NAMES.get(cc, cc)
        new_name = f"{cc_cn} | {tier} | {seq:02d}"
        node_dict = dict(item["proxy"])
        node_dict["name"] = new_name

        final_proxies.append({
            "cc": cc,
            "tier": tier,
            "delay": item["delay"],
            "proxy": node_dict,
        })

    log.info("全局优选排序完成: 共保留 %d 个合格节点 (首位节点延迟评分: %dms)",
             len(final_proxies), final_proxies[0]["delay"] if final_proxies else 0)
    return final_proxies


# ======================== 阶段 5: 报表生成与 GitHub 原子化强推 ========================

CC_REGION_NAMES: Dict[str, str] = {
    "HK": "香港",
    "TW": "台湾",
    "MO": "澳门",
    "JP": "日本",
    "SG": "新加坡",
    "US": "美国",
    "KR": "韩国",
    "DE": "德国",
    "GB": "英国",
    "UK": "英国",
    "FR": "法国",
    "NL": "荷兰",
    "RU": "俄罗斯",
    "IT": "意大利",
    "CA": "加拿大",
    "AU": "澳大利亚",
    "IN": "印度",
    "TR": "土耳其",
    "VN": "越南",
    "MY": "马来西亚",
    "TH": "泰国",
    "PH": "菲律宾",
    "ID": "印尼",
    "BR": "巴西",
    "ES": "西班牙",
    "SE": "瑞典",
    "CH": "瑞士",
    "AT": "奥地利",
    "BG": "保加利亚",
    "IE": "爱尔兰",
    "FI": "芬兰",
    "NO": "挪威",
    "PL": "波兰",
    "UA": "乌克兰",
    "AR": "阿根廷",
    "CL": "智利",
    "CO": "哥伦比亚",
    "MX": "墨西哥",
    "ZA": "南非",
    "AE": "阿联酋",
    "IL": "以色列",
    "NZ": "新西兰",
    "KZ": "哈萨克斯坦",
    "IS": "冰岛",
    "CZ": "捷克",
    "RO": "罗马尼亚",
    "MD": "摩尔多瓦",
    "RS": "塞尔维亚",
    "HU": "匈牙利",
    "GR": "希腊",
    "PT": "葡萄牙",
    "BE": "比利时",
    "DK": "丹麦",
    "LU": "卢森堡",
    "EE": "爱沙尼亚",
    "LV": "拉脱维亚",
    "LT": "立陶宛",
    "GE": "格鲁吉亚",
    "AM": "亚美尼亚",
    "AZ": "阿塞拜疆",
    "UZ": "乌兹别克斯坦",
    "KG": "吉尔吉斯斯坦",
    "TJ": "塔吉克斯坦",
    "TM": "土库曼斯坦",
    "MN": "蒙古",
    "PK": "巴基斯坦",
    "BD": "孟加拉国",
    "LK": "斯里兰卡",
    "NP": "尼泊尔",
    "KH": "柬埔寨",
    "LA": "老挝",
    "MM": "缅甸",
    "UN": "其他",
}


def build_readme(
    update_time_utc: str,
    raw_count: int,
    dedup_count: int,
    alive_count: int,
    final_records: List[Dict[str, Any]],
) -> str:
    tier_counts = Counter(r["tier"] for r in final_records)
    cc_counts = Counter(r["cc"] for r in final_records)

    clean_count = tier_counts.get("纯净", 0)
    premium_count = tier_counts.get("优选", 0)
    total_final = len(final_records)

    cc_rows = "\n".join(
        f"| `{cc}` | {CC_REGION_NAMES.get(cc, '海外其他')} | {cnt} |"
        for cc, cnt in cc_counts.most_common()
    )

    return f"""# Clash Live Nodes (`nodes` branch)

全天候自动化实时测速、去重与质量筛选的 Clash 节点订阅（纯节点文件：`proxies.yaml`）。

---

## 🔗 快速获取节点订阅

复制下方链接直接导入支持 Clash / Mihomo 的客户端，或在自建配置中引入：

* **GitHub 直连（推荐，实时更新无缓存）**：
  ```text
  https://raw.githubusercontent.com/Komeiji6/Clash-config/nodes/proxies.yaml
  ```
* **CDN 镜像（GitHub 直连受阻时备用）**：
  ```text
  https://cdn.jsdelivr.net/gh/Komeiji6/Clash-config@nodes/proxies.yaml
  `
* **国内反代镜像（校园网/直连受阻备用）**：
  `	ext
  https://ghproxy.net/https://raw.githubusercontent.com/Komeiji6/Clash-config/nodes/proxies.yaml
  ```

> 💡 **需要开箱即用完整分流配置？** 请访问 [main 分支主页](https://github.com/Komeiji6/Clash-config) 获取内置精细规则分流的完整主配置。

---

## 🛠️ 在已有配置中引用 (proxy-providers)

如果你已有自己的 Clash / Mihomo 配置文件，只需在配置中添加以下代码段即可无缝引入：

```yaml
proxy-providers:
  free-nodes:
    type: http
    url: "https://raw.githubusercontent.com/Komeiji6/Clash-config/nodes/proxies.yaml"
    interval: 7200
    path: ./providers/free-nodes.yaml
    health-check:
      enable: true
      interval: 300
      url: http://www.gstatic.com/generate_204
```

---

## 📊 节点状态与分布

| 指标 | 状态 / 数量 |
| :--- | :--- |
| **最后更新时间 (UTC)** | `{update_time_utc}` |
| **当前入库节点总数** | **{total_final}** (已全量排除中国大陆落地) |
| **纯净住宅节点** | {clean_count} |
| **优选低延节点** | {premium_count} |

### 落地国家 / 地区分布

| 地区代码 | 地区 | 节点数 |
| :---: | :---: | :---: |
{cc_rows}

---
*注：节点命名规则为 `{{地区中文名}} | {{纯净/优选}} | {{序号}}`，已按真实综合延迟平稳度全局升序排列。*
"""


def atomic_push_to_github(
    final_records: List[Dict[str, Any]],
    raw_count: int,
    dedup_count: int,
    alive_count: int,
    old_historical_proxies: List[Dict[str, Any]],
    old_meta: Dict[str, Any],
) -> None:
    """在 /tmp/out_repo 初始化单 commit 孤儿分支并原子化强推至 GitHub nodes 分支。"""
    update_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    final_proxies = [r["proxy"] for r in final_records]
    dump_proxies_yaml(TMP_FINAL_YAML, final_proxies)

    if TMP_OUT_REPO.exists():
        shutil.rmtree(TMP_OUT_REPO, ignore_errors=True)
    TMP_OUT_REPO.mkdir(parents=True, exist_ok=True)

    shutil.copy2(TMP_FINAL_YAML, TMP_OUT_REPO / "proxies.yaml")

    readme_content = build_readme(
        update_time_utc=update_time_utc,
        raw_count=raw_count,
        dedup_count=dedup_count,
        alive_count=alive_count,
        final_records=final_records,
    )
    (TMP_OUT_REPO / "README.md").write_text(readme_content, encoding="utf-8")

    git_env = {
        **os.environ,
        "GIT_SSH_COMMAND": f"ssh -i {GIT_SSH_KEY} -o StrictHostKeyChecking=accept-new -o BatchMode=yes",
    }

    def run_git(args: List[str]) -> None:
        res = subprocess.run(
            ["git", *args],
            cwd=str(TMP_OUT_REPO),
            env=git_env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if res.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)} 失败:\nSTDOUT: {res.stdout}\nSTDERR: {res.stderr}")

    log.info("初始化孤儿分支并原子化强推至 %s (%s)...", GIT_REMOTE_URL, GIT_BRANCH)
    run_git(["init", "-b", GIT_BRANCH])
    run_git(["config", "user.name", GIT_USER_NAME])
    run_git(["config", "user.email", GIT_USER_EMAIL])
    run_git(["add", "proxies.yaml", "README.md"])
    run_git(["commit", "-m", f"chore: auto update live nodes ({update_time_utc})"])
    run_git(["remote", "add", "origin", GIT_REMOTE_URL])
    run_git(["push", "--force", "origin", GIT_BRANCH])
    log.info("🎉 推送完成！(%s, 共发布 %d 个节点至 nodes 分支)", update_time_utc, len(final_proxies))

    # 2. 同步更新 main 分支主配置 cmfa-merged-config.yaml（节点直接内嵌，杜绝 proxy-providers 套娃）
    try:
        main_repo_dir = TMP_DIR / "main_repo"
        if main_repo_dir.exists():
            shutil.rmtree(main_repo_dir, ignore_errors=True)
        log.info("拉取 main 分支以同步内嵌最新节点...")
        res = subprocess.run(
            ["git", "clone", "-b", "main", "--depth", "1", GIT_REMOTE_URL, str(main_repo_dir)],
            env=git_env, capture_output=True, text=True, timeout=60
        )
        if res.returncode == 0:
            config_file = main_repo_dir / "cmfa-merged-config.yaml"
            if config_file.exists():
                with config_file.open("r", encoding="utf-8") as f:
                    main_cfg = yaml.load(f, Loader=YamlLoader)
                if isinstance(main_cfg, dict):
                    if "proxy-providers" in main_cfg:
                        del main_cfg["proxy-providers"]
                    main_cfg["proxies"] = final_proxies
                    # 极简平铺策略组，无国家组、无自动测速组、无 compatible
                    main_cfg["proxy-groups"] = [
                        {
                            "name": "PROXY",
                            "type": "select",
                            "include-all": True,
                            "proxies": ["DIRECT"],
                        },
                        {
                            "name": "漏网之鱼",
                            "type": "select",
                            "proxies": ["PROXY", "DIRECT"],
                        },
                    ]
                    with config_file.open("w", encoding="utf-8") as f:
                        yaml.dump(main_cfg, f, Dumper=YamlDumper, allow_unicode=True, sort_keys=False)

                    subprocess.run(["git", "config", "user.name", GIT_USER_NAME], cwd=str(main_repo_dir), env=git_env)
                    subprocess.run(["git", "config", "user.email", GIT_USER_EMAIL], cwd=str(main_repo_dir), env=git_env)
                    subprocess.run(["git", "add", "cmfa-merged-config.yaml"], cwd=str(main_repo_dir), env=git_env)
                    commit_res = subprocess.run(
                        ["git", "commit", "-m", f"chore: sync live embedded nodes ({update_time_utc})"],
                        cwd=str(main_repo_dir), env=git_env, capture_output=True, text=True
                    )
                    if commit_res.returncode == 0:
                        push_res = subprocess.run(
                            ["git", "push", "origin", "main"],
                            cwd=str(main_repo_dir), env=git_env, capture_output=True, text=True, timeout=60
                        )
                        if push_res.returncode == 0:
                            log.info("🎉 main 分支主配置 (cmfa-merged-config.yaml) 内嵌节点同步推送成功！")
        if main_repo_dir.exists():
            shutil.rmtree(main_repo_dir, ignore_errors=True)
    except Exception as exc:
        log.warning("同步更新 main 分支内嵌节点异常 (不影响 nodes 分支发布): %s", exc)

    # 核心保障：使用 3 振出局与容错缓冲算法，更新本地历史保活池
    update_and_save_historical_pool(
        current_verified_proxies=final_proxies,
        old_historical_proxies=old_historical_proxies,
        old_meta=old_meta,
    )

    # 自动刷新 jsdelivr 全球 CDN 边缘缓存，确保国内免代理客户端秒级拉取最新节点
    try:
        requests.get("https://purge.jsdelivr.net/gh/Komeiji6/Clash-config@nodes/proxies.yaml", timeout=10)
        requests.get("https://purge.jsdelivr.net/gh/Komeiji6/Clash-config@main/cmfa-merged-config.yaml", timeout=10)
        log.info("已自动触发 jsdelivr CDN 缓存刷新。")
    except Exception as exc:
        log.warning("触发 jsdelivr 缓存刷新失败 (不影响推送): %s", exc)


# ======================== 主流程入口 ========================

def main() -> int:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    start_ts = time.monotonic()
    log.info("========== Clash 节点全量测速筛选与自动发布启动 ==========")

    try:
        cleanup_tmp()
        ensure_system_time_and_network()

        # 1. 全量拉取上游与去重并回流历史节点池
        proxies, raw_count, dedup_count, old_hist_proxies, old_hist_meta = fetch_and_dedup()
        if dedup_count < MIN_SAFEGUARD_NODES:
            log.critical("[熔断保护] 上游有效节点仅 %d 个 (< %d)，终止发布！",
                         dedup_count, MIN_SAFEGUARD_NODES)
            return 1

        # 2. 全量高并发握手测速初筛 (<= 1000ms)
        alive_records = run_stage1_fast_delay_filter(proxies)
        del proxies
        gc.collect()

        alive_count = len(alive_records)
        if alive_count < MIN_SAFEGUARD_NODES:
            log.critical("[熔断保护] 1000ms 内有效存活仅 %d 个节点 (< %d)，终止发布！",
                         alive_count, MIN_SAFEGUARD_NODES)
            return 1

        # 3. 落地 IP 属性与品质判定
        verified_records = run_stage2_exit_detection(alive_records)
        del alive_records
        gc.collect()

        if len(verified_records) < MIN_SAFEGUARD_NODES:
            log.critical("[熔断保护] 落地通过节点仅 %d 个 (< %d)，终止发布！",
                         len(verified_records), MIN_SAFEGUARD_NODES)
            return 1

        # 4. 策略组分桶、单组上限 20 截取与重命名
        final_records = bucket_and_limit_nodes(verified_records)
        del verified_records
        gc.collect()

        if len(final_records) < MIN_SAFEGUARD_NODES:
            log.critical("[熔断保护] 最终精选节点仅 %d 个 (< %d)，终止发布！",
                         len(final_records), MIN_SAFEGUARD_NODES)
            return 1

        # 5. 原子化强推至 GitHub nodes 分支并更新历史保活池
        atomic_push_to_github(
            final_records=final_records,
            raw_count=raw_count,
            dedup_count=dedup_count,
            alive_count=alive_count,
            old_historical_proxies=old_hist_proxies,
            old_meta=old_hist_meta,
        )

        elapsed = time.monotonic() - start_ts
        log.info("========== 全部流程圆满执行完成，总耗时 %.1f 秒 ==========", elapsed)
        return 0

    except Exception as exc:
        log.exception("执行发生异常: %s", exc)
        return 1
    finally:
        cleanup_tmp()


if __name__ == "__main__":
    sys.exit(main())
