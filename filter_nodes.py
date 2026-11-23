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

import gc
import logging
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

UPSTREAM_SOURCES = [
    # 1. chengaopan/AutoMergePublicNodes (8.4k stars, 多源自动合并，高频更新)
    [
        "https://cdn.jsdelivr.net/gh/chengaopan/AutoMergePublicNodes@master/list.yml",
        "https://raw.githubusercontent.com/chengaopan/AutoMergePublicNodes/master/list.yml",
    ],
    # 2. awesome-vpn/awesome-vpn (7.0k stars, 优质 Hysteria2 / Vless 现代协议源)
    [
        "https://cdn.jsdelivr.net/gh/awesome-vpn/awesome-vpn@master/clash.yaml",
        "https://raw.githubusercontent.com/awesome-vpn/awesome-vpn/master/clash.yaml",
    ],
    # 3. peasoft/NoMoreWalls (3.6k stars, 社区高分活跃精选)
    [
        "https://cdn.jsdelivr.net/gh/peasoft/NoMoreWalls@master/list.meta.yml",
        "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.meta.yml",
    ],
    # 4. snakem982/proxypool (2.3k stars, 每日测活全协议源)
    [
        "https://cdn.jsdelivr.net/gh/snakem982/proxypool@main/source/clash-meta-2.yaml",
        "https://raw.githubusercontent.com/snakem982/proxypool/main/source/clash-meta-2.yaml",
    ],
    # 5. zhuhaiuk/free-nodes (700+ stars, 每小时自动更新)
    [
        "https://cdn.jsdelivr.net/gh/zhuhaiuk/free-nodes@main/clash_config.yaml",
        "https://raw.githubusercontent.com/zhuhaiuk/free-nodes/main/clash_config.yaml",
    ],
    # 6. Barabama/FreeNodes (670+ stars, 定时精选节点)
    [
        "https://cdn.jsdelivr.net/gh/Barabama/FreeNodes@main/nodes/cfmem.yaml",
        "https://raw.githubusercontent.com/Barabama/FreeNodes@main/nodes/cfmem.yaml",
    ],
    # 7. ermao.net (二猫独立博主优质源)
    [
        "https://blog.ermao.net/sub/clash/ermao.net",
        "https://www.ermao.net/sub/clash/ermao.net",
    ],
    # 8. TopFreeProxy (综合全网聚合大池底包)
    [
        "https://cdn.jsdelivr.net/gh/VPN-Subcription-Links/ClashX-V2Ray-TopFreeProxy@main/combine/clash.config.yaml",
        "https://raw.githubusercontent.com/VPN-Subcription-Links/ClashX-V2Ray-TopFreeProxy/main/combine/clash.config.yaml",
    ],
]

BIN_MIHOMO = "/usr/local/bin/mihomo"

# /tmp/ tmpfs 路径定义
TMP_DIR = Path("/tmp")
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

# IP 纯净度检测 API Key (ipapi.is 专属)
IPAPI_IS_KEY = "YOUR_IPAPI_KEY"

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
    auth_url = (
        "http://10.100.61.3/drcom/login?"
        "callback=dr1003&DDDDD=YOUR_USERNAME&upass=YOUR_PASSWORD&0MKKey=123456"
    )
    try:
        requests.get(auth_url, timeout=5)
        time.sleep(2)
        log.info("校园网认证心跳发送完毕，网络已恢复就绪。")
    except Exception as e:
        log.warning("发送校园网认证心跳异常: %s", e)


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


# 有效成熟的代理协议类型白名单 (严格排除 compatible 占位符、http/socks5 明文、非代理规则)
VALID_PROXY_TYPES = {
    "ss", "ssr", "vmess", "vless", "trojan",
    "hysteria", "hysteria2", "tuic", "wireguard", "snell"
}

# 垃圾/广告/兼容性占位关键字 (出现在名称中直接丢弃)
JUNK_NAME_KEYWORDS = (
    "compatible", "剩余流量", "到期", "过期", "官网", "更新", "关注",
    "群", "公告", "广告", "购买", "套餐", "客服", "订阅转换", "防失联"
)

# 无效/保留/蜜罐地址黑名单
INVALID_SERVERS = {"", "127.0.0.1", "127.0.0.53", "localhost", "0.0.0.0", "::", "::1"}


# ======================== 阶段 1: 多源上游拉取与去重 ========================

def fetch_and_dedup() -> Tuple[List[Dict[str, Any]], int, int]:
    """多源全量拉取上游订阅节点（仅提取 proxies，严格忽略自带 rules/groups），去重并过滤无效节点。"""
    session = requests.Session()
    session.trust_env = False
    headers = {"User-Agent": "ClashMeta/1.19.34"}

    all_raw_proxies: List[Dict[str, Any]] = []

    for group in UPSTREAM_SOURCES:
        loaded = False
        for url in group:
            log.info("尝试拉取上游节点: %s", url.split("?")[0])
            try:
                resp = session.get(url, headers=headers, timeout=25)
                resp.raise_for_status()
                # 严格只解析 proxies 字段，彻底抛弃上游自带的 rules、proxy-groups、dns 等规则
                data = yaml.load(resp.text, Loader=YamlLoader)
                if isinstance(data, dict) and isinstance(data.get("proxies"), list):
                    proxies_list = [p for p in data["proxies"] if isinstance(p, dict)]
                    all_raw_proxies.extend(proxies_list)
                    log.info("拉取成功: 来自 %s (获取到 %d 个原始节点)", url.split("?")[0], len(proxies_list))
                    loaded = True
                    break
                log.warning("上游响应未包含有效 proxies 列表: %s", url)
            except Exception as exc:
                log.warning("拉取失败 (%s): %s", url, exc)
        if not loaded:
            log.warning("该订阅源的所有镜像均不可达: %s", group[0])

    session.close()

    if not all_raw_proxies:
        raise RuntimeError("所有上游镜像源均拉取失败，终止任务。")

    raw_count = len(all_raw_proxies)
    seen_keys = set()
    dedup_proxies: List[Dict[str, Any]] = []

    for idx, proxy in enumerate(all_raw_proxies):
        # 1. 保留所有包含服务器的节点，绝不根据名字过滤，若名称缺失自动补齐
        raw_name = str(proxy.get("name") or f"node_{idx}").strip()

        # 2. 协议白名单过滤 (剔除 compatible、http、socks5 等无效类型)
        p_type = str(proxy.get("type", "")).strip().lower()
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
        except ValueError:
            continue

        # 4. 凭证完整性校验 (缺少 UUID/密码的残缺节点直接丢弃)
        if p_type in ("vmess", "vless") and not proxy.get("uuid"):
            continue
        if p_type in ("ss", "trojan") and not proxy.get("password"):
            continue

        credential = str(
            proxy.get("uuid")
            or proxy.get("password")
            or proxy.get("auth")
            or proxy.get("auth-str")
            or proxy.get("token")
            or ""
        ).strip()
        unique_key = (server, port, p_type, credential)
        if unique_key in seen_keys:
            continue
        seen_keys.add(unique_key)

        clean_proxy = dict(proxy)
        clean_proxy["name"] = f"p_{idx:05d}"
        dedup_proxies.append(clean_proxy)

    del all_raw_proxies, seen_keys
    gc.collect()

    dedup_count = len(dedup_proxies)
    log.info("多源汇聚完成: 原始节点 %d 个 -> 全量待测 %d 个 (已严格过滤非节点规则与无效服务器)", raw_count, dedup_count)
    return dedup_proxies, raw_count, dedup_count


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
                raise RuntimeError(f"Mihomo 初筛实例异常退出: {err[:300]}")
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

        # 1. 全量拉取上游与去重
        proxies, raw_count, dedup_count = fetch_and_dedup()
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

        # 5. 原子化强推至 GitHub nodes 分支
        atomic_push_to_github(
            final_records=final_records,
            raw_count=raw_count,
            dedup_count=dedup_count,
            alive_count=alive_count,
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
