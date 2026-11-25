#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
网络引导、Stage 0.5 异步 TCP 预筛、Git SSH Python3 ProxyCommand 与CDN代理回退模块。

专为 Qualcomm Snapdragon 410 (512MB RAM / Debian 11 ARM64 / 无 nc, ncat, socat) 设计：
1. BootstrapProxyManager: 按需以低内存限制 (GOMEMLIMIT=72MiB) 拉起临时 Mihomo，
   仅在直连拉取/推送/CDN刷新受阻时启用，并在 Stage 1 启动前确保彻底终止清理。
2. async_tcp_prefilter / run_tcp_prefilter: 基于 asyncio 的 Stage 0.5 TCP SYN 预筛，
   无条件放行 UDP 协议 (hysteria/hysteria2/hy2/tuic/wireguard) 与历史保活节点 (is_hist)。
3. Git SSH Python3 ProxyCommand 与 jsDelivr 缓存刷新回退: 纯 Python3 标准库实现的
   HTTP CONNECT / SOCKS5 隧道脚本，零外部工具依赖。
4. gh_mirrors / RECOMMENDED_EXTRA_SOURCES / fetch_urls_with_fallback: 高覆盖上游源与镜像回退工具。
"""

from __future__ import annotations

import asyncio
import atexit
import ipaddress
import json
import logging
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import weakref
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple

try:
    import resource  # type: ignore
except ImportError:  # pragma: no cover - Windows 无 resource 模块
    resource = None  # type: ignore

import requests

try:
    import yaml  # type: ignore

    try:
        _YamlLoader = yaml.CSafeLoader
        _YamlDumper = yaml.CSafeDumper
    except AttributeError:
        _YamlLoader = yaml.SafeLoader
        _YamlDumper = yaml.SafeDumper
except ImportError:  # pragma: no cover - 测试环境无 PyYAML 时优雅降级
    yaml = None  # type: ignore
    _YamlLoader = None
    _YamlDumper = None


log = logging.getLogger("net_bootstrap")

# ======================== 常量与默认配置 ========================

DEFAULT_BIN_MIHOMO = "/usr/local/bin/mihomo"
DEFAULT_SAVED_POOL_FILE = Path("/opt/clash-filter/saved_working_nodes.yaml")
DEFAULT_TMP_DIR = Path("/tmp")

DEFAULT_BOOTSTRAP_PORT = 19890
DEFAULT_BOOTSTRAP_API_PORT = 19891
DEFAULT_BOOTSTRAP_GOMEMLIMIT = "72MiB"
DEFAULT_BOOTSTRAP_GOGC = "25"
DEFAULT_BOOTSTRAP_MAX_NODES = 24
DEFAULT_BOOTSTRAP_PROBE_URL = "https://cp.cloudflare.com/generate_204"

# Stage 0.5 必须无条件保留放行的 UDP 协议
UDP_BYPASS_PROTOCOLS: frozenset[str] = frozenset({
    "hysteria",
    "hysteria2",
    "hy2",
    "tuic",
    "wireguard",
})

VALID_PROXY_TYPES: frozenset[str] = frozenset({
    "ss",
    "ssr",
    "vmess",
    "vless",
    "trojan",
    "hysteria",
    "hysteria2",
    "tuic",
    "wireguard",
    "snell",
    "anytls",
})

INVALID_SERVERS: frozenset[str] = frozenset({
    "",
    "127.0.0.1",
    "127.0.0.53",
    "localhost",
    "0.0.0.0",
    "::",
    "::1",
})

DEFAULT_PURGE_URLS: Tuple[str, ...] = (
    "https://purge.jsdelivr.net/gh/Komeiji6/Clash-config@nodes/proxies.yaml",
    "https://purge.jsdelivr.net/gh/Komeiji6/Clash-config@main/cmfa-merged-config.yaml",
)

_BOOTSTRAP_PROTO_PRIORITY: Dict[str, int] = {
    "vless": 0,
    "hysteria2": 1,
    "tuic": 2,
    "trojan": 3,
    "anytls": 4,
    "ss": 5,
    "vmess": 6,
    "hysteria": 7,
    "ssr": 8,
    "snell": 9,
    "wireguard": 10,
}


# ======================== 镜像与扩展高产上游源 ========================

def gh_mirrors(
    owner: str,
    repo: str,
    branch: str,
    path: str,
    extra_urls: Optional[Sequence[str]] = None,
) -> List[str]:
    """生成 GitHub 订阅源的多镜像故障转移列表 (raw 实时最新 -> ghfast 国内加速 -> jsDelivr CDN 兜底)。"""
    clean_path = path.lstrip("/")
    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{clean_path}"
    urls = [
        raw_url,
        f"https://ghfast.top/{raw_url}",
        f"https://cdn.jsdelivr.net/gh/{owner}/{repo}@{branch}/{clean_path}",
    ]
    if extra_urls:
        for u in extra_urls:
            if u and u not in urls:
                urls.append(u)
    return urls


# 配合 Stage 0.5 TCP 预筛使用的精简高产/现代协议源 (已剔除死链、重复源与零产出超大死池)
RECOMMENDED_EXTRA_SOURCES: List[Tuple[str, List[str]]] = [
    # 实测高产预检与 Mihomo 精选池 (Ariston-mihomo 实测单源贡献 33+ 存活)
    ("Edsel-Rahimi/Ariston-mihomo", gh_mirrors("Edsel-Rahimi", "Ariston", "data", "mihomo.yaml")),
    ("Edsel-Rahimi/Ariston-gold", gh_mirrors("Edsel-Rahimi", "Ariston", "data", "gold.txt")),
    # Hysteria2 / 轻量现代协议补充池
    ("MatinGhanbari/hysteria2", gh_mirrors("MatinGhanbari", "v2ray-configs", "main", "subscriptions/filtered/subs/hysteria2.txt")),
    ("MatinGhanbari/super", gh_mirrors("MatinGhanbari", "v2ray-configs", "main", "subscriptions/v2ray/super-sub.txt")),
    ("Reconss/mihomo", gh_mirrors("Reconss", "free-clash-nodes", "main", "config/mihomo.yaml")),
    ("awesome-vpn/clash", gh_mirrors("awesome-vpn", "awesome-vpn", "master", "clash.yaml")),
    ("ShatakVPN/light", gh_mirrors("ShatakVPN", "ConfigForge-V2Ray", "main", "configs/light.txt")),
]


def build_merged_sources(
    base_sources: Sequence[Tuple[str, List[str]]],
    extra_sources: Optional[Sequence[Tuple[str, List[str]]]] = None,
) -> List[Tuple[str, List[str]]]:
    """合并基础源与扩展源，按来源名称与首镜像 URL 去重并保持优先级顺序。"""
    extras = RECOMMENDED_EXTRA_SOURCES if extra_sources is None else extra_sources
    merged: List[Tuple[str, List[str]]] = []
    seen_names: Set[str] = set()
    seen_primary_urls: Set[str] = set()

    for name, urls in list(base_sources) + list(extras):
        if not name or not urls:
            continue
        primary = urls[0]
        if name in seen_names or primary in seen_primary_urls:
            continue
        seen_names.add(name)
        seen_primary_urls.add(primary)
        merged.append((name, list(urls)))
    return merged


# ======================== 通用代理/端口工具 ========================

def _proxy_fingerprint(proxy: Dict[str, Any]) -> str:
    """生成节点唯一指纹，兼容 filter_nodes.get_proxy_fingerprint。"""
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


def _find_free_port(preferred: Optional[int] = None, exclude: Optional[Set[int]] = None) -> int:
    """优先尝试绑定指定本地端口；若被占用则由内核分配空闲端口。"""
    excluded = exclude or set()
    if preferred is not None and preferred not in excluded:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", preferred))
                return preferred
            except OSError:
                pass
    for _ in range(10):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = int(s.getsockname()[1])
            if port not in excluded:
                return port
    raise RuntimeError("无法分配可用的本地监听端口")


def _can_tcp_connect(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _load_yaml_proxies(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    text = path.read_text(encoding="utf-8", errors="replace")
    if yaml is not None:
        data = yaml.load(text, Loader=_YamlLoader)
    else:
        data = json.loads(text)
    if isinstance(data, dict) and isinstance(data.get("proxies"), list):
        return [p for p in data["proxies"] if isinstance(p, dict)]
    return []


def _dump_yaml_config(path: Path, cfg: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as f:
        if yaml is not None:
            yaml.dump(cfg, f, Dumper=_YamlDumper, allow_unicode=True, sort_keys=False)
        else:
            # JSON 是 YAML 1.2 的严格子集，可被 Mihomo 直接解析
            json.dump(cfg, f, ensure_ascii=False, indent=2)


# ======================== 1. BootstrapProxyManager ========================

_ACTIVE_MANAGERS: "weakref.WeakSet[BootstrapProxyManager]" = weakref.WeakSet()


def cleanup_all_bootstrap_proxies() -> None:
    """终止所有仍在运行的引导 Mihomo 实例并清理其临时目录（供信号处理器调用）。"""
    for mgr in list(_ACTIVE_MANAGERS):
        try:
            mgr.stop()
        except Exception:
            pass


def cleanup_stale_tmp_artifacts(
    tmp_dir: Path | str = DEFAULT_TMP_DIR,
    *,
    max_age_sec: float = 3600.0,
) -> int:
    """清理由异常断电或 SIGKILL 遗留的过期 `/tmp/mihomo_bootstrap_*` 与 `clash_ssh_proxy_*.py`。"""
    tdir = Path(tmp_dir)
    if not tdir.exists():
        return 0
    now = time.time()
    removed = 0
    for pattern in ("mihomo_bootstrap_*", "clash_ssh_proxy_*.py"):
        for item in tdir.glob(pattern):
            try:
                mtime = item.stat().st_mtime
                if (now - mtime) < max_age_sec:
                    continue
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    item.unlink(missing_ok=True)
                removed += 1
            except OSError:
                pass
    return removed


def select_best_saved_nodes(
    proxies: Sequence[Dict[str, Any]],
    max_nodes: int = DEFAULT_BOOTSTRAP_MAX_NODES,
) -> List[Dict[str, Any]]:
    """
    从历史存活池中筛选最适合作为引导代理的高质量节点。

    优先保留“纯净”与“优选”标签节点及现代低开销协议，同时保持同类节点的既有顺序。
    """
    if max_nodes <= 0:
        return []

    candidates: List[Tuple[Tuple[int, int, int], Dict[str, Any]]] = []
    seen_fps: Set[str] = set()

    for idx, raw in enumerate(proxies):
        if not isinstance(raw, dict):
            continue
        p = dict(raw)
        p_type = str(p.get("type", "")).strip().lower()
        if p_type == "hy2":
            p_type = "hysteria2"
            p["type"] = p_type
        if p_type not in VALID_PROXY_TYPES:
            continue

        server = str(p.get("server", "")).strip().lower()
        if not server or server in INVALID_SERVERS or " " in server:
            continue
        try:
            port = int(p.get("port", 0))
            if not (1 <= port <= 65535):
                continue
            p["port"] = port
        except (TypeError, ValueError):
            continue

        if p_type in ("vmess", "vless") and not p.get("uuid"):
            continue
        if p_type in ("ss", "trojan", "hysteria", "hysteria2", "anytls") and not (
            p.get("password") or p.get("auth")
        ):
            continue
        if p_type == "tuic" and not (p.get("uuid") or p.get("token")):
            continue

        fp = _proxy_fingerprint(p)
        if fp in seen_fps:
            continue
        seen_fps.add(fp)

        name = str(p.get("name", ""))
        if ("非代理" in name or "未标代理" in name or "纯净" in name) and "原生" in name:
            tier_rank = 0
        elif "非代理" in name or "未标代理" in name or "纯净" in name or "原生" in name or "优选" in name:
            tier_rank = 1
        else:
            tier_rank = 2

        proto_rank = _BOOTSTRAP_PROTO_PRIORITY.get(p_type, 99)
        candidates.append(((tier_rank, proto_rank, idx), p))

    candidates.sort(key=lambda item: item[0])
    return [item[1] for item in candidates[:max_nodes]]


class BootstrapProxyManager:
    """
    按需启动的轻量级 Mihomo 引导代理上下文管理器。

    设计原则：
    - 仅在调用 `ensure_started()` / `start()` 时拉起（默认 `eager=False`），直连正常时不消耗任何内存；
    - 严格设置 `GOMEMLIMIT` (~72MiB) 与 `GOGC=25`，仅加载精选历史存活节点；
    - 使用独立 `mixed-port` 与 `external-controller` 端口，不与 Stage 1/2 冲突；
    - 退出上下文或调用 `stop()` 时必定 `terminate()`/`kill()` 并清理临时目录，确保 Stage 1 启动前零残留。
    """

    def __init__(
        self,
        saved_pool_file: Path | str = DEFAULT_SAVED_POOL_FILE,
        bin_mihomo: str = DEFAULT_BIN_MIHOMO,
        tmp_dir: Path | str = DEFAULT_TMP_DIR,
        proxy_port: Optional[int] = None,
        api_port: Optional[int] = None,
        max_nodes: int = DEFAULT_BOOTSTRAP_MAX_NODES,
        gomemlimit: str = DEFAULT_BOOTSTRAP_GOMEMLIMIT,
        gogc: str = DEFAULT_BOOTSTRAP_GOGC,
        ready_timeout: float = 15.0,
        probe_url: str = DEFAULT_BOOTSTRAP_PROBE_URL,
        eager: bool = False,
        strict: bool = False,
        preloaded_proxies: Optional[Sequence[Dict[str, Any]]] = None,
    ) -> None:
        self.saved_pool_file = Path(saved_pool_file)
        self.bin_mihomo = str(bin_mihomo)
        self.tmp_dir = Path(tmp_dir)
        self._preferred_proxy_port = proxy_port
        self._preferred_api_port = api_port
        self.max_nodes = max_nodes
        self.gomemlimit = gomemlimit
        self.gogc = gogc
        self.ready_timeout = ready_timeout
        self.probe_url = probe_url
        self.eager = eager
        self.strict = strict
        self._preloaded_proxies = list(preloaded_proxies) if preloaded_proxies is not None else None

        self._proc: Optional[subprocess.Popen[bytes]] = None
        self._work_dir: Optional[Path] = None
        self._proxy_port: Optional[int] = None
        self._api_port: Optional[int] = None
        self._selected_count: int = 0
        self._atexit_registered: bool = False

    @property
    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    @property
    def port(self) -> Optional[int]:
        return self._proxy_port if self.is_running else None

    @property
    def api_port(self) -> Optional[int]:
        return self._api_port if self.is_running else None

    @property
    def proxy_url(self) -> Optional[str]:
        if self.is_running and self._proxy_port is not None:
            return f"http://127.0.0.1:{self._proxy_port}"
        return None

    @property
    def socks5_url(self) -> Optional[str]:
        if self.is_running and self._proxy_port is not None:
            return f"socks5h://127.0.0.1:{self._proxy_port}"
        return None

    @property
    def requests_proxies(self) -> Optional[Dict[str, str]]:
        url = self.proxy_url
        if not url:
            return None
        return {"http": url, "https": url}

    @property
    def selected_count(self) -> int:
        return self._selected_count

    def __enter__(self) -> "BootstrapProxyManager":
        if self.eager:
            self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop()

    def _resolve_mihomo_binary(self) -> Optional[str]:
        p = Path(self.bin_mihomo)
        if p.exists():
            return str(p)
        found = shutil.which(self.bin_mihomo)
        return found

    def _load_candidate_nodes(self) -> List[Dict[str, Any]]:
        if self._preloaded_proxies is not None:
            raw_list = self._preloaded_proxies
        else:
            try:
                raw_list = _load_yaml_proxies(self.saved_pool_file)
            except Exception as exc:
                log.warning("[Bootstrap] 读取历史节点池失败 (%s): %s", self.saved_pool_file, exc)
                return []
        return select_best_saved_nodes(raw_list, max_nodes=self.max_nodes)

    def _build_mihomo_config(
        self,
        nodes: Sequence[Dict[str, Any]],
        proxy_port: int,
        api_port: int,
    ) -> Dict[str, Any]:
        sanitized: List[Dict[str, Any]] = []
        names: List[str] = []
        for i, node in enumerate(nodes):
            p_copy = dict(node)
            pname = f"bs_{i:03d}"
            p_copy["name"] = pname
            sanitized.append(p_copy)
            names.append(pname)

        return {
            "allow-lan": False,
            "bind-address": "127.0.0.1",
            "mixed-port": proxy_port,
            "mode": "rule",
            "log-level": "silent",
            "ipv6": False,
            "unified-delay": True,
            "tcp-concurrent": True,
            "external-controller": f"127.0.0.1:{api_port}",
            "geodata-mode": False,
            "geo-auto-update": False,
            "profile": {"store-selected": False, "store-fake-ip": False},
            "dns": {"enable": False},
            "proxies": sanitized,
            "proxy-groups": [
                {
                    "name": "BOOTSTRAP",
                    "type": "url-test",
                    "proxies": names,
                    "url": self.probe_url,
                    "interval": 120,
                    "tolerance": 100,
                    "lazy": False,
                }
            ],
            "rules": ["MATCH,BOOTSTRAP"],
        }

    def start(self) -> Optional[str]:
        """按需启动引导 Mihomo 代理并等待其 REST API 与 mixed-port 就绪。"""
        if self.is_running:
            return self.proxy_url

        bin_path = self._resolve_mihomo_binary()
        if not bin_path:
            msg = f"Mihomo 可执行文件不存在: {self.bin_mihomo}"
            if self.strict:
                raise FileNotFoundError(msg)
            log.warning("[Bootstrap] %s，无法启动引导代理", msg)
            return None

        selected = self._load_candidate_nodes()
        if not selected:
            msg = f"未找到可用的历史引导节点 ({self.saved_pool_file})"
            if self.strict:
                raise RuntimeError(msg)
            log.warning("[Bootstrap] %s，跳过启动引导代理", msg)
            return None

        proxy_port = _find_free_port(self._preferred_proxy_port or DEFAULT_BOOTSTRAP_PORT)
        api_port = _find_free_port(
            self._preferred_api_port or DEFAULT_BOOTSTRAP_API_PORT,
            exclude={proxy_port},
        )

        cleanup_stale_tmp_artifacts(self.tmp_dir)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        work_dir: Optional[Path] = None
        proc: Optional[subprocess.Popen[bytes]] = None
        try:
            work_dir = Path(
                tempfile.mkdtemp(prefix=f"mihomo_bootstrap_{os.getpid()}_", dir=str(self.tmp_dir))
            )
            self._work_dir = work_dir
            cfg_path = work_dir / "config.yaml"
            cfg = self._build_mihomo_config(selected, proxy_port=proxy_port, api_port=api_port)
            _dump_yaml_config(cfg_path, cfg)

            env = {
                **os.environ,
                "GOGC": str(self.gogc),
                "GOMEMLIMIT": str(self.gomemlimit),
            }

            log.info(
                "[Bootstrap] 按需启动轻量引导代理 (节点=%d, mixed-port=%d, api=%d, GOMEMLIMIT=%s)...",
                len(selected),
                proxy_port,
                api_port,
                self.gomemlimit,
            )

            popen_kwargs: Dict[str, Any] = {
                "env": env,
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.PIPE,
            }
            if os.name != "nt":
                popen_kwargs["start_new_session"] = True

            proc = subprocess.Popen(
                [bin_path, "-d", str(work_dir), "-f", str(cfg_path)],
                **popen_kwargs,
            )
            self._proc = proc
            self._proxy_port = proxy_port
            self._api_port = api_port
            self._selected_count = len(selected)
            _ACTIVE_MANAGERS.add(self)

            if not self._atexit_registered:
                atexit.register(self.stop)
                self._atexit_registered = True
        except Exception:
            self.stop()
            raise

        deadline = time.monotonic() + self.ready_timeout
        api_ready = False
        port_ready = False
        with requests.Session() as s:
            s.trust_env = False
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    err = ""
                    if proc.stderr:
                        try:
                            err = proc.stderr.read().decode("utf-8", errors="replace")[:500]
                        except Exception:
                            pass
                    self.stop()
                    msg = f"Bootstrap Mihomo 异常退出 (code={proc.returncode}): {err}"
                    if self.strict:
                        raise RuntimeError(msg)
                    log.warning("[Bootstrap] %s", msg)
                    return None

                if not api_ready:
                    try:
                        r = s.get(f"http://127.0.0.1:{api_port}/version", timeout=0.8)
                        if r.status_code == 200:
                            api_ready = True
                    except Exception:
                        pass

                if api_ready and not port_ready:
                    port_ready = _can_tcp_connect("127.0.0.1", proxy_port, timeout=0.5)

                if api_ready and port_ready:
                    # 留极短窗口让 url-test 完成首批测速选优
                    time.sleep(0.4)
                    log.info("[Bootstrap] 引导代理就绪: %s", self.proxy_url)
                    return self.proxy_url

                time.sleep(0.25)

        self.stop()
        msg = f"等待 Bootstrap Mihomo 就绪超时 ({self.ready_timeout:.1f}s)"
        if self.strict:
            raise TimeoutError(msg)
        log.warning("[Bootstrap] %s", msg)
        return None

    def ensure_started(self) -> Optional[str]:
        """`start()` 的幂等别名：若已运行则直接返回代理 URL，否则按需启动。"""
        return self.start()

    def stop(self) -> None:
        """强制优雅停止引导 Mihomo 进程组并彻底清理 /tmp 临时目录。"""
        proc = self._proc
        self._proc = None

        if self._atexit_registered:
            try:
                atexit.unregister(self.stop)
            except Exception:
                pass
            self._atexit_registered = False

        if proc is not None:
            try:
                if proc.poll() is None:
                    pid = getattr(proc, "pid", None)
                    use_pg = os.name != "nt" and isinstance(pid, int) and hasattr(os, "killpg")
                    if use_pg:
                        try:
                            os.killpg(pid, signal.SIGTERM)
                        except (ProcessLookupError, PermissionError, OSError):
                            proc.terminate()
                    else:
                        proc.terminate()
                    try:
                        proc.wait(timeout=3.0)
                    except subprocess.TimeoutExpired:
                        if use_pg:
                            try:
                                os.killpg(pid, signal.SIGKILL)
                            except (ProcessLookupError, PermissionError, OSError):
                                proc.kill()
                        else:
                            proc.kill()
                        try:
                            proc.wait(timeout=2.0)
                        except subprocess.TimeoutExpired:
                            pass
            except Exception as exc:
                log.warning("[Bootstrap] 终止引导进程异常: %s", exc)
            finally:
                if proc.stderr is not None:
                    try:
                        proc.stderr.close()
                    except Exception:
                        pass

        work_dir = self._work_dir
        self._work_dir = None
        self._proxy_port = None
        self._api_port = None
        self._selected_count = 0

        if work_dir is not None and work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)


# ======================== 2. Stage 0.5 Async TCP SYN 预筛 ========================

def is_udp_proxy(proxy: Dict[str, Any]) -> bool:
    """判断节点是否属于 UDP 传输协议（Stage 0.5 必须无条件放行）。"""
    p_type = str(proxy.get("type", "")).strip().lower()
    return p_type in UDP_BYPASS_PROTOCOLS


def is_historical_proxy(
    proxy: Dict[str, Any],
    historical_fps: Optional[Set[str]] = None,
    is_historical_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> bool:
    """判断节点是否属于历史保活池（Stage 0.5 必须无条件放行）。"""
    if proxy.get("is_hist") or proxy.get("_is_hist"):
        return True
    if historical_fps and _proxy_fingerprint(proxy) in historical_fps:
        return True
    if is_historical_fn is not None:
        try:
            if is_historical_fn(proxy):
                return True
        except Exception:
            pass
    return False


def _normalize_host_port(proxy: Dict[str, Any]) -> Optional[Tuple[str, int]]:
    server = str(proxy.get("server", "")).strip()
    if server.startswith("[") and server.endswith("]"):
        server = server[1:-1].strip()
    if not server or server.lower() in INVALID_SERVERS or " " in server:
        return None
    try:
        port = int(proxy.get("port", 0))
    except (TypeError, ValueError):
        return None
    if not (1 <= port <= 65535):
        return None
    return (server, port)


def _safe_prefilter_concurrency(requested: int) -> int:
    """结合系统 RLIMIT_NOFILE 上限钳位 Stage 0.5 并发数，防止耗尽 FD。"""
    cap = max(1, min(int(requested), 128))
    if resource is not None and hasattr(resource, "RLIMIT_NOFILE"):
        try:
            soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            if soft_limit and soft_limit > 0:
                cap = min(cap, max(8, int(soft_limit) // 4))
        except (ValueError, OSError):
            pass
    return max(1, cap)


async def _default_tcp_probe(host: str, port: int, timeout: float) -> bool:
    """执行单次非阻塞 TCP SYN 建连探测，成功后立即关闭释放文件描述符。"""
    writer: Optional[asyncio.StreamWriter] = None
    family = socket.AF_UNSPEC
    try:
        ip_obj = ipaddress.ip_address(host)
        family = socket.AF_INET6 if ip_obj.version == 6 else socket.AF_INET
    except ValueError:
        # 棒子端禁用 IPv6，域名仅做 IPv4 A 记录解析以防止 AAAA 解析拖垮 DNS/离线卡死
        family = socket.AF_INET

    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, family=family),
            timeout=timeout,
        )
        return True
    except (asyncio.TimeoutError, OSError, ValueError):
        return False
    except Exception:
        return False
    finally:
        if writer is not None:
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=0.3)
            except Exception:
                pass


async def async_tcp_prefilter(
    proxies: Sequence[Dict[str, Any]],
    *,
    concurrency: int = 64,
    timeout: float = 1.8,
    historical_fps: Optional[Set[str]] = None,
    is_historical_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    stats: Optional[Dict[str, Any]] = None,
    probe_fn: Optional[Callable[[str, int, float], Awaitable[bool]]] = None,
) -> List[Dict[str, Any]]:
    """
    Stage 0.5 异步 TCP SYN 预筛：
    - 对 TCP 类节点并发探测 `server:port` 可达性（严格受 `concurrency` 信号量限制，默认 64，适配 4 核棒子 ulimit）；
    - 无条件放行所有 UDP 协议 (`hysteria`, `hysteria2`, `hy2`, `tuic`, `wireguard`)；
    - 无条件放行所有历史保活池节点 (`is_hist`/`_is_hist` 或命中 `historical_fps`/`is_historical_fn`)；
    - 相同 `(server, port)` 自动合并探测一次，保持返回列表的原始相对顺序。
    """
    t0 = time.monotonic()
    total = len(proxies)
    if total == 0:
        if stats is not None:
            stats.update({
                "total": 0,
                "kept": 0,
                "dropped": 0,
                "unique_endpoints": 0,
                "tcp_ok": 0,
                "tcp_fail": 0,
                "udp_bypassed": 0,
                "hist_bypassed": 0,
                "elapsed_s": 0.0,
            })
        return []

    max_conc = _safe_prefilter_concurrency(concurrency)
    probe_impl = probe_fn or _default_tcp_probe

    # 第一遍：识别无条件放行节点与需要探测的唯一 (host, port)
    bypass_flags: List[bool] = []
    endpoints: List[Optional[Tuple[str, int]]] = []
    unique_targets: Set[Tuple[str, int]] = set()
    udp_bypassed = 0
    hist_bypassed = 0

    for p in proxies:
        if is_udp_proxy(p):
            udp_bypassed += 1
            bypass_flags.append(True)
            endpoints.append(None)
        elif is_historical_proxy(p, historical_fps=historical_fps, is_historical_fn=is_historical_fn):
            hist_bypassed += 1
            bypass_flags.append(True)
            endpoints.append(None)
        else:
            bypass_flags.append(False)
            ep = _normalize_host_port(p)
            endpoints.append(ep)
            if ep is not None:
                unique_targets.add(ep)

    sem = asyncio.Semaphore(max_conc)
    results_map: Dict[Tuple[str, int], bool] = {}

    async def _bounded_probe(ep: Tuple[str, int]) -> Tuple[Tuple[str, int], bool]:
        host, port = ep
        async with sem:
            try:
                ok = await probe_impl(host, port, timeout)
            except asyncio.CancelledError:
                raise
            except Exception:
                ok = False
            return ep, ok

    if unique_targets:
        tasks = [asyncio.create_task(_bounded_probe(ep)) for ep in unique_targets]
        try:
            for ep, ok in await asyncio.gather(*tasks):
                results_map[ep] = ok
        except BaseException:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    kept: List[Dict[str, Any]] = []
    tcp_ok = 0
    tcp_fail = 0
    for p, bypassed, ep in zip(proxies, bypass_flags, endpoints):
        if bypassed:
            kept.append(p)
        elif ep is not None and results_map.get(ep, False):
            tcp_ok += 1
            kept.append(p)
        else:
            tcp_fail += 1

    elapsed = round(time.monotonic() - t0, 3)
    summary = {
        "total": total,
        "kept": len(kept),
        "dropped": total - len(kept),
        "unique_endpoints": len(unique_targets),
        "tcp_ok": tcp_ok,
        "tcp_fail": tcp_fail,
        "udp_bypassed": udp_bypassed,
        "hist_bypassed": hist_bypassed,
        "elapsed_s": elapsed,
    }
    if stats is not None:
        stats.update(summary)

    log.info(
        "[Stage 0.5] TCP 预筛完成 (耗时 %.2fs): 总计 %d -> 保留 %d (TCP存活=%d, TCP丢弃=%d, UDP放行=%d, 历史放行=%d)",
        elapsed,
        total,
        len(kept),
        tcp_ok,
        tcp_fail,
        udp_bypassed,
        hist_bypassed,
    )
    return kept


def run_tcp_prefilter(
    proxies: Sequence[Dict[str, Any]],
    *,
    concurrency: int = 64,
    timeout: float = 1.8,
    historical_fps: Optional[Set[str]] = None,
    is_historical_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """`async_tcp_prefilter` 的同步封装入口，返回 `(保留节点列表, 统计字典)`。"""
    stats: Dict[str, Any] = {}
    coro = async_tcp_prefilter(
        proxies,
        concurrency=concurrency,
        timeout=timeout,
        historical_fps=historical_fps,
        is_historical_fn=is_historical_fn,
        stats=stats,
    )
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        kept = asyncio.run(coro)
        return kept, stats

    # 若当前线程已有运行中的事件循环，则放到独立线程执行
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        kept = ex.submit(asyncio.run, coro).result()
    return kept, stats


# ======================== 3. Python3 SSH ProxyCommand & CDN Purge ========================

_SSH_PROXY_TUNNEL_SCRIPT = r'''#!/usr/bin/env python3
"""Minimal stdlib HTTP CONNECT / SOCKS5 proxy tunnel for OpenSSH ProxyCommand (no nc/socat)."""
import os
import socket
import struct
import sys
import threading
import urllib.parse


def _read_http_connect_header(sock):
    buf = bytearray()
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(1)
        if not chunk:
            raise RuntimeError("EOF while waiting for HTTP CONNECT response")
        buf.extend(chunk)
        if len(buf) > 16384:
            raise RuntimeError("HTTP CONNECT response header too large")
    idx = buf.index(b"\r\n\r\n") + 4
    header = bytes(buf[:idx]).decode("latin-1", errors="replace")
    remainder = bytes(buf[idx:])
    status_line = header.split("\r\n", 1)[0]
    parts = status_line.split()
    if len(parts) < 2 or parts[1] != "200":
        raise RuntimeError(f"HTTP CONNECT failed: {status_line}")
    return remainder


def _connect_http(proxy_host, proxy_port, target_host, target_port, timeout=15.0):
    sock = socket.create_connection((proxy_host, proxy_port), timeout=timeout)
    sock.settimeout(None)
    host_hdr = f"[{target_host}]" if ":" in target_host else target_host
    req = (
        f"CONNECT {host_hdr}:{target_port} HTTP/1.1\r\n"
        f"Host: {host_hdr}:{target_port}\r\n"
        "Proxy-Connection: Keep-Alive\r\n\r\n"
    ).encode("ascii")
    sock.sendall(req)
    remainder = _read_http_connect_header(sock)
    return sock, remainder


def _recv_exact(sock, n):
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise RuntimeError("Unexpected EOF from SOCKS5 proxy")
        out.extend(chunk)
    return bytes(out)


def _connect_socks5(proxy_host, proxy_port, target_host, target_port, timeout=15.0):
    sock = socket.create_connection((proxy_host, proxy_port), timeout=timeout)
    sock.settimeout(None)
    sock.sendall(b"\x05\x01\x00")
    ver_method = _recv_exact(sock, 2)
    if ver_method != b"\x05\x00":
        raise RuntimeError(f"SOCKS5 auth negotiation failed: {ver_method!r}")

    addr_payload = None
    for family, atyp in ((socket.AF_INET, b"\x01"), (socket.AF_INET6, b"\x04")):
        try:
            addr_payload = atyp + socket.inet_pton(family, target_host)
            break
        except OSError:
            pass
    if addr_payload is None:
        host_bytes = target_host.encode("utf-8")
        if len(host_bytes) > 255:
            raise ValueError("Target hostname too long for SOCKS5")
        addr_payload = b"\x03" + bytes([len(host_bytes)]) + host_bytes

    req = b"\x05\x01\x00" + addr_payload + struct.pack("!H", int(target_port))
    sock.sendall(req)

    hdr = _recv_exact(sock, 4)
    if hdr[0] != 5 or hdr[1] != 0:
        raise RuntimeError(f"SOCKS5 CONNECT failed: rep={hdr[1]}")
    atyp = hdr[3]
    if atyp == 1:
        _recv_exact(sock, 4)
    elif atyp == 3:
        ln = _recv_exact(sock, 1)[0]
        _recv_exact(sock, ln)
    elif atyp == 4:
        _recv_exact(sock, 16)
    else:
        raise RuntimeError(f"Unknown SOCKS5 ATYP: {atyp}")
    _recv_exact(sock, 2)
    return sock, b""


def _write_all(fd, data):
    view = memoryview(data)
    written = 0
    while written < len(view):
        n = os.write(fd, view[written:])
        if n <= 0:
            raise OSError("os.write returned 0")
        written += n


def _relay(proxy_url, target_host, target_port):
    parsed = urllib.parse.urlparse(proxy_url)
    scheme = (parsed.scheme or "http").lower()
    proxy_host = parsed.hostname or "127.0.0.1"
    proxy_port = parsed.port or (1080 if scheme.startswith("socks") else 8080)

    if scheme in ("socks5", "socks5h", "socks"):
        sock, remainder = _connect_socks5(proxy_host, proxy_port, target_host, target_port)
    else:
        sock, remainder = _connect_http(proxy_host, proxy_port, target_host, target_port)

    if remainder:
        _write_all(1, remainder)

    stop_event = threading.Event()

    def stdin_to_sock():
        try:
            while not stop_event.is_set():
                data = os.read(0, 32768)
                if not data:
                    break
                sock.sendall(data)
        except Exception:
            pass
        finally:
            try:
                sock.shutdown(socket.SHUT_WR)
            except Exception:
                pass

    t = threading.Thread(target=stdin_to_sock, daemon=True)
    t.start()

    try:
        while True:
            data = sock.recv(32768)
            if not data:
                break
            _write_all(1, data)
    except Exception:
        pass
    finally:
        stop_event.set()
        try:
            sock.close()
        except Exception:
            pass


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.stderr.write("Usage: ssh_proxy.py <proxy_url> <target_host> <target_port>\n")
        sys.exit(2)
    _relay(sys.argv[1], sys.argv[2], int(sys.argv[3]))
'''


def write_ssh_proxy_script(
    script_path: Optional[Path | str] = None,
    tmp_dir: Path | str = DEFAULT_TMP_DIR,
) -> Path:
    """将纯 Python3 HTTP CONNECT/SOCKS5 SSH 隧道脚本写入临时文件并赋予可执行权限。"""
    if script_path is not None:
        target = Path(script_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_SSH_PROXY_TUNNEL_SCRIPT, encoding="utf-8")
    else:
        tdir = Path(tmp_dir)
        tdir.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix="clash_ssh_proxy_", suffix=".py", dir=str(tdir))
        os.close(fd)
        target = Path(name)
        target.write_text(_SSH_PROXY_TUNNEL_SCRIPT, encoding="utf-8")

    try:
        os.chmod(target, 0o700)
    except OSError:
        pass
    return target


def cleanup_ssh_proxy_script(script_path: Optional[Path | str]) -> None:
    """安全删除临时 SSH ProxyCommand 脚本。"""
    if script_path is None:
        return
    try:
        Path(script_path).unlink(missing_ok=True)
    except OSError:
        pass


def build_git_ssh_command(
    ssh_key_path: str,
    proxy_url: Optional[str] = None,
    script_path: Optional[Path | str] = None,
    connect_timeout: int = 15,
    use_ssh_443: Optional[bool] = None,
    python_bin: Optional[str] = None,
) -> str:
    """
    构造用于 `GIT_SSH_COMMAND` 的 ssh 命令字符串。

    当 `proxy_url` 非空且提供了 `script_path` 时，注入纯 Python3 `ProxyCommand`
    （不依赖 `nc`/`ncat`/`socat`）；当 `use_ssh_443` 为 `True`（或未指定且启用了 `proxy_url`）时，
    自动切换至 `ssh.github.com:443` 以绕过校园网/代理对 22 端口的封锁。
    """
    effective_443 = bool(proxy_url) if use_ssh_443 is None else bool(use_ssh_443)
    parts = [
        "ssh",
        "-i",
        shlex.quote(str(ssh_key_path)),
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={int(connect_timeout)}",
        "-o",
        "ServerAliveInterval=10",
        "-o",
        "ServerAliveCountMax=3",
    ]

    if effective_443:
        parts.extend(["-o", "Hostname=ssh.github.com", "-o", "Port=443"])

    if proxy_url:
        if script_path is None:
            raise ValueError("启用 proxy_url 时必须提供 script_path")
        py = python_bin or ("python3" if os.name != "nt" else sys.executable)
        script_posix = Path(script_path).as_posix()
        proxy_cmd = f"{shlex.quote(py)} {shlex.quote(script_posix)} {shlex.quote(proxy_url)} %h %p"
        parts.extend(["-o", f"ProxyCommand={shlex.quote(proxy_cmd)}"])

    return " ".join(parts)


@contextmanager
def git_ssh_env(
    ssh_key_path: str,
    proxy_url: Optional[str] = None,
    tmp_dir: Path | str = DEFAULT_TMP_DIR,
    connect_timeout: int = 15,
    use_ssh_443: Optional[bool] = None,
    base_env: Optional[Dict[str, str]] = None,
    python_bin: Optional[str] = None,
) -> Iterator[Dict[str, str]]:
    """
    上下文管理器：按需生成临时 Python3 ProxyCommand 脚本并构建包含 `GIT_SSH_COMMAND` 的环境变量字典，
    退出作用域时自动清理临时脚本。
    """
    script_path: Optional[Path] = None
    cleanup_cb: Optional[Callable[[], None]] = None
    try:
        if proxy_url:
            script_path = write_ssh_proxy_script(tmp_dir=tmp_dir)
            _path_ref = script_path

            def _cleanup_script() -> None:
                cleanup_ssh_proxy_script(_path_ref)

            cleanup_cb = _cleanup_script
            atexit.register(cleanup_cb)
        cmd = build_git_ssh_command(
            ssh_key_path=ssh_key_path,
            proxy_url=proxy_url,
            script_path=script_path,
            connect_timeout=connect_timeout,
            use_ssh_443=use_ssh_443,
            python_bin=python_bin,
        )
        env = dict(base_env if base_env is not None else os.environ)
        env["GIT_SSH_COMMAND"] = cmd
        yield env
    finally:
        if cleanup_cb is not None:
            try:
                atexit.unregister(cleanup_cb)
            except Exception:
                pass
        cleanup_ssh_proxy_script(script_path)


def purge_jsdelivr_urls(
    urls: Optional[Sequence[str]] = None,
    *,
    proxy_url: Optional[str] = None,
    proxy_manager: Optional[BootstrapProxyManager] = None,
    timeout: float = 10.0,
) -> Dict[str, Dict[str, Any]]:
    """
    刷新 jsDelivr CDN 边缘缓存：优先直连请求；若直连失败且提供代理（或引导管理器），
    自动按需拉起本地代理完成回退刷新。
    """
    target_urls = list(urls) if urls is not None else list(DEFAULT_PURGE_URLS)
    results: Dict[str, Dict[str, Any]] = {}
    headers = {"User-Agent": "ClashMeta/1.19.34"}

    with requests.Session() as s:
        s.trust_env = False
        for url in target_urls:
            direct_err: Optional[str] = None
            try:
                resp = s.get(url, headers=headers, timeout=timeout)
                if 200 <= resp.status_code < 400:
                    results[url] = {"ok": True, "via": "direct", "status": resp.status_code, "error": None}
                    continue
                direct_err = f"HTTP {resp.status_code}"
            except Exception as exc:
                direct_err = str(exc)

            eff_proxy = proxy_url
            if not eff_proxy and proxy_manager is not None:
                eff_proxy = proxy_manager.ensure_started()

            if eff_proxy:
                try:
                    proxies = {"http": eff_proxy, "https": eff_proxy}
                    resp = s.get(url, headers=headers, proxies=proxies, timeout=timeout)
                    if 200 <= resp.status_code < 400:
                        results[url] = {"ok": True, "via": "proxy", "status": resp.status_code, "error": None}
                        continue
                    results[url] = {
                        "ok": False,
                        "via": "proxy",
                        "status": resp.status_code,
                        "error": f"direct={direct_err}; proxy=HTTP {resp.status_code}",
                    }
                    continue
                except Exception as exc:
                    results[url] = {
                        "ok": False,
                        "via": "proxy",
                        "status": None,
                        "error": f"direct={direct_err}; proxy={exc}",
                    }
                    continue

            results[url] = {"ok": False, "via": "direct", "status": None, "error": direct_err}

    return results


def fetch_urls_with_fallback(
    urls: Sequence[str],
    *,
    proxy_url: Optional[str] = None,
    proxy_manager: Optional[BootstrapProxyManager] = None,
    timeout: float = 12.0,
    headers: Optional[Dict[str, str]] = None,
    validator: Optional[Callable[[str], bool]] = None,
) -> Tuple[Optional[str], Optional[str], str]:
    """
    先直连依次尝试 `urls`；若全部失败且提供 `proxy_url` 或 `proxy_manager`，
    按需启用本地引导代理并优先从上游源（包括 raw.githubusercontent.com）回退拉取。

    返回 `(response_text, matched_url, transport_used)`，其中 `transport_used` 为
    `"direct"` / `"proxy"` / `"none"`。
    """
    req_headers = headers or {
        "User-Agent": "ClashMeta/1.19.34",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    def _try_list(url_list: Sequence[str], proxies: Optional[Dict[str, str]]) -> Tuple[Optional[str], Optional[str]]:
        req_timeout = (min(float(timeout), 4.5), float(timeout))
        with requests.Session() as s:
            s.trust_env = False
            for url in url_list:
                try:
                    resp = s.get(url, headers=req_headers, proxies=proxies, timeout=req_timeout)
                    resp.raise_for_status()
                    text = resp.text
                    if validator is None or validator(text):
                        return text, url
                except Exception:
                    continue
        return None, None

    text, matched = _try_list(urls, None)
    if text is not None:
        return text, matched, "direct"

    eff_proxy = proxy_url
    if not eff_proxy and proxy_manager is not None:
        eff_proxy = proxy_manager.ensure_started()

    if eff_proxy:
        # 代理通道下优先尝试直连 raw.githubusercontent.com（绕过缓存陈旧或国内反代故障）
        raw_first = sorted(urls, key=lambda u: 0 if "raw.githubusercontent.com" in u else 1)
        text, matched = _try_list(raw_first, {"http": eff_proxy, "https": eff_proxy})
        if text is not None:
            return text, matched, "proxy"

    return None, None, "none"


__all__ = [
    "BootstrapProxyManager",
    "DEFAULT_BIN_MIHOMO",
    "DEFAULT_BOOTSTRAP_API_PORT",
    "DEFAULT_BOOTSTRAP_GOGC",
    "DEFAULT_BOOTSTRAP_GOMEMLIMIT",
    "DEFAULT_BOOTSTRAP_MAX_NODES",
    "DEFAULT_BOOTSTRAP_PORT",
    "DEFAULT_BOOTSTRAP_PROBE_URL",
    "DEFAULT_PURGE_URLS",
    "DEFAULT_SAVED_POOL_FILE",
    "DEFAULT_TMP_DIR",
    "RECOMMENDED_EXTRA_SOURCES",
    "UDP_BYPASS_PROTOCOLS",
    "async_tcp_prefilter",
    "build_git_ssh_command",
    "build_merged_sources",
    "cleanup_all_bootstrap_proxies",
    "cleanup_ssh_proxy_script",
    "cleanup_stale_tmp_artifacts",
    "fetch_urls_with_fallback",
    "gh_mirrors",
    "git_ssh_env",
    "is_historical_proxy",
    "is_udp_proxy",
    "purge_jsdelivr_urls",
    "run_tcp_prefilter",
    "select_best_saved_nodes",
    "write_ssh_proxy_script",
]
