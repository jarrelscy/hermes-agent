"""Per-thread webtop container pool.

Gives each Hermes ``task_id`` its own linuxserver/webtop container (isolated
XFCE desktop + chromium + X server), so concurrent threads never collide on
the same browser tab / keyboard / mouse.

Architecture (mirrors the Phase 1 single-container setup, generalized):

  - Each container runs its own X server at ``DISPLAY=:1`` (inside the
    container namespace) and its own chromium launched with
    ``--remote-debugging-port=9222`` bound to the container loopback.
  - ``cdp-forward.py`` (autostarted inside the container) bridges
    ``0.0.0.0:9223 -> 127.0.0.1:9222`` so the host can reach chromium's
    DevTools endpoint through a published port.  The host maps a unique
    ``127.0.0.1:<cdp_port>`` -> container ``9223`` per task.
  - The agent's browser tool connects to ``http://127.0.0.1:<cdp_port>``;
    CDP ``Input.*`` gives per-thread browser keyboard/mouse for free.
  - The container's ``/config`` volume is a host directory per task
    (``<CONFIG_ROOT>/<slot>``), so each thread's chromium profile (M365
    logins, cookies) persists across restarts and evictions.

Selection: enabled when ``HERMES_WEBTOP_POOL`` is ``1``/``true``.  When
disabled, :func:`get_cdp_url` returns ``""`` and the caller falls back to
the single global ``browser.cdp_url`` (Phase 1 behaviour).

Env knobs:
  HERMES_WEBTOP_POOL              enable (1/true)
  HERMES_WEBTOP_POOL_SIZE         max concurrent *running* containers (10)
  HERMES_WEBTOP_POOL_CDP_BASE     host CDP port base (9222)
  HERMES_WEBTOP_POOL_NOVNC_BASE   host noVNC port base (3210)
  HERMES_WEBTOP_POOL_IMAGE        webtop image
                                  (lscr.io/linuxserver/webtop:ubuntu-xfce)
  HERMES_WEBTOP_POOL_CONFIG_ROOT  host dir for per-task /config volumes
                                  (~/hermes-gui-sandbox/agents)
  HERMES_WEBTOP_POOL_KASADA_DOMAINS   comma-sep Kasada domains to prewarm
                                  (realestate.com.au)
  HERMES_WEBTOP_POOL_KASADA_PREWARM_ON_START  1 = mint eagerly at cold start
                                  (default 0 = lazy, mint on first protected
                                  navigate via ensure_warm)

All docker calls go through ``subprocess`` with timeouts; failures raise so
the browser tool can fall back rather than silently misroute.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()
# task_id -> entry dict:
#   slot, container, config_dir, cdp_port, novnc_port,
#   running(bool), last_used(float)
_MAPPING: Dict[str, Dict] = {}
_STATE_FILE: Optional[Path] = None
_LOADED = False

# Idle reaping is OFF by default: a webtop persists for the life of its thread
# (across idle periods, WS disconnects, and gateway restarts) so its per-thread
# profile — logins + the minted Kasada KP_UIDz — stays warm and ready. Teardown
# is driven by THREAD DELETION only (delete_task, wired to db.delete_session),
# not by inactivity. Set HERMES_WEBTOP_POOL_IDLE_TTL to a positive number of
# seconds to opt back into idle reaping (stop+rm the container, keep the volume)
# after that much inactivity. The LRU cap (_evict_lru at pool SIZE) is a separate
# capacity valve and always applies.
_IDLE_TTL = float(os.environ.get("HERMES_WEBTOP_POOL_IDLE_TTL", "0"))
_REAPER_STARTED = False
_REAPER_LOCK = threading.Lock()

_SEED_DIR = Path(__file__).resolve().parent / "webtop_pool_seed"
# Fall back to a sibling seed dir shipped alongside this module.  The seed
# files (cdp-forward.py, chromium-cdp.sh, autostart .desktop) are written by
# the installer; see _ensure_seed_files().


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.environ.get(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "on")


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "").strip())
    except (TypeError, ValueError):
        return default


def _str_env(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v or default


def pool_enabled() -> bool:
    return _bool_env("HERMES_WEBTOP_POOL", False)


def _config_root() -> Path:
    raw = os.environ.get("HERMES_WEBTOP_POOL_CONFIG_ROOT", "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path.home() / "hermes-gui-sandbox" / "agents"


def _image() -> str:
    return os.environ.get(
        "HERMES_WEBTOP_POOL_IMAGE",
        "lscr.io/linuxserver/webtop:ubuntu-xfce",
    ).strip() or "lscr.io/linuxserver/webtop:ubuntu-xfce"


# ── Kasada prewarm config ───────────────────────────────────────────────────
# KP_UIDz (Kasada) expires in ~24h. The pool mints a fresh cookie per container
# so each webtop has its own warm profile. Domains list is comma-separated; the
# recipe generalizes to any Kasada-protected domain (google -> click-thru ->
# humanize on target). Prewarm-on-start is opt-in (eager); default is lazy —
# mint on the first protected navigate via ensure_warm().

def _kasada_domains() -> list:
    raw = os.environ.get("HERMES_WEBTOP_POOL_KASADA_DOMAINS", "").strip()
    if not raw:
        return ["realestate.com.au"]
    return [d.strip().lower() for d in raw.split(",") if d.strip()]


def _prewarm_on_start() -> bool:
    # Default OFF (lazy). Eager mint-on-create is a trap: it runs the ~28s
    # no-CDP Kasada mint SYNCHRONOUSLY inside _create_container, so the caller's
    # get_cdp_url() blocks for the whole mint (browser first-navigate stalls),
    # AND the mint can't succeed once the browser attaches CDP to that same
    # chrome during the sensor window. The correct path is LAZY: mint on the
    # first navigate to a Kasada domain via ensure_warm() (kill+relaunch+mint
    # with the no-CDP window, THEN navigate on the warm cookie). Non-Kasada
    # sites (e.g. the realestate cron scrapes domain.com.au) never mint at all.
    # Now that the eager mint runs in a background daemon (non-blocking —
    # get_cdp_url returns immediately, the mint proceeds off the create path and
    # ensure_warm coordinates via _warm_lock), eager is the default so a fresh
    # container is minting its KP_UIDz before the agent's first navigate.
    # Set HERMES_WEBTOP_POOL_KASADA_PREWARM_ON_START=0 to force lazy.
    return _bool_env("HERMES_WEBTOP_POOL_KASADA_PREWARM_ON_START", True)


def _apex(domain: str) -> str:
    d = (domain or "").strip().lower().lstrip(".")
    if d.startswith("www."):
        d = d[4:]
    return d


def _kasada_startup_url(domain: str) -> str:
    """The startup URL for the startup-URL mint: chrome's own initial nav to
    this URL runs the Kasada sensor undisturbed (no CDP WS client) and mints a
    validated KP_UIDz. See BREAKTHROUGH #4."""
    apex = _apex(domain)
    return f"https://www.{apex}/"


def _is_kasada_host(host: str) -> bool:
    """True if `host` (or a suffix of it) is a configured Kasada domain."""
    if not host:
        return False
    h = host.strip().lower().lstrip(".")
    if h.startswith("www."):
        h = h[4:]
    return any(h == d or h.endswith("." + d) for d in _kasada_domains())


# Per-container mint locks so a background prewarm and an on-demand ensure_warm
# never run two mints on the same profile at once (they'd collide). Not part of
# the persisted state (a Lock isn't JSON-serializable).
_WARM_LOCKS: Dict[str, "threading.Lock"] = {}


def _warm_lock(container: str) -> "threading.Lock":
    lk = _WARM_LOCKS.get(container)
    if lk is None:
        lk = threading.Lock()
        _WARM_LOCKS[container] = lk
    return lk


def _state_file() -> Path:
    return _config_root() / "pool_state.json"


def _slot_for(task_id: str) -> str:
    return hashlib.sha1(task_id.encode("utf-8")).hexdigest()[:12]


# ── seed files (identical to Phase 1; container-internal paths) ────────────

_CDP_FORWARD_PY = """#!/usr/bin/env python3
import socket, threading, select, sys

LISTEN = ("0.0.0.0", 9223)
TARGET = ("127.0.0.1", 9222)

def pipe(a, b):
    try:
        while True:
            r, _, _ = select.select([a, b], [], [])
            if a in r:
                d = a.recv(65536)
                if not d: break
                b.sendall(d)
            if b in r:
                d = b.recv(65536)
                if not d: break
                a.sendall(d)
    except Exception:
        pass
    finally:
        try: a.close()
        except: pass
        try: b.close()
        except: pass

def handle(c):
    try:
        t = socket.create_connection(TARGET, timeout=5)
        threading.Thread(target=pipe, args=(c, t), daemon=True).start()
        threading.Thread(target=pipe, args=(t, c), daemon=True).start()
    except Exception as e:
        sys.stderr.write(f"cdp-forward connect failed: {e}\\n")
        c.close()

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(LISTEN)
    s.listen(64)
    sys.stderr.write(f"cdp-forward {LISTEN} -> {TARGET}\\n")
    while True:
        c, _ = s.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()

if __name__ == "__main__":
    main()
"""

# Direct in-container CDP forwarder for the Kasada mint flow. The pool's
# cdp-forward.py (select-based, above) can drop the websocket under google's
# event burst even with WsMux draining; the proven mint path uses this
# threading-based forwarder bound to 0.0.0.0:9298, reachable from the host via
# the container's bridge IP (no socat / extra host port needed). Used only for
# the prewarm mint; the browser tool still goes through the published cdp_port.
_DIRECT_FWD_PY = """import socket,threading
def fwd(a,b):
    try:
        while 1:
            d=a.recv(65536)
            if not d: break
            b.sendall(d)
    except: pass
    finally:
        try:a.close()
        except:pass
        try:b.close()
        except:pass
s=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);s.bind(('0.0.0.0',9298));s.listen(64)
while 1:
    c,_=s.accept();u=socket.create_connection(('127.0.0.1',9222))
    threading.Thread(target=fwd,args=(c,u),daemon=True).start();threading.Thread(target=fwd,args=(u,c),daemon=True).start()
"""


def _container_bridge_ip(container: str) -> Optional[str]:
    try:
        r = _run(["docker", "inspect", "-f",
                  "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                  container], timeout=10)
        ip = (r.stdout or "").strip()
        return ip or None
    except Exception:
        return None


def _direct_cdp_url(entry: Dict):
    """Return (http_url, cleanup) for a direct in-container CDP forwarder.

    The mint flow needs a low-congestion path to chrome's 9222; the pool's
    published cdp_port goes through cdp-forward.py (select-based) which can
    drop the websocket under google's event burst. This starts the threading
    forwarder (seed: /config/bin/direct-fwd.py, 0.0.0.0:9298 -> 127.0.0.1:9222)
    and reaches it via the container's bridge IP. ``cleanup`` stops it.
    """
    container = entry["container"]
    fwd = "/config/bin/direct-fwd.py"
    # Kill a stale forwarder from a prior mint, then start fresh.
    _run(["docker", "exec", container, "bash", "-lc",
          "pkill -f direct-fwd.py 2>/dev/null; sleep 0.3"], timeout=10, check=False)
    _run(["docker", "exec", "-d", container, "/usr/bin/python3", fwd], timeout=10, check=False)
    time.sleep(1.0)
    ip = _container_bridge_ip(container)
    url = f"http://{ip}:9298" if ip else None

    def cleanup():
        try:
            _run(["docker", "exec", container, "bash", "-lc",
                  "pkill -f direct-fwd.py 2>/dev/null"], timeout=10, check=False)
        except Exception:
            pass
    return url, cleanup


# LD_PRELOAD shim that makes navigator.deviceMemory and hardwareConcurrency
# report NATIVE values (8 / 8) so no JS override is needed. Kasada detects JS
# native-function/getter overrides via Error().stack (2026-07-10 root cause),
# so we must fix these at the libc level instead of via document-start scripts.
# Chrome 150 reads deviceMemory via sysconf(_SC_PHYS_PAGES) * page_size, NOT
# /proc/meminfo (the earlier --memory=8g cgroup and /proc/meminfo bind-mount
# attempts both failed for this reason). hardwareConcurrency uses
# sysconf(_SC_NPROCESSORS_ONLN/_CONF) + sched_getaffinity. The shim also
# redirects open("/proc/meminfo") to a fake file and rewrites sysinfo().totalram
# for any reader that uses those. Scoped to the chrome process only (set in
# chromium-cdp.sh, NOT container-wide) so X/xfce/at-spi see real values.
_FAKEMEM_C = r"""/* fakemem.c — native deviceMemory=8 / hardwareConcurrency=8 for Chrome.
 * Built at container init by 10-install-tools.sh -> /config/bin/fakemem.so.
 * LD_PRELOAD=/config/bin/fakemem.so is exported in chromium-cdp.sh (chrome only).
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/sysinfo.h>
#include <sched.h>

static const char *FAKE_PATH = "/config/bin/fake_meminfo";
static int match(const char *p){ return p && strcmp(p, "/proc/meminfo") == 0; }
typedef int (*open_fn)(const char *, int, ...);

int open(const char *p, int fl, ...){
    static open_fn r = NULL; if (!r) r = dlsym(RTLD_NEXT, "open");
    if (match(p)) return r(FAKE_PATH, fl, 0666);
    return r(p, fl);
}
int open64(const char *p, int fl, ...){
    static open_fn r = NULL; if (!r) r = dlsym(RTLD_NEXT, "open64");
    if (match(p)) return r(FAKE_PATH, fl, 0666);
    return r(p, fl);
}
int __open_2(const char *p, int fl){
    static int (*r)(const char *, int) = NULL; if (!r) r = dlsym(RTLD_NEXT, "__open_2");
    if (match(p)) return r(FAKE_PATH, fl);
    return r(p, fl);
}
int access(const char *p, int m){
    static int (*r)(const char *, int) = NULL; if (!r) r = dlsym(RTLD_NEXT, "access");
    if (match(p)) return 0;
    return r(p, m);
}
int stat(const char *p, struct stat *st){
    static int (*r)(const char *, struct stat *) = NULL; if (!r) r = dlsym(RTLD_NEXT, "stat");
    if (match(p)) return r(FAKE_PATH, st);
    return r(p, st);
}
int __xstat(int ver, const char *p, struct stat *st){
    static int (*r)(int, const char *, struct stat *) = NULL; if (!r) r = dlsym(RTLD_NEXT, "__xstat");
    if (match(p)) return r(ver, FAKE_PATH, st);
    return r(ver, p, st);
}
int sysinfo(struct sysinfo *info){
    static int (*r)(struct sysinfo *) = NULL; if (!r) r = dlsym(RTLD_NEXT, "sysinfo");
    int ret = r(info);
    if (ret == 0 && info){
        unsigned long mu = info->mem_unit ? info->mem_unit : 1;
        info->totalram  = 8388608ULL / mu;
        info->freeram   = 4000000ULL / mu;
        info->bufferram = 100000ULL  / mu;
        info->sharedram = 0;
        info->totalswap = 0; info->freeswap = 0;
    }
    return ret;
}
long sysconf(int name){
    static long (*r)(int) = NULL; if (!r) r = dlsym(RTLD_NEXT, "sysconf");
    if (name == 85 /*_SC_PHYS_PAGES*/)      return 2097152L;  /* 8GB / 4KB */
    if (name == 86 /*_SC_AVPHYS_PAGES*/)    return 1000000L;
    if (name == 84 /*_SC_NPROCESSORS_ONLN*/) return 8;
    if (name == 83 /*_SC_NPROCESSORS_CONF*/) return 8;
    return r(name);
}
int sched_getaffinity(pid_t pid, size_t cpusetsize, cpu_set_t *mask){
    static int (*r)(pid_t, size_t, cpu_set_t *) = NULL; if (!r) r = dlsym(RTLD_NEXT, "sched_getaffinity");
    int ret = r(pid, cpusetsize, mask);
    if (ret == 0 && mask){
        unsigned char *m = (unsigned char *)mask;
        size_t i; for (i = 0; i < cpusetsize; i++) m[i] = 0;
        for (i = 0; i < 8 && i < cpusetsize * 8; i++) m[i / 8] |= (1 << (i % 8));
    }
    return ret;
}
"""

_CHROMIUM_CDP_SH = """#!/bin/sh
# Launch the per-thread browser with CDP on 127.0.0.1:9222 (bridged out by
# cdp-forward.py). Prefer real Google Chrome (branded UA + build id) over
# Ubuntu Chromium: consumer sites behind Kasada/DataDome score the Chromium
# build + Linux combo as bot-like. Falls back to chromium until chrome is
# installed by the init script.
export DISPLAY=:1
# Kasada scores timezone (TZ=UTC is a server/bot tell; Australia/Sydney matches
# the realestate.com.au audience and is a plausible real-user tz). Set LANG/LC
# to en_AU.UTF-8 so navigator.language resolves to en-AU natively. NOTE:
# Intl.DateTimeFormat().resolvedOptions().locale still resolves to en-GB via
# Chrome ICU's en_AU->en_GB parent chain — that is the EXPECTED native value for
# a real Australian Chrome user; do NOT override it to en-AU (the override is
# detected via Error().stack -> Kasada reject, 2026-07-10).
export TZ=Australia/Sydney
export LANG=en_AU.UTF-8
export LC_ALL=en_AU.UTF-8
export LANGUAGE=en_AU:en
# Native deviceMemory=8 / hardwareConcurrency=8 via the LD_PRELOAD shim. Scoped
# to this chrome process only (not exported container-wide in docker run) so the
# X server, xfwm4 and at-spi see real CPU/RAM. The webtop image ignores
# /config/custom-cont-init.d (it scans /custom-cont-init.d on the ephemeral root
# FS), so boot-time init hooks never fire. Instead we build the shim HERE, as
# abc, right before launching chrome: gcc ships in the image, /config/bin is
# abc-writable, and this runs at every chrome start so it self-heals. Chrome 150
# reads deviceMemory via sysconf(_SC_PHYS_PAGES) (NOT /proc/meminfo), so the shim
# hooks sysconf + sched_getaffinity; the fake_meminfo file covers older paths.
if [ -s /config/bin/fakemem.c ] && command -v gcc >/dev/null 2>&1; then
  if [ ! -s /config/bin/fakemem.so ] || \
     [ /config/bin/fakemem.c -nt /config/bin/fakemem.so ]; then
    gcc -shared -fPIC -O2 -o /config/bin/fakemem.so /config/bin/fakemem.c -ldl \
      >/config/.fakemem-build.log 2>&1 || true
  fi
  if [ ! -s /config/bin/fake_meminfo ] || \
     [ /config/bin/fakemem.c -nt /config/bin/fake_meminfo ]; then
    { echo "MemTotal:       8388608 kB";
      echo "MemFree:        4000000 kB";
      echo "MemAvailable:   7000000 kB";
      echo "Buffers:         100000 kB";
      echo "Cached:         2000000 kB";
      echo "SwapTotal:             0 kB";
      echo "SwapFree:              0 kB";
    } > /config/bin/fake_meminfo
  fi
fi
if [ -s /config/bin/fakemem.so ]; then
  export LD_PRELOAD=/config/bin/fakemem.so
fi

# Never blank the virtual display (idle blank -> black computer_use captures).
xset s off >/dev/null 2>&1 || true
xset s noblank >/dev/null 2>&1 || true
# Report a plausible physical display size (~109 dpi 27in 1440p). A 0mm /
# odd-dpi screen is a headless/VM tell to fingerprinting vendors.
xrandr --fbmm 597x336 >/dev/null 2>&1 || true
# Ensure a window manager is up so window chrome gives outer > inner (the
# headless tell is outer == inner / outer == 0). The XFCE session normally
# starts xfwm4; this is a belt-and-braces fallback for bare containers.
if ! pgrep -x xfwm4 >/dev/null 2>&1; then
  xfwm4 --daemon >/dev/null 2>&1 &
fi

# Use real Google Chrome only — never Ubuntu Chromium. A branded Chrome
# fingerprint ("Google Chrome" UA brand, productSub 20030107, the real Chrome
# plugin set) is what Kasada-class bot protection expects; Chromium differs in
# userAgentData.brands and the vendor fingerprint and gets rejected. The pool
# installs google-chrome-stable via _ensure_chrome_in_container (docker exec,
# as root, apt install from a host-cached deb) right after container create,
# then launches this script as abc. On a fresh create the autostart fires this
# script BEFORE chrome is installed — exit cleanly here so no browser runs yet;
# the pool's explicit launch (after install) starts Chrome. On a docker restart
# (not recreate) chrome is already installed on the root FS, so the autostart
# launches it directly. chromium is deliberately NOT a fallback.
if ! command -v google-chrome-stable >/dev/null 2>&1; then
  echo "chromium-cdp.sh: google-chrome-stable not installed yet; exiting (pool will install + relaunch)" >>/config/browser.log 2>&1
  exit 0
fi
BROWSER=google-chrome-stable
UDD=/config/.config/chrome-hermes
mkdir -p "$UDD"
# Optional startup URL (arg 1). When set, chrome's OWN initial navigation goes
# to this URL — used by the Kasada prewarm mint: the sensor runs undisturbed
# (no CDP WS client attached during the window) so the full challenge solves
# and mints a validated KP_UIDz. Empty -> chrome launches to about:blank
# (the default; CDP driving attaches later). See BREAKTHROUGH #4.
STARTUP_URL="${1:-}"

# Clear stale singleton locks left by a previous container hostname.
HN=$(hostname)
for f in SingletonLock SingletonCookie SingletonSocket; do
  if [ -e "$UDD/$f" ]; then
    tgt=$(readlink "$UDD/$f" 2>/dev/null || echo "")
    case "$tgt" in
      $HN-*|"$HN") : ;;
      *) rm -f "$UDD/$f" ;;
    esac
  fi
done

# Chrome-defaults for Kasada-class bot protection:
#   --disable-blink-features=AutomationControlled : navigator.webdriver stays
#     undefined and the "controlled by automated software" infobar is suppressed.
#   NO --start-maximized : a maximized window reports outerWidth==innerWidth
#     (no visible chrome) which is the classic headless tell. A slightly-under-
#     desktop window-size lets xfwm4 add a title bar -> outer > inner.
#   --password-store=basic : avoid a gnome-keyring prompt on the session.
# Prefer the sandbox: dropping --no-sandbox is itself an automation signal.
# If the container lacks unprivileged user namespaces chrome dies at once;
# detect that and relaunch permissively so the browser always comes up.
"$BROWSER" --window-size=2556,1398 --remote-debugging-port=9222 \\
  --remote-debugging-address=0.0.0.0 --remote-allow-origins=* \\
  --user-data-dir="$UDD" --no-first-run --no-default-browser-check \\
  --lang=en-AU --accept-lang=en-AU,en \\
  --disable-blink-features=AutomationControlled \\
  --disable-features=AutomationControlled \\
  --password-store=basic \\
  --enable-unsafe-swiftshader --use-gl=angle --use-angle=swiftshader-webgl \\
  --in-process-gpu --disable-gpu-sandbox \\
  --disable-session-crashed-bubble --restore-last-session=false $STARTUP_URL \\
  >/config/browser.log 2>&1 &
PID=$!
sleep 5
if kill -0 "$PID" 2>/dev/null; then
  wait "$PID"
else
  exec "$BROWSER" --no-sandbox --window-size=2556,1398 --remote-debugging-port=9222 \\
    --remote-debugging-address=0.0.0.0 --remote-allow-origins=* \\
    --user-data-dir="$UDD" --no-first-run --no-default-browser-check \\
    --disable-blink-features=AutomationControlled \\
    --disable-features=AutomationControlled \\
    --password-store=basic \\
    --enable-unsafe-swiftshader --use-gl=angle --use-angle=swiftshader-webgl \\
    --in-process-gpu --disable-gpu-sandbox \\
    --disable-session-crashed-bubble --restore-last-session=false $STARTUP_URL
fi
"""

_CHROMIUM_DESKTOP = """[Desktop Entry]
Type=Application
Name=Chromium (CDP)
Exec=/config/bin/chromium-cdp.sh
Icon=chromium
Terminal=false
X-GNOME-Autostart-enabled=true
"""

_CDP_FORWARD_DESKTOP = """[Desktop Entry]
Type=Application
Name=CDP Forward (9223->9222)
Exec=python3 /config/bin/cdp-forward.py
Terminal=false
X-GNOME-Autostart-enabled=true
"""

# NOTE: this script is written to /config/custom-cont-init.d/ for documentation
# and manual invocation, but the linuxserver webtop image does NOT scan that
# path at boot (its init-custom-files stage scans /custom-cont-init.d on the
# ephemeral root FS only -> "[custom-init] No custom files found, skipping").
# So nothing here runs automatically. The pool instead installs Chrome via
# _ensure_chrome_in_container (docker exec) and builds the fakemem shim inside
# chromium-cdp.sh at every chrome start. Kept for the xclip/at-spi apt installs
# in case the image path is ever fixed or the script is run by hand.
_INSTALL_TOOLS_SH = """#!/bin/sh
# Idempotent container-boot setup. Intended to run as root before the X session.
# Safe to re-run. (See module note above: NOT auto-run by this image.)

if command -v apt-get >/dev/null 2>&1; then
  if ! command -v xclip >/dev/null 2>&1 || ! command -v xsel >/dev/null 2>&1; then
    apt-get update -qq >/dev/null 2>&1 || true
    apt-get install -y -qq xclip xsel >/dev/null 2>&1 || true
  fi
  if ! command -v at-spi-bus-launcher >/dev/null 2>&1; then
    apt-get update -qq >/dev/null 2>&1 || true
    apt-get install -y -qq at-spi2-core >/dev/null 2>&1 || true
  fi
  # Install real Google Chrome from a deb cached in /config (persists across
  # container recreates; downloaded once, ~130 MB). Gives a branded Chrome
  # fingerprint instead of Ubuntu Chromium.
  if ! command -v google-chrome-stable >/dev/null 2>&1; then
    mkdir -p /config/pkgcache
    DEB=/config/pkgcache/google-chrome-stable.deb
    if [ ! -s "$DEB" ]; then
      curl -fsSL -o "$DEB" https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb 2>/dev/null \\
        || wget -qO "$DEB" https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb 2>/dev/null || true
    fi
    if [ -s "$DEB" ]; then
      apt-get update -qq >/dev/null 2>&1 || true
      apt-get install -y -qq "$DEB" >/dev/null 2>&1 || true
    fi
  fi
fi
# Start the AT-SPI bus for the session user if not already up (best-effort).
pgrep -u abc at-spi-bus-launcher >/dev/null 2>&1 || \\
  su -s /bin/sh abc -c "DISPLAY=:1 at-spi-bus-launcher >/config/.at-spi.log 2>&1 &" 2>/dev/null || true
exit 0
"""

_SEED_FILES = {
    "bin/cdp-forward.py": _CDP_FORWARD_PY,
    "bin/direct-fwd.py": _DIRECT_FWD_PY,
    "bin/chromium-cdp.sh": _CHROMIUM_CDP_SH,
    "bin/fakemem.c": _FAKEMEM_C,
    "bin/cua-driver": None,  # marker — binary copied at container-create, not a text seed
    ".config/autostart/chromium-cdp.desktop": _CHROMIUM_DESKTOP,
    ".config/autostart/cdp-forward.desktop": _CDP_FORWARD_DESKTOP,
    "custom-cont-init.d/10-install-tools.sh": _INSTALL_TOOLS_SH,
}


def _ensure_seed(config_dir: Path) -> None:
    """Write the autostart + bin scripts into a per-task /config dir.

    Idempotent: overwrites so every container runs the current script version.
    Entries with ``content=None`` are markers (e.g. the cua-driver binary,
    which is copied separately at container-create time) and are skipped.
    """
    for rel, content in _SEED_FILES.items():
        if content is None:
            continue
        p = config_dir / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        if rel.endswith(".sh") or rel.endswith(".py"):
            p.chmod(0o755)


# ── docker helpers ─────────────────────────────────────────────────────────

def _run(cmd, timeout=120, check=True):
    """Run a docker command, return CompletedProcess. Raises on failure."""
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        stdin=subprocess.DEVNULL,
        check=check,
    )


def _container_exists(name: str) -> bool:
    try:
        _run(["docker", "inspect", name], timeout=10, check=True)
        return True
    except Exception:
        return False


def _container_running(name: str) -> bool:
    try:
        r = _run(["docker", "inspect", "-f", "{{.State.Running}}", name],
                 timeout=10, check=True)
        return r.stdout.strip() == "true"
    except Exception:
        return False


def _used_host_ports() -> set:
    """Host ports currently bound by any docker container."""
    ports = set()
    try:
        r = _run(["docker", "ps", "--format", "{{.Ports}}"], timeout=15)
        for tok in r.stdout.split():
            # tokens like 127.0.0.1:9222->9223/tcp, 0.0.0.0:3210->3000/tcp
            if "->" in tok:
                host = tok.split("->", 1)[0]
                if ":" in host:
                    try:
                        ports.add(int(host.rsplit(":", 1)[1]))
                    except ValueError:
                        pass
    except Exception:
        pass
    return ports


def _pick_port(base: int, size: int, used: set, reserved: set) -> int:
    """Lowest free port in [base, base+size) not in used/reserved."""
    for i in range(size):
        p = base + i
        if p in used or p in reserved:
            continue
        return p
    raise RuntimeError(f"webtop pool exhausted: no free port in [{base}, {base+size})")


def _reserve_ports(entry: Dict):
    """Pick + reserve host CDP/noVNC ports and stamp them on ``entry``.

    MUST be called under ``_LOCK``. Reserves against docker-bound ports AND
    every in-flight entry that already has ports assigned (running OR
    mid-cold-boot), so two tasks cold-booting in parallel never grab the same
    host port. Stamping the ports on the entry before releasing the lock is
    what lets the slow ``_create_container`` run OUTSIDE the global lock without
    a port race.
    """
    size = _int_env("HERMES_WEBTOP_POOL_SIZE", 10)
    cdp_base = _int_env("HERMES_WEBTOP_POOL_CDP_BASE", 9222)
    novnc_base = _int_env("HERMES_WEBTOP_POOL_NOVNC_BASE", 3210)
    used = _used_host_ports()
    reserved = {e["cdp_port"] for e in _MAPPING.values() if e.get("cdp_port")}
    reserved |= {e["novnc_port"] for e in _MAPPING.values() if e.get("novnc_port")}
    cdp_port = _pick_port(cdp_base, size, used, reserved)
    novnc_port = _pick_port(novnc_base, size, used, reserved | {cdp_port})
    entry["cdp_port"] = cdp_port
    entry["novnc_port"] = novnc_port
    return cdp_port, novnc_port


def _host_cua_driver_path() -> Optional[str]:
    """Resolve the host cua-driver binary path (symlink target), or None."""
    for cand in (
        os.environ.get("HERMES_CUA_DRIVER_CMD", "").strip(),
        os.path.expanduser("~/.local/bin/cua-driver"),
    ):
        if cand and os.path.exists(cand):
            try:
                return os.path.realpath(cand)
            except Exception:
                return cand
    # Follow the install dir layout used by the upstream installer.
    pkg = os.path.expanduser("~/.cua-driver/packages/current/cua-driver")
    if os.path.exists(pkg):
        try:
            return os.path.realpath(pkg)
        except Exception:
            return pkg
    return None


def _ensure_cua_driver_in_container(container: str, config_dir: Path) -> None:
    """Copy the host cua-driver binary into the container at /config/bin/.

    Idempotent: skips if the in-container binary already matches the host
    version. /config is bind-mounted to config_dir on the host, so the copy
    also lands on the host filesystem and survives container recreate.
    """
    src = _host_cua_driver_path()
    if not src:
        logger.warning("webtop pool: cua-driver not found on host; "
                       "computer_use will be unavailable in container %s", container)
        return
    try:
        # Host destination mirrors the container path (/config/bin).
        host_bin = config_dir / "bin" / "cua-driver"
        host_bin.parent.mkdir(parents=True, exist_ok=True)
        need_copy = True
        if host_bin.exists():
            try:
                if os.path.realpath(host_bin) == src and os.path.exists(src):
                    need_copy = False  # already a copy/symlink of the same binary
                elif _files_same(host_bin, src):
                    need_copy = False
            except Exception:
                pass
        if need_copy:
            shutil.copy2(src, host_bin)
            host_bin.chmod(0o755)
        # Ensure it's present inside the container at /config/bin/cua-driver
        # (bind mount makes host config_dir/bin == container /config/bin, so
        # this is usually a no-op, but docker cp guarantees it even if the
        # mount hadn't propagated yet at container start).
        try:
            _run(["docker", "exec", container, "test", "-x", "/config/bin/cua-driver"],
                 timeout=10)
        except Exception:
            try:
                _run(["docker", "cp", host_bin, f"{container}:/config/bin/cua-driver"],
                     timeout=30)
                _run(["docker", "exec", container, "chmod", "+x",
                      "/config/bin/cua-driver"], timeout=10)
            except Exception as e:
                logger.warning("webtop pool: docker cp cua-driver into %s failed: %s",
                               container, e)
        # The agent-cursor overlay is a fullscreen ARGB window; without a
        # compositor (xfwm4 compositing is off in webtop) it renders opaque
        # black, so every capture after the first session call photographs
        # the overlay instead of the desktop. Persisted per-container in
        # /config/.cua-driver, so one docker exec per container lifetime.
        try:
            _run(["docker", "exec", "-e", "HOME=/config", container,
                  "/config/bin/cua-driver", "config", "set",
                  "agent_cursor_enabled", "false"], timeout=15)
        except Exception as e:
            logger.warning("webtop pool: disable agent cursor in %s failed: %s",
                           container, e)
    except Exception as e:
        logger.warning("webtop pool: ensure cua-driver in %s failed: %s", container, e)


def _files_same(a: str, b: str) -> bool:
    try:
        import hashlib
        ha = hashlib.sha1(open(a, "rb").read()).hexdigest()
        hb = hashlib.sha1(open(b, "rb").read()).hexdigest()
        return ha == hb
    except Exception:
        return False


# ── state persistence ──────────────────────────────────────────────────────

def _load_state() -> None:
    global _LOADED, _MAPPING
    if _LOADED:
        return
    _LOADED = True
    sf = _state_file()
    try:
        data = json.loads(sf.read_text())
        if isinstance(data, dict):
            _MAPPING = {k: dict(v) for k, v in data.items()}
    except Exception:
        _MAPPING = {}
    # Reconcile running state with actual docker state.
    for entry in _MAPPING.values():
        name = entry.get("container")
        if name and _container_running(name):
            entry["running"] = True
        else:
            entry["running"] = False
            entry["cdp_port"] = None
            entry["novnc_port"] = None
    # Surface orphan /config volume dirs (no pool entry AND no container) so
    # they can be pruned manually. We never auto-delete a volume — it may hold
    # a warm profile / site logins the operator wants back.
    try:
        known_slots = {e.get("slot") for e in _MAPPING.values()}
        root = _config_root()
        if root.is_dir():
            for d in sorted(root.iterdir()):
                if (d.is_dir() and d.name not in known_slots
                        and not _container_exists(f"hermes-wt-{d.name}")):
                    logger.info("webtop pool: orphan volume dir %s "
                                "(no task/container; prune manually)", d)
    except Exception:
        pass


def _save_state() -> None:
    sf = _state_file()
    try:
        sf.parent.mkdir(parents=True, exist_ok=True)
        sf.write_text(json.dumps(_MAPPING, indent=2))
    except Exception as e:
        logger.warning("webtop pool: could not persist state: %s", e)


# ── container lifecycle ────────────────────────────────────────────────────

def _host_chrome_deb() -> Optional[str]:
    """Path to a cached google-chrome .deb on the host; download once if absent."""
    cache = _config_root().parent / "pkgcache"
    deb = cache / "google-chrome-stable.deb"
    if deb.is_file() and deb.stat().st_size > 1_000_000:
        return str(deb)
    try:
        cache.mkdir(parents=True, exist_ok=True)
        _run(["curl", "-fsSL", "-o", str(deb),
              "https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb"],
             timeout=180)
        if deb.is_file() and deb.stat().st_size > 1_000_000:
            return str(deb)
    except Exception as e:
        logger.warning("webtop pool: chrome deb download failed: %s", e)
    return None


def _ensure_chrome_in_container(container: str) -> None:
    """Install real Google Chrome in a container from a host-cached .deb.

    This LinuxServer image ignores /config/custom-cont-init.d, so boot-time
    install hooks never fire; and apt installs live on the ephemeral container
    root FS (lost on docker rm), so the pool (re)installs on every create. A
    branded Chrome fingerprint beats Ubuntu Chromium on bot-protected sites.
    Best-effort: on failure the chromium-cdp seed falls back to chromium.
    """
    try:
        try:
            _run(["docker", "exec", container, "sh", "-c",
                  "command -v google-chrome-stable"], timeout=10)
            return  # already installed (survives docker restart)
        except Exception:
            pass
        deb = _host_chrome_deb()
        if not deb:
            logger.warning("webtop pool: no chrome deb; %s stays on chromium", container)
            return
        _run(["docker", "cp", deb, f"{container}:/tmp/chrome.deb"], timeout=60)
        _run(["docker", "exec", container, "bash", "-c",
              "apt-get update -qq >/dev/null 2>&1; "
              "apt-get install -y -qq /tmp/chrome.deb >/dev/null 2>&1"], timeout=180)
    except Exception as e:
        logger.warning("webtop pool: ensure chrome in %s failed: %s", container, e)


def _wait_x_session(container: str, timeout: float = 60.0) -> bool:
    """Block until the XFCE session + Xvfb :1 are up inside the container.

    Unlike _wait_desktop_ready this does NOT key on the CDP endpoint, because we
    no longer autostart a browser on fresh create (chromium-cdp.sh exits until
    google-chrome is installed). Used after `docker run` and before the pool
    installs + launches Chrome, so apt/X races don't trip the launch.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = _run(["docker", "exec", container, "bash", "-lc",
                      "pgrep -x xfce4-session >/dev/null 2>&1 && "
                      "DISPLAY=:1 xdpyinfo >/dev/null 2>&1"], timeout=10,
                     check=False)
            if r.returncode == 0:
                return True
        except Exception:
            pass
        time.sleep(1.0)
    logger.warning("webtop pool: %s X session not ready after %.0fs; proceeding", container, timeout)
    return False


def _launch_chrome(container: str, url: Optional[str] = None) -> None:
    """Launch Google Chrome inside the container as the abc session user.

    Runs the seed chromium-cdp.sh, which (now that google-chrome-stable is
    installed) starts Chrome with the Kasada recipe flags + LD_PRELOAD shim.
    Detached via `docker exec -d` so the pool call returns immediately; the
    script backgrounds chrome itself and exits. Best-effort: if a chrome is
    already on 9222 (e.g. a docker-restart autostart already fired) this is a
    harmless no-op duplicate that exits at the singleton lock.

    ``url``: optional startup URL. When set, chrome's OWN initial navigation
    loads this URL — used by the Kasada prewarm mint so the sensor runs
    undisturbed (no CDP WS client attached during the window) and the full
    challenge solves, minting a validated KP_UIDz. See BREAKTHROUGH #4. Empty
    -> chrome launches to about:blank (default; CDP driving attaches later).
    """
    try:
        cmd = ["docker", "exec", "-d",
               "-e", "DISPLAY=:1", "-e", "HOME=/config", "-u", "1000",
               container, "/config/bin/chromium-cdp.sh"]
        if url:
            cmd.append(url)
        _run(cmd, timeout=15)
    except Exception as e:
        logger.warning("webtop pool: launch chrome in %s failed: %s", container, e)


def _create_container(entry: Dict) -> None:
    """docker run -d for a new (or evicted) task, assigning fresh host ports."""
    # Ports are normally pre-reserved by _ensure_running under _LOCK (so this
    # slow create can run OUTSIDE the global lock without a port race). Fall
    # back to reserving here only for direct/legacy callers.
    cdp_port = entry.get("cdp_port")
    novnc_port = entry.get("novnc_port")
    if not cdp_port or not novnc_port:
        with _LOCK:
            cdp_port, novnc_port = _reserve_ports(entry)
    name = entry["container"]
    config_dir = Path(entry["config_dir"])
    config_dir.mkdir(parents=True, exist_ok=True)
    _ensure_seed(config_dir)
    # Remove a leftover stopped container of the same name (evicted but not rm'd).
    if _container_exists(name) and not _container_running(name):
        try:
            _run(["docker", "rm", "-f", name], timeout=20)
        except Exception:
            pass
    cmd = [
        "docker", "run", "-d",
        "--name", name,
        "--restart", "unless-stopped",
        # /dev/shm defaults to 64M, which crashes Chrome's font_data_service
        # renderer under load ("No space left on device (28)" -> devtools ws
        # dies). 2G is the decisive transport fix (2026-07-10).
        "--shm-size=2g",
        "-p", f"127.0.0.1:{cdp_port}:9223",
        "-p", f"127.0.0.1:{novnc_port}:3000",
        "-v", f"{config_dir}:/config",
        "-e", "PUID=1000",
        "-e", "PGID=1000",
        # Australia/Sydney matches the realestate.com.au audience; TZ is a
        # scored Kasada signal (UTC is a server tell). LANG/LC set the session
        # locale so chrome --lang=en-AU resolves navigator.language natively.
        "-e", f"TZ={_str_env('HERMES_WEBTOP_POOL_TZ', 'Australia/Sydney')}",
        "-e", "LANG=en_AU.UTF-8",
        "-e", "LC_ALL=en_AU.UTF-8",
        "-e", "LANGUAGE=en_AU:en",
        "-e", "DISPLAY=:1",
        # Default the virtual display to 2K. webtop's svc-de honours
        # SELKIES_MANUAL_WIDTH/HEIGHT (xrandr mode on top of the huge Xvfb
        # virtual screen); without it the desktop comes up 1024x768.
        "-e", f"SELKIES_MANUAL_WIDTH={_int_env('HERMES_WEBTOP_POOL_WIDTH', 2560)}",
        "-e", f"SELKIES_MANUAL_HEIGHT={_int_env('HERMES_WEBTOP_POOL_HEIGHT', 1440)}",
        _image(),
    ]
    _run(cmd, timeout=180)
    # Wait for the X session (no browser autostarts on fresh create now), then
    # install Google Chrome as root, then launch it as the session user. This
    # is the "just Chrome, not chromium-then-chrome" path: chromium-cdp.sh
    # refuses to fall back to Ubuntu Chromium, so it exits at the fresh-create
    # autostart; we install Chrome and launch it explicitly here. On a
    # docker-restart the autostart launches Chrome directly (it survives on the
    # root FS across restart, just not across docker rm).
    _wait_x_session(name, timeout=60)
    # Phase 2b: copy cua-driver into the container so computer_use can run
    # inside it (bound to its own :1 X server). /config is bind-mounted, so
    # placing the binary at /config/bin/cua-driver persists across container
    # recreates — no re-copy needed on eviction/reactivation. The binary only
    # needs libX11/libXi/libXext which the webtop image already ships.
    _ensure_cua_driver_in_container(name, config_dir)
    _ensure_chrome_in_container(name)
    # Kasada prewarm (eager, default ON): launch chrome with the first
    # configured domain as the STARTUP URL so its own initial navigation runs
    # the sensor undisturbed (no CDP WS client attached during the window) and
    # mints a validated KP_UIDz into the profile. The mint is SYNCHRONOUS here
    # because the no-CDP window is mandatory — if a task attached CDP during
    # the ~28s sensor window the mint would fail (BREAKTHROUGH #4). So the
    # container is not handed to any task (running=True set after the mint)
    # until the cookie is minted. Cold-start pays ~28s per domain; the default
    # single-domain case adds ~28s. Set HERMES_WEBTOP_POOL_KASADA_PREWARM_ON_START=0
    # for lazy (mint on first protected navigate via ensure_warm).
    domains = _kasada_domains()
    do_prewarm = _prewarm_on_start() and bool(domains)
    if do_prewarm:
        _launch_chrome(name, url=_kasada_startup_url(domains[0]))
    else:
        _launch_chrome(name)
    # Mark the container USABLE and persist state the moment Chrome is launched,
    # BEFORE any Kasada mint. The mint is slow (~28s no-CDP window + verify) and
    # can fail/stall; in the old order (running=True + _save_state AFTER the
    # mint) any hiccup left a perfectly good container orphaned from
    # pool_state.json — so it was never reused and every task re-cold-started,
    # and the caller's browser hung waiting on the create that never "finished".
    # Persisting here makes the container reusable immediately; the mint becomes
    # a best-effort enhancement on top of an already-live container.
    entry["cdp_port"] = cdp_port
    entry["novnc_port"] = novnc_port
    entry["last_used"] = time.time()
    entry["running"] = True
    _save_state()
    if do_prewarm:
        # Eager Kasada mint, NON-BLOCKING: chrome was already launched above
        # with domains[0] as its startup URL (its own first nav mints under the
        # no-CDP window). Run the wait+verify in a background daemon so
        # get_cdp_url() returns immediately (fast cold-start). _prewarm_kasada
        # serializes per-container via _warm_lock and is idempotent, so the
        # first protected navigate's ensure_warm() just waits on the same lock
        # and no-ops once this mint lands. Best-effort + NON-FATAL.
        def _eager_mint(_entry=entry, _domains=list(domains), _name=name):
            try:
                _prewarm_kasada(_entry, _domains[0], relaunch=False)
                for d in _domains[1:]:
                    _prewarm_kasada(_entry, d, relaunch=True)
            except Exception as e:
                logger.warning("webtop pool: eager prewarm for %s failed (non-fatal): %s",
                               _name, e)
        threading.Thread(target=_eager_mint, name="kasada-prewarm-" + str(name),
                         daemon=True).start()


def _stop_and_remove(entry: Dict, remove: bool = True) -> None:
    """docker stop (+ rm) a container, KEEPING its /config volume.

    Mutates the entry in place: clears running/ports and the warm markers (the
    running chrome is gone, so is_warm must read False until the container is
    recreated and re-minted; the cookie itself survives in the retained
    volume). Best-effort — docker errors are logged, not raised, so a partial
    teardown never wedges the caller.
    """
    name = entry.get("container")
    if name:
        try:
            _run(["docker", "stop", name], timeout=40, check=False)
        except Exception as e:
            logger.warning("webtop pool: stop %s failed: %s", name, e)
        if remove:
            try:
                _run(["docker", "rm", name], timeout=40, check=False)
            except Exception as e:
                logger.warning("webtop pool: rm %s failed: %s", name, e)
    entry["running"] = False
    entry["cdp_port"] = None
    entry["novnc_port"] = None
    entry.pop("warm_expiry", None)
    entry.pop("warm_domain", None)


def _evict_lru() -> None:
    """Stop + remove the least-recently-used running container (keep volume)."""
    size = _int_env("HERMES_WEBTOP_POOL_SIZE", 10)
    running = [e for e in _MAPPING.values() if e.get("running")]
    if len(running) < size:
        return
    running.sort(key=lambda e: e.get("last_used", 0))
    victim = running[0]
    name = victim.get("container")
    _stop_and_remove(victim, remove=True)
    logger.info("webtop pool: evicted %s (volume retained at %s)",
                name, victim.get("config_dir"))


def _wait_desktop_ready(entry: Dict, timeout: float = 90.0) -> bool:
    """Block until the container's desktop session is actually ready.

    Chromium is autostarted inside the XFCE session, so its CDP endpoint
    answering implies the X session is up and the desktop is drawn. Without
    this gate a freshly created container is handed to the browser tool /
    computer_use while XFCE is still booting -- captures come back black and
    the AT-SPI tree is empty (observed live: capture 11s after docker run).
    Returns True when ready, False on timeout (callers proceed anyway --
    degrade to the old behaviour rather than hard-fail the tool call).
    """
    port = entry.get("cdp_port")
    if not port:
        return False
    import urllib.request

    url = f"http://127.0.0.1:{port}/json/version"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(1.0)
    logger.warning(
        "webtop pool: %s desktop not ready after %.0fs (CDP port %s); proceeding anyway",
        entry.get("container"), timeout, port,
    )
    return False


def _ensure_running(task_id: str) -> Dict:
    """Return a mapping entry for task_id with a running container.

    The slow cold-boot (docker run + desktop wait + chrome launch) runs OUTSIDE
    the global ``_LOCK`` — serialized per-container via ``_warm_lock`` so the
    same container isn't created twice, while DIFFERENT tasks cold-boot in
    parallel. Holding ``_LOCK`` for the whole ~60-90s boot (the old behaviour)
    serialized every browser task in the gateway behind a single mutex: one
    cold-boot stalled all others (the "silent hang" — a task blocked forever at
    the ``with _LOCK`` acquire while another task booted). Ports are reserved
    under the brief lock so the off-lock boots can't race for host ports.
    """
    _load_state()
    _start_reaper()
    with _LOCK:
        entry = _MAPPING.get(task_id)
        if entry is None:
            slot = _slot_for(task_id)
            entry = {
                "slot": slot,
                "container": f"hermes-wt-{slot}",
                "config_dir": str(_config_root() / slot),
                "cdp_port": None,
                "novnc_port": None,
                "running": False,
                "last_used": 0.0,
            }
            _MAPPING[task_id] = entry
        entry["last_used"] = time.time()
        already_running = bool(entry.get("running")) and _container_running(entry["container"])
        if not already_running:
            entry["running"] = False
            if not entry.get("cdp_port"):
                _evict_lru()
                _reserve_ports(entry)  # atomic under _LOCK; stamps entry ports
    if already_running:
        return entry
    # Cold-boot OUTSIDE _LOCK, serialized per-container.
    with _warm_lock(entry["container"]):
        if _container_running(entry["container"]):
            with _LOCK:
                entry["running"] = True
                _save_state()
        else:
            _create_container(entry)   # persists state itself (running=True + _save_state)
            with _LOCK:
                entry["running"] = True
                _save_state()
    _wait_desktop_ready(entry)
    return entry


# ── public API ─────────────────────────────────────────────────────────────

def get_cdp_url(task_id: Optional[str]) -> str:
    """Per-task CDP URL, or ``""`` if the pool is disabled.

    The browser tool calls this and falls back to the global ``browser.cdp_url``
    when it returns empty.
    """
    if not pool_enabled():
        return ""
    if not task_id:
        task_id = "default"
    try:
        entry = _ensure_running(task_id)
        port = entry.get("cdp_port")
        if not port:
            return ""
        return f"http://127.0.0.1:{port}"
    except Exception as e:
        logger.warning("webtop pool: get_cdp_url(%s) failed: %s", task_id, e)
        return ""


def get_container_name(task_id: Optional[str]) -> Optional[str]:
    """Container name for a task (used by the computer-use backend to docker-exec)."""
    if not pool_enabled():
        return None
    if not task_id:
        task_id = "default"
    try:
        entry = _ensure_running(task_id)
        return entry.get("container")
    except Exception as e:
        logger.warning("webtop pool: get_container_name(%s) failed: %s", task_id, e)
        return None


def status() -> Dict:
    """Snapshot of the pool for diagnostics (consumed by the LAN dashboard)."""
    _load_state()
    return {
        "enabled": pool_enabled(),
        "size": _int_env("HERMES_WEBTOP_POOL_SIZE", 10),
        "image": _image(),
        "config_root": str(_config_root()),
        "idle_ttl": _IDLE_TTL,
        "tasks": {
            tid: {
                "container": e.get("container"),
                "slot": e.get("slot"),
                "config_dir": e.get("config_dir"),
                "cdp_port": e.get("cdp_port"),
                "novnc_port": e.get("novnc_port"),
                "running": e.get("running"),
                "last_used": e.get("last_used"),
                "warm_expiry": e.get("warm_expiry"),
                "warm_domain": e.get("warm_domain"),
            }
            for tid, e in _MAPPING.items()
        },
    }


def release(task_id: Optional[str], remove: bool = True) -> bool:
    """Tear down a task's webtop container, KEEPING its /config volume.

    Called on session teardown (Agent.close) and by the idle reaper so idle
    desktops don't leak past the thread that spawned them. The volume is
    retained, so reactivating the same task_id later recreates the container
    from its persisted profile (and re-mints Kasada via the eager prewarm).
    Returns True if a container was present and torn down.
    """
    if not pool_enabled():
        return False
    task_id = task_id or "default"
    _load_state()
    with _LOCK:
        entry = _MAPPING.get(task_id)
        if not entry:
            return False
        name = entry.get("container")
        present = bool(name) and _container_exists(name)
        if not present and not entry.get("running"):
            return False
        logger.info("webtop pool: releasing %s (task %s; volume retained)",
                    name, task_id)
        _stop_and_remove(entry, remove=remove)
        _save_state()
    return present


def _purge_volume(config_dir: str) -> None:
    """Delete a per-task /config volume directory.

    The webtop creates files inside /config as root and other uids (its
    custom-cont-init runs as root), so a host-side rmtree by the unprivileged
    gateway user leaves residue behind. Fast-path rmtree what we can, then, if
    anything survives, nuke it from inside a throwaway root container mounting
    the parent (docker's root can delete any uid's files — no host sudo).
    """
    p = Path(config_dir)
    if not p.exists():
        return
    shutil.rmtree(p, ignore_errors=True)
    if p.exists():
        try:
            _run(["docker", "run", "--rm", "--entrypoint", "/bin/rm",
                  "-v", f"{p.parent}:/p", _image(), "-rf", f"/p/{p.name}"],
                 timeout=60, check=False)
        except Exception as e:
            logger.warning("webtop pool: docker purge %s failed: %s", config_dir, e)
    if p.exists():
        logger.warning("webtop pool: volume %s not fully purged", config_dir)


def delete_task(task_id: Optional[str]) -> bool:
    """Permanently remove a task's webtop: stop + rm the container AND delete
    its /config volume, then drop the mapping entry.

    Called when the thread/session is DELETED (db.delete_session) — the profile,
    logins, and Kasada cookie are gone with the thread on purpose. This is the
    ONLY path that destroys a volume; release()/the LRU valve retain it.
    Returns True if an entry was removed.
    """
    if not pool_enabled():
        return False
    task_id = task_id or "default"
    _load_state()
    with _LOCK:
        entry = _MAPPING.pop(task_id, None)
        if entry is None:
            return False
        name = entry.get("container")
        cfg = entry.get("config_dir")
        _stop_and_remove(entry, remove=True)
        if cfg:
            _purge_volume(cfg)
        _save_state()
        logger.info("webtop pool: deleted task %s (container %s + volume purged)",
                    task_id, name)
    return True


def _reap_idle_once() -> None:
    """Release running containers idle longer than the TTL (one pass)."""
    ttl = _IDLE_TTL
    if ttl <= 0:
        return
    now = time.time()
    with _LOCK:
        victims = [
            tid for tid, e in _MAPPING.items()
            if e.get("running") and (now - float(e.get("last_used", 0) or 0)) > ttl
        ]
    for tid in victims:
        try:
            release(tid)
        except Exception as e:
            logger.warning("webtop pool: idle-reap %s failed: %s", tid, e)


def _reaper_loop() -> None:
    while True:
        try:
            time.sleep(60)
            _reap_idle_once()
        except Exception:
            time.sleep(60)


def _start_reaper() -> None:
    """Start the idle-reaper daemon thread once (lazy, on first pool use)."""
    global _REAPER_STARTED
    if _IDLE_TTL <= 0:
        return
    with _REAPER_LOCK:
        if _REAPER_STARTED:
            return
        _REAPER_STARTED = True
        threading.Thread(target=_reaper_loop, name="webtop-pool-reaper",
                         daemon=True).start()
        logger.info("webtop pool: idle reaper started (TTL %ss)", int(_IDLE_TTL))

# ── behavioural humanizer (Kasada) ──────────────────────────────────────────
# Trusted CDP Input stream (mouse/scroll/key, isTrusted=true) run on the HOST
# against a task's CDP port. Kasada's behavioural check ignores JS-dispatched
# events (isTrusted=false), so this must go through CDP Input, not a page script.
def humanize_session(cdp_url: str, seconds: float = 6.0, vw: int = 2556, vh: int = 1398) -> int:
    """Stream human-like trusted input into the task's browser for `seconds`.

    Best-effort: returns the number of dispatched events, 0 on failure. Safe to
    call before navigating a Kasada-protected page; adds `seconds` of latency.
    """
    import asyncio
    try:
        from . import cdp_humanize  # type: ignore
    except Exception:
        try:
            import importlib.util, os
            here = os.path.dirname(__file__)
            spec = importlib.util.spec_from_file_location("cdp_humanize", os.path.join(here, "cdp_humanize.py"))
            mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)  # type: ignore
            cdp_humanize = mod
        except Exception as e:
            logger.warning("webtop pool: humanize unavailable: %s", e)
            return 0
    try:
        url = cdp_url or ""
        if url.startswith("ws://") or url.startswith("wss://"):
            url = "http" + url[2:]
        return asyncio.run(cdp_humanize.humanize(url, float(seconds), int(vw), int(vh)))
    except Exception as e:
        logger.warning("webtop pool: humanize_session failed: %s", e)
        return 0


# ── Kasada cookie minting / prewarm ─────────────────────────────────────────
# Each webtop mints its own KP_UIDz via the proven google -> click-thru ->
# humanize recipe (tools/kasada_warmup.py). The cookie lives in the container's
# bind-mounted /config profile, so it survives container recreates and is lost
# only on docker rm of the volume. KP_UIDz expires in ~24h; is_warm() gates on
# the cookie's expiry so we only re-mint when stale.

def _cookie_db_path(entry: Dict) -> Path:
    return Path(entry["config_dir"]) / ".config" / "chrome-hermes" / "Default" / "Cookies"


def _kp_uidz_expiry(entry: Dict, domain: str) -> Optional[float]:
    """Unix-epoch seconds at which the KP_UIDz cookie for `domain` expires, or
    None if absent. Read host-side from the chrome profile's Cookies sqlite
    (the /config volume is bind-mounted). immutable=1 avoids fighting chrome's
    WAL lock; we only read presence + expiry, not the encrypted value."""
    path = _cookie_db_path(entry)
    if not path.is_file():
        return None
    apex = _apex(domain)
    try:
        import sqlite3
        uri = f"file:{path}?mode=ro&immutable=1"
        con = sqlite3.connect(uri, uri=True)
        try:
            row = con.execute(
                "SELECT MAX(expires_utc) FROM cookies "
                "WHERE name='KP_UIDz' AND host_key LIKE ?",
                (f"%.{apex}",),
            ).fetchone()
        finally:
            con.close()
    except Exception as e:
        logger.debug("webtop pool: cookie db read failed for %s: %s", apex, e)
        return None
    if not row or not row[0]:
        return None
    # Chrome stores expires_utc as microseconds since 1601-01-01 UTC.
    return row[0] / 1_000_000 - 11644473600


def is_warm(entry: Dict, domain: str) -> bool:
    """True if the container has a verified-warm KP_UIDz for `domain`.

    Primary signal: `entry["warm_expiry"]` (unix epoch), set ONLY by
    `_prewarm_kasada` after a startup-URL mint that genuinely loaded real page
    content (k=false + real title + KP_UIDz present). This avoids the
    BREAKTHROUGH #3 false positive where a pending 429 KP_UIDz (set during a
    failed challenge) has a ~24h expiry but does NOT short-circuit. Falling
    back to the sqlite cookie expiry is intentionally NOT done: a pending
    cookie there would falsely report warm. On a container recreate that
    retained the /config volume, warm_expiry is lost -> is_warm False -> the
    mint re-runs and re-verifies (cheap if the cookie persisted: the startup
    URL short-circuits in ~3s, though the ~28s no-CDP wait still applies).
    """
    exp = entry.get("warm_expiry", 0)
    return bool(exp and exp > time.time() + 1800)


def _wait_cdp_http(port: int, timeout: float = 60.0) -> bool:
    import urllib.request
    url = f"http://127.0.0.1:{port}/json/version"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(1.0)
    return False


def _load_kasada_warmup():
    """Import tools/kasada_warmup.py, tolerating package + by-path loaders."""
    try:
        import kasada_warmup as m  # type: ignore
        return m
    except Exception:
        import importlib.util, os
        here = os.path.dirname(os.path.abspath(__file__))
        spec = importlib.util.spec_from_file_location(
            "kasada_warmup", os.path.join(here, "kasada_warmup.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore
        return mod


# The no-CDP sensor window for the startup-URL mint. The 2026-07-09 16:40
# genuine solve waited 30s; 25s worked reproducibly on the pool (RUN1/RUN3,
# 2026-07-10). 28s gives margin for slow chrome startup. During this window
# NO CDP WS client may be attached to the page target — attaching on a fresh
# profile makes the sensor detect devtools activity and reject (BREAKTHROUGH #4).
_KASADA_SENSOR_WINDOW = float(os.environ.get("HERMES_WEBTOP_POOL_KASADA_SENSOR_WINDOW", "28"))


def _kill_chrome(container: str) -> None:
    """Stop the in-container chrome (launched to about:blank or a prior startup
    URL) so _launch_chrome can relaunch with a new startup URL. Clears stale
    singleton locks so the relaunch doesn't refuse."""
    try:
        _run(["docker", "exec", container, "bash", "-lc",
              "pkill -9 -f remote-debugging-port=9222 2>/dev/null; sleep 1; "
              "rm -rf /config/.config/chrome-hermes/SingletonLock "
              "/config/.config/chrome-hermes/SingletonCookie "
              "/config/.config/chrome-hermes/SingletonSocket 2>/dev/null"], timeout=15)
    except Exception as e:
        logger.debug("webtop pool: kill chrome in %s: %s", container, e)


def _prewarm_kasada(entry: Dict, domain: str, timeout: float = 120.0,
                    relaunch: bool = True) -> bool:
    """Mint a validated KP_UIDz for `domain` via the startup-URL recipe.

    The mint (BREAKTHROUGH #4, reproducible): chrome launched with the domain
    as its STARTUP URL runs the Kasada sensor undisturbed (no CDP WS client
    during the ~28s window) -> the full challenge solves -> the page loads real
    content and the validated KP_UIDz is minted into the profile. Verify
    attaches CDP AFTER the window and requires k=false + real title + l>1500 +
    KP_UIDz present (the reauid/len>2000 signals are false positives — #3).

    ``relaunch``: True (default, for lazy ensure_warm + extra eager domains) ->
    kill the current chrome and relaunch with the domain startup URL. False ->
    chrome was already launched to this domain's startup URL by the caller
    (``_create_container`` eager first-domain path); just wait + verify.

    Idempotent + serialized per container via _warm_lock. Skips if is_warm
    (verified warm_expiry fresh). Returns True if warm after the attempt.
    """
    port = entry.get("cdp_port")
    name = entry.get("container")
    if not port or not name:
        return False
    lk = _warm_lock(name)
    with lk:
        if is_warm(entry, domain):
            return True
        if relaunch:
            _kill_chrome(name)
            _launch_chrome(name, url=_kasada_startup_url(domain))
        # Wait for chrome's CDP HTTP endpoint WITHOUT attaching a WS client
        # (an HTTP GET to /json/version does not disturb the sensor). Then
        # hold the no-CDP sensor window so the fresh challenge solves clean.
        if not _wait_cdp_http(int(port), timeout=60.0):
            logger.warning("webtop pool: prewarm CDP not ready for %s; skipping mint", name)
            return is_warm(entry, domain)
        time.sleep(_KASADA_SENSOR_WINDOW)
        # Verify through a direct in-container forwarder (0.0.0.0:9298 -> 9222):
        # cdp-forward.py is select-based and can drop the WS under event bursts.
        direct_url, cleanup = _direct_cdp_url(entry)
        if not direct_url:
            direct_url = f"http://127.0.0.1:{int(port)}"
            cleanup = lambda: None
        solved = False
        expires = None
        try:
            warmup = _load_kasada_warmup()
            import asyncio
            res = asyncio.run(asyncio.wait_for(
                warmup.verify_mint(direct_url, domain, timeout=max(15.0, timeout - _KASADA_SENSOR_WINDOW - 5)),
                timeout=timeout))
            solved = bool(res.get("solved"))
            expires = res.get("expires")
        except Exception as e:
            logger.warning("webtop pool: prewarm mint verify failed for %s @ %s: %s",
                           name, domain, e)
        finally:
            cleanup()
        if solved and expires and expires > time.time() + 1800:
            entry["warm_expiry"] = expires
            entry["warm_domain"] = domain
            try:
                _save_state()
            except Exception:
                pass
            logger.info("webtop pool: prewarm minted KP_UIDz for %s @ %s (expires %.0fs)",
                        name, domain, expires - time.time())
            return True
        # Fallback: a prior persisted cookie (recreate case) may already be
        # warm even if the verify didn't fire a fresh solve this run.
        exp = _kp_uidz_expiry(entry, domain)
        if exp and exp > time.time() + 1800 and is_warm(entry, domain):
            return True
        logger.warning("webtop pool: prewarm mint did NOT solve for %s @ %s "
                       "(page did not load real content); cookie may be pending",
                       name, domain)
        return is_warm(entry, domain)


def _prewarm_kasada_thread(entry: Dict, domain: str) -> None:
    """Background daemon target (unused by the eager path, which mints
    synchronously in _create_container; kept for a future async-lazy mode)."""
    try:
        _prewarm_kasada(entry, domain)
    except Exception as e:
        logger.warning("webtop pool: prewarm thread error for %s @ %s: %s",
                       entry.get("container"), domain, e)


def ensure_warm(task_id: Optional[str], host: str, timeout: float = 120.0) -> bool:
    """Block until the task's container has a warm KP_UIDz for `host`, minting
    via the startup-URL recipe if needed. Called by browser_navigate for
    Kasada-protected hosts right before the actual navigate.

    With eager prewarm (default ON) the container is already warm at creation,
    so this is a cheap no-op (is_warm True). For lazy mode (or a stale cookie
    on a cold container) this mints: kill chrome, relaunch with the host's
    startup URL, hold the ~28s no-CDP window, verify. The first protected
    navigate pays the ~30s mint; subsequent visits reuse the cookie.

    CAVEAT: re-minting on a SUBSEQUENT navigate (cookie expired ~30d while a
    task's agent-browser daemon is live) kills chrome under the daemon — the
    daemon's CDP connection breaks. browser_navigate's following
    _run_browser_command("open") spawns a fresh daemon to the new warm chrome,
    so the navigate still completes, but if agent-browser doesn't reconnect
    cleanly the navigate may error and the agent re-mints manually per the
    mnemosyne kasada-warmup procedure. Returns True if warm; False on failure
    (caller proceeds to navigate anyway)."""
    if not pool_enabled() or not host:
        return False
    if not _is_kasada_host(host):
        return True  # not a protected host; no warm needed
    tid = task_id or "default"
    try:
        entry = _ensure_running(tid)
    except Exception as e:
        logger.warning("webtop pool: ensure_warm ensure_running failed: %s", e)
        return False
    if is_warm(entry, host):
        return True
    return _prewarm_kasada(entry, _apex(host), timeout=timeout, relaunch=True)

