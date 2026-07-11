#!/usr/bin/env python3
"""LAN webtop dashboard — list the per-thread webtops and click to enter one.

Runs on the Hermes box, bound to 0.0.0.0 so another machine on the LAN can
browse to ``http://<box-ip>:<DASH_PORT>/`` and see every running webtop pool
container (thread id, container, uptime, Kasada warm state, current page) with
an "Enter" button that opens that webtop's noVNC desktop.

Each webtop's noVNC is published by the pool on ``127.0.0.1:<novnc_port>``
(loopback only, for safety).  This dashboard exposes it on the LAN on demand by
running a transparent TCP forwarder ``0.0.0.0:(novnc_port+LAN_OFFSET) ->
127.0.0.1:novnc_port`` — noVNC (HTTP + websocket) is served at that port's
root, so it works unmodified with no container recreate.

READ-ONLY: this process never writes pool_state.json or stops containers, so it
never races the gateway's pool state.  It only reads the state file + docker +
each container's CDP endpoint.

Config (env, or ~/.hermes/.env):
  HERMES_WEBTOP_DASH_PORT        dashboard port (default 8899)
  HERMES_WEBTOP_DASH_LAN_OFFSET  novnc_port -> lan_port offset (default 100)
  HERMES_WEBTOP_POOL_CONFIG_ROOT pool state/volume root (default
                                 ~/hermes-gui-sandbox/agents)

SECURITY: the forwarded desktops are UNAUTHENTICATED on the LAN.  Only run this
on a trusted network; the dashboard binds 0.0.0.0 by design (that is the ask).
"""
from __future__ import annotations

import html
import json
import os
import socket
import subprocess
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen


def _load_env(path: str) -> None:
    """Best-effort load of KEY=VALUE lines from ~/.hermes/.env (no override)."""
    try:
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    except Exception:
        pass


_load_env(os.path.expanduser("~/.hermes/.env"))

CONFIG_ROOT = Path(
    os.environ.get("HERMES_WEBTOP_POOL_CONFIG_ROOT", "").strip()
    or (Path.home() / "hermes-gui-sandbox" / "agents")
).expanduser()
STATE_FILE = CONFIG_ROOT / "pool_state.json"
DASH_PORT = int(os.environ.get("HERMES_WEBTOP_DASH_PORT", "8899"))
LAN_OFFSET = int(os.environ.get("HERMES_WEBTOP_DASH_LAN_OFFSET", "100"))

# container -> lan_port for forwarders we've started this process.
_FWDS: dict = {}
_FWD_LOCK = threading.Lock()
# The webtop's KasmVNC web UI refuses a non-secure origin ("requires HTTPS")
# when reached over the LAN by IP, so we forward to the container's HTTPS port
# (3001, KasmVNC's own self-signed TLS) via its docker bridge IP and hand out
# https:// links — TLS terminates end-to-end at KasmVNC through the raw pipe.
_HTTPS_PORT = int(os.environ.get("HERMES_WEBTOP_DASH_HTTPS_PORT", "3001"))
_BRIDGE_CACHE: dict = {}   # container -> (ip, ts)
_BRIDGE_TTL = 10.0


# ── transparent TCP forwarder (0.0.0.0:lan -> <container bridge>:3001) ───────

def _bridge_ip(container: str):
    """Container docker bridge IP, cached briefly (survives container restarts
    that reassign the IP without a docker inspect per connection)."""
    import time as _t
    hit = _BRIDGE_CACHE.get(container)
    if hit and (_t.time() - hit[1]) < _BRIDGE_TTL:
        return hit[0]
    try:
        r = subprocess.run(
            ["docker", "inspect", "-f",
             "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container],
            capture_output=True, text=True, timeout=8,
        )
        ip = (r.stdout or "").strip() or None
    except Exception:
        ip = None
    _BRIDGE_CACHE[container] = (ip, _t.time())
    return ip


def _pipe(a: socket.socket, b: socket.socket) -> None:
    try:
        while True:
            d = a.recv(65536)
            if not d:
                break
            b.sendall(d)
    except Exception:
        pass
    finally:
        for s in (a, b):
            try:
                s.close()
            except Exception:
                pass


def _serve_forwarder(lan_port: int, container: str) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        srv.bind(("0.0.0.0", lan_port))
        srv.listen(64)
    except Exception as e:
        print(f"[fwd] bind {lan_port} failed: {e}", flush=True)
        return
    print(f"[fwd] 0.0.0.0:{lan_port} -> {container}:{_HTTPS_PORT} (https)", flush=True)
    while True:
        try:
            client, _ = srv.accept()
        except Exception:
            continue
        ip = _bridge_ip(container)   # resolved per-connection (restart-safe)
        if not ip:
            try:
                client.close()
            except Exception:
                pass
            continue
        try:
            upstream = socket.create_connection((ip, _HTTPS_PORT), timeout=8)
        except Exception:
            try:
                client.close()
            except Exception:
                pass
            continue
        threading.Thread(target=_pipe, args=(client, upstream), daemon=True).start()
        threading.Thread(target=_pipe, args=(upstream, client), daemon=True).start()


def _ensure_forwarder(novnc_port: int, container: str) -> int:
    """Ensure a LAN forwarder exists for a container; return the LAN port
    (derived from the stable noVNC port so each webtop keeps one port)."""
    lan_port = novnc_port + LAN_OFFSET
    with _FWD_LOCK:
        if container in _FWDS:
            return _FWDS[container]
        threading.Thread(target=_serve_forwarder, args=(lan_port, container),
                         daemon=True).start()
        _FWDS[container] = lan_port
    return lan_port


# ── data collection ──────────────────────────────────────────────────────────

def _read_state() -> dict:
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _docker_state(name: str):
    """(running: bool, started_iso: str) for a container, or (False, '')."""
    try:
        r = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}|{{.State.StartedAt}}", name],
            capture_output=True, text=True, timeout=8,
        )
        out = (r.stdout or "").strip()
        if "|" in out:
            run, started = out.split("|", 1)
            return run == "true", started
    except Exception:
        pass
    return False, ""


def _uptime(started_iso: str) -> str:
    if not started_iso:
        return "-"
    try:
        s = started_iso.replace("Z", "+00:00")
        # trim fractional seconds to microseconds for fromisoformat
        if "." in s:
            head, tail = s.split(".", 1)
            frac = "".join(c for c in tail if c.isdigit())[:6]
            tz = tail[len(frac):] if len(tail) > len(frac) else ""
            # recover tz offset if present after the fraction
            for marker in ("+", "-"):
                if marker in tail:
                    tz = marker + tail.split(marker, 1)[1]
                    break
            s = f"{head}.{frac}{tz}"
        dt = datetime.fromisoformat(s)
        secs = (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return "-"
    if secs < 0:
        secs = 0
    d, rem = divmod(int(secs), 86400)
    h, rem = divmod(rem, 3600)
    m, _ = divmod(rem, 60)
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


def _current_page(cdp_port):
    if not cdp_port:
        return "", ""
    try:
        data = json.loads(urlopen(f"http://127.0.0.1:{cdp_port}/json/list", timeout=3).read())
        for p in data:
            if p.get("type") == "page":
                return p.get("title") or "", p.get("url") or ""
    except Exception:
        pass
    return "", ""


def _warm_str(warm_expiry) -> str:
    try:
        exp = float(warm_expiry or 0)
    except (TypeError, ValueError):
        return "cold"
    if exp <= 0:
        return "cold"
    left = exp - time.time()
    if left <= 0:
        return "expired"
    h = left / 3600.0
    if h >= 48:
        return f"warm ({h/24:.0f}d)"
    return f"warm ({h:.0f}h)"


def _collect() -> list:
    rows = []
    for tid, e in _read_state().items():
        name = e.get("container") or ""
        running, started = _docker_state(name) if name else (False, "")
        cdp_port = e.get("cdp_port")
        novnc_port = e.get("novnc_port")
        title, url = ("", "")
        lan_port = None
        if running and cdp_port:
            title, url = _current_page(cdp_port)
        if running and novnc_port and name:
            lan_port = _ensure_forwarder(int(novnc_port), name)
        rows.append({
            "task_id": tid,
            "container": name,
            "running": running,
            "uptime": _uptime(started) if running else "-",
            "cdp_port": cdp_port,
            "novnc_port": novnc_port,
            "lan_port": lan_port,
            "warm": _warm_str(e.get("warm_expiry")),
            "warm_domain": e.get("warm_domain") or "",
            "title": title,
            "url": url,
        })
    rows.sort(key=lambda r: (not r["running"], r["task_id"]))
    return rows


# ── HTML ─────────────────────────────────────────────────────────────────────

_PAGE = """<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="15">
<title>Hermes webtops</title>
<style>
:root{{color-scheme:dark}}
*{{box-sizing:border-box}}
body{{margin:0;font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
background:#0d1117;color:#e6edf3}}
header{{padding:18px 22px;border-bottom:1px solid #21262d;display:flex;
align-items:baseline;gap:14px;flex-wrap:wrap}}
h1{{font-size:17px;margin:0;font-weight:600}}
.sub{{color:#7d8590;font-size:12px}}
.wrap{{padding:18px 22px;overflow-x:auto}}
table{{border-collapse:collapse;width:100%;min-width:760px}}
th,td{{text-align:left;padding:10px 12px;border-bottom:1px solid #21262d;vertical-align:middle}}
th{{color:#7d8590;font-weight:600;font-size:12px;text-transform:uppercase;letter-spacing:.4px}}
tr:hover td{{background:#161b22}}
.mono{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}}
.dot{{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:7px}}
.up{{background:#3fb950}}.down{{background:#f85149}}
.pill{{display:inline-block;padding:2px 9px;border-radius:999px;font-size:12px}}
.warm{{background:#132e1a;color:#3fb950}}.cold{{background:#2d2119;color:#d29922}}
.exp{{background:#3d1417;color:#f85149}}
.enter{{display:inline-block;padding:6px 15px;border-radius:6px;background:#238636;color:#fff;
text-decoration:none;font-weight:600;font-size:13px}}
.enter:hover{{background:#2ea043}}
.enter.off{{background:#21262d;color:#7d8590;pointer-events:none}}
.pg{{color:#7d8590;font-size:12px;max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.empty{{color:#7d8590;padding:40px 0;text-align:center}}
footer{{padding:12px 22px;color:#484f58;font-size:11px;border-top:1px solid #21262d}}
</style></head><body>
<header><h1>Hermes webtops</h1>
<span class="sub">{count} container(s) &middot; auto-refresh 15s &middot; idle-reap {ttl}m</span></header>
<div class="wrap">{table}</div>
<footer>Read-only view. Desktops are unauthenticated on the LAN — trusted networks only.
First open of each webtop shows a self-signed-certificate warning (KasmVNC HTTPS) — accept it to proceed.
State: {state}</footer>
</body></html>"""


def _render(rows: list, host: str) -> str:
    ttl = int(float(os.environ.get("HERMES_WEBTOP_POOL_IDLE_TTL", "1800")) / 60)
    if not rows:
        table = '<div class="empty">No webtop containers in the pool state yet.</div>'
        return _PAGE.format(count=0, ttl=ttl, table=table, state=html.escape(str(STATE_FILE)))
    body = []
    for r in rows:
        dot = "up" if r["running"] else "down"
        status = "running" if r["running"] else "stopped"
        wcls = "warm" if r["warm"].startswith("warm") else ("exp" if r["warm"] == "expired" else "cold")
        wtxt = r["warm"] + (f" · {r['warm_domain']}" if r["warm_domain"] and wcls == "warm" else "")
        if r["running"] and r["lan_port"]:
            enter = f'<a class="enter" href="https://{html.escape(host)}:{r["lan_port"]}/" target="_blank">Enter &rarr;</a>'
        else:
            enter = '<span class="enter off">Enter</span>'
        pg = ""
        if r["title"] or r["url"]:
            pg = html.escape((r["title"] or r["url"])[:120])
        body.append(
            "<tr>"
            f'<td><span class="dot {dot}"></span>{status}</td>'
            f'<td class="mono">{html.escape(r["task_id"])}</td>'
            f'<td class="mono">{html.escape((r["container"] or "")[:20])}</td>'
            f'<td>{html.escape(r["uptime"])}</td>'
            f'<td><span class="pill {wcls}">{html.escape(wtxt)}</span></td>'
            f'<td class="pg" title="{html.escape(r["url"])}">{pg}</td>'
            f"<td>{enter}</td>"
            "</tr>"
        )
    table = (
        "<table><thead><tr><th>State</th><th>Thread (task_id)</th><th>Container</th>"
        "<th>Uptime</th><th>Kasada</th><th>Current page</th><th></th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )
    return _PAGE.format(count=sum(1 for r in rows if r["running"]), ttl=ttl,
                        table=table, state=html.escape(str(STATE_FILE)))


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # quiet
        pass

    def do_GET(self):
        host = (self.headers.get("Host", "") or "").split(":")[0] or "127.0.0.1"
        if self.path.rstrip("/") in ("/api", "/api"):
            payload = json.dumps({"rows": _collect(),
                                  "state_file": str(STATE_FILE)}, indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path.rstrip("/") not in ("", "/"):
            self.send_response(404)
            self.end_headers()
            return
        page = _render(_collect(), host).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(page)))
        self.end_headers()
        self.wfile.write(page)


def main() -> None:
    srv = ThreadingHTTPServer(("0.0.0.0", DASH_PORT), _Handler)
    print(f"webtop dashboard on http://0.0.0.0:{DASH_PORT}/  "
          f"(state={STATE_FILE}, lan_offset={LAN_OFFSET})", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
