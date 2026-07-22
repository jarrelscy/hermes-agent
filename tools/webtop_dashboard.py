#!/usr/bin/env python3
"""LAN webtop dashboard — complete working version with fixed URL (bridge IP + HTTPS 3001)."""
from __future__ import annotations
import html, json, os, socket, subprocess, threading, time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen

# Config
CONFIG_ROOT = Path(os.environ.get("HERMES_WEBTOP_POOL_CONFIG_ROOT", "").strip() or (Path.home() / "hermes-gui-sandbox" / "agents")).expanduser()
STATE_FILE = CONFIG_ROOT / "pool_state.json"
DASH_PORT = int(os.environ.get("HERMES_WEBTOP_DASH_PORT", "8899"))
LAN_OFFSET = int(os.environ.get("HERMES_WEBTOP_DASH_LAN_OFFSET", "100"))
_HTTPS_PORT = int(os.environ.get("HERMES_WEBTOP_DASH_HTTPS_PORT", "3001"))
_FWDS: dict = {}
_FWD_LOCK = threading.Lock()
_BRIDGE_CACHE: dict = {}
_BRIDGE_TTL = 10.0

# Helper: load .env best-effort
def _load_env(path: str) -> None:
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

# Bridge IP resolution (container -> IP via docker inspect, cached)
def _bridge_ip(container: str) -> str | None:
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

# TCP pipe between sockets
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

# Forwarder: 0.0.0.0:(novnc_port+offset) -> container bridge IP:3001 (HTTPS)
def _serve_forwarder(lan_port: int, container: str) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        srv.bind(("0.0.0.0", lan_port))
        srv.listen(64)
    except Exception as e:
        print(f"[fwd] bind {lan_port} failed: {e}", flush=True)
        return
    print(f"[fwd] 0.0.0.0:{lan_port} -> {container}:{_HTTPS_PORT} (https via bridge IP)", flush=True)
    while True:
        try:
            client, _ = srv.accept()
        except Exception:
            continue
        ip = _bridge_ip(container)
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

# Ensure a forwarder exists for a container; return LAN port
def _ensure_forwarder(novnc_port: int, container: str) -> int:
    lan_port = novnc_port + LAN_OFFSET
    with _FWD_LOCK:
        if container in _FWDS:
            return _FWDS[container]
        threading.Thread(target=_serve_forwarder, args=(lan_port, container), daemon=True).start()
        _FWDS[container] = lan_port
    return lan_port

# Read pool state JSON
def _read_state() -> dict:
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

# Container running state and started time
def _docker_state(name: str):
    try:
        r = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}|{{.State.StartedAt}}", name],
            capture_output=True, text=True, timeout=8,
        )
        out = (r.stdout or "").strip()
        if "|" in out:
            run_str, started = out.split("|", 1)
            return run_str == "true", started
    except Exception:
        pass
    return False, ""

# Format uptime string
def _uptime(started_iso: str) -> str:
    if not started_iso:
        return "-"
    try:
        s = started_iso.replace("Z", "+00:00")
        if "." in s:
            head, tail = s.split(".", 1)
            frac = "".join(ch for ch in tail if ch.isdigit())[:6]
            s = f"{head}.{frac}+00:00"
        dt = datetime.fromisoformat(s)
        secs = (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return "-"
    if secs < 0:
        secs = 0
    d, rem = divmod(int(secs), 86400)
    h, rem = divmod(rem, 3600)
    m, s2 = divmod(rem, 60)
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    return f"{m}m"

# Read current page from container CDP endpoint
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

# Warm state display
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

# Collect data for all entries
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

# HTML template (with fixed links: container bridge IP + HTTPS 3001)
_PAGE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="15">
<title>Hermes webtops</title>
<style>
:root{color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0d1117;color:#e6edf3}
header{padding:18px 22px;border-bottom:1px solid #21262d;display:flex;align-items:baseline;gap:14px;flex-wrap:wrap}
h1{font-size:17px;margin:0;font-weight:600}
.sub{color:#7d8590;font-size:12px}
.wrap{padding:18px 22px;overflow-x:auto}
table{border-collapse:collapse;width:100%;min-width:760px}
th,td{text-align:left;padding:10px 12px;border-bottom:1px solid #21262d;vertical-align:middle}
th{color:#7d8590;font-weight:600;font-size:12px;text-transform:uppercase;letter-spacing:.4px}
tr:hover td{background:#161b22}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:7px}
.up{background:#3fb950}.down{background:#f85149}
.pill{display:inline-block;padding:2px 9px;border-radius:999px;font-size:12px}
.warm{background:#132e1a;color:#3fb950}.cold{background:#2d2119;color:#d29922}.exp{background:#3d1417;color:#f85149}
.enter{display:inline-block;padding:6px 15px;border-radius:6px;background:#238636;color:#fff;text-decoration:none;font-weight:600;font-size:13px}
.enter:hover{background:#2ea043}
.enter.off{background:#21262d;color:#7d8590;pointer-events:none}
.pg{color:#7d8590;font-size:12px;max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.empty{color:#7d8590;padding:40px 0;text-align:center}
footer{padding:12px 22px;color:#484f58;font-size:11px;border-top:1px solid #21262d}
</style></head><body>
<header><h1>Hermes webtops</h1>
<span class="sub">{count} container(s) · auto-refresh 15s · idle-reap {ttl}m</span></header>
<div class="wrap">{table}</div>
<footer>Read-only view. Forwarded desktops: container bridge IP + HTTPS 3001 (fixed — not broken host:3311). Unauthenticated — trusted networks only.
State file: {state}</footer>
</body></html>"""

# HTML renderer (uses .replace() instead of .format() to avoid CSS brace conflicts)
def _render(rows: list, host: str) -> str:
    ttl_val = int(float(os.environ.get("HERMES_WEBTOP_POOL_IDLE_TTL", "1800")) / 60)
    if not rows:
        table = '<div class="empty">No webtop containers in the pool state yet.</div>'
        result = _PAGE.replace("{count}", "0").replace("{ttl}", str(ttl_val)).replace("{table}", table).replace("{state}", html.escape(str(STATE_FILE)))
        return result
    body_parts = []
    for r in rows:
        dot_class = "up" if r["running"] else "down"
        status_txt = "running" if r["running"] else "stopped"
        warm_class = "warm" if r["warm"].startswith("warm") else ("exp" if r["warm"] == "expired" else "cold")
        warm_text = r["warm"] + (f" · {r['warm_domain']}" if r["warm_domain"] and warm_class == "warm" else "")
        # Always go through the host's TCP forwarder (LAN-routable), never the
        # container's Docker bridge IP (172.17.x.x is only reachable from the
        # box itself, not from other LAN machines -> times out over the LAN).
        if r["running"] and r.get("lan_port"):
            href_target = html.escape(host)
            href_port = r.get("lan_port", 0) or 0
            enter_href = f"https://{href_target}:{href_port}/"
            enter_link = f'<a class="enter" href="{enter_href}" target="_blank">Enter &rarr;</a>'
        else:
            enter_link = '<span class="enter off">Enter</span>'
        pg_text = ""
        if r["title"] or r["url"]:
            pg_text = html.escape((r["title"] or r["url"])[:120])
        body_parts.append(
            "<tr>"
            f'<td><span class="dot {dot_class}"></span>{status_txt}</td>'
            f'<td class="mono">{html.escape(str(r["task_id"]))}</td>'
            f'<td class="mono">{html.escape(str(r.get("container") or "")[:20])}</td>'
            f'<td>{html.escape(str(r.get("uptime") or "-"))}</td>'
            f'<td><span class="pill {warm_class}">{html.escape(str(warm_text))}</span></td>'
            f'<td class="pg" title="{html.escape(str(r.get("url") or ""))}">{pg_text}</td>'
            f"<td>{enter_link}</td>"
            "</tr>"
        )
    table_html = (
        "<table><thead><tr>"
        "<th>State</th><th>Thread (task_id)</th><th>Container</th>"
        "<th>Uptime</th><th>Kasada</th><th>Current page</th><th></th>"
        "</tr></thead><tbody>" + "".join(body_parts) + "</tbody></table>"
    )
    result = _PAGE.replace("{count}", str(sum(1 for r in rows if r.get("running")))).replace("{ttl}", str(ttl_val)).replace("{table}", table_html).replace("{state}", html.escape(str(STATE_FILE)))
    return result

# Request handler
class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args, **kwargs):
        pass

    def do_GET(self):
        host_hdr = (self.headers.get("Host", "") or "").split(":")[0] or "127.0.0.1"
        path_normalized = self.path.rstrip("/")
        if path_normalized not in ("", "/"):
            self.send_response(404)
            self.end_headers()
            return
        rows = _collect()
        page_bytes = _render(rows, host_hdr).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(page_bytes)))
        self.end_headers()
        self.wfile.write(page_bytes)

def main():
    srv = ThreadingHTTPServer(("0.0.0.0", DASH_PORT), _Handler)
    print(f"Dashboard on http://0.0.0.0:{DASH_PORT}/ (links: container bridge IP + HTTPS {_HTTPS_PORT})", flush=True)
    srv.serve_forever()

if __name__ == "__main__":
    main()
