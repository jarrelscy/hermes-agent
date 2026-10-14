"""Shared browser logins across webtops (Slack, Outlook/Microsoft, Google).

Each thread has its own Chrome profile, so a login made in one thread is
invisible to the next thread or cron run. This module keeps one shared cookie
store on the host and syncs it with each webtop's Chrome over CDP:

- cookies a thread gained or changed since its last sync are copied into the
  store (so a login in any thread is kept);
- store cookies the thread lacks or holds an older copy of are set in its
  Chrome (so every thread starts logged in).

Deletions are not propagated: logging out of one thread does not log out the
others, and a thread's stale copy never overwrites a newer store value.

Only cookies on the configured domains are touched. The sync uses browser-level
``Storage.getCookies`` / ``Storage.setCookies`` and never attaches to a page, so
it is safe during a Kasada sensor window.

Env:
  HERMES_WEBTOP_SHARED_LOGINS          1 (default) = enabled, 0 = off
  HERMES_WEBTOP_SHARED_LOGIN_DOMAINS   comma list, overrides the defaults
  HERMES_WEBTOP_SHARED_LOGIN_DIR       store dir (default ~/.hermes/browser-auth)

The store holds live session cookies: it is written 0600 in a 0700 dir.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import threading
import time
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_DEFAULT_DOMAINS = (
    # Slack
    "slack.com",
    # Microsoft 365 / Outlook (work and personal) and its login service
    "microsoftonline.com", "microsoft.com", "microsoft365.com", "live.com",
    "office.com", "office365.com", "outlook.com", "cloud.microsoft",
    # Google / Gmail (en-AU sign-in also sets cookies on google.com.au)
    "google.com", "google.com.au", "gmail.com",
)

_SYNC_INTERVAL = 30.0
_LAST_SYNC: Dict[str, float] = {}
_LOCK = threading.Lock()


def enabled() -> bool:
    return os.environ.get("HERMES_WEBTOP_SHARED_LOGINS", "1").strip().lower() not in (
        "0", "false", "no", "off")


def _domains() -> List[str]:
    raw = os.environ.get("HERMES_WEBTOP_SHARED_LOGIN_DOMAINS", "").strip()
    if raw:
        return [d.strip().lstrip(".").lower() for d in raw.split(",") if d.strip()]
    return list(_DEFAULT_DOMAINS)


def _store_dir() -> Path:
    raw = os.environ.get("HERMES_WEBTOP_SHARED_LOGIN_DIR", "").strip()
    d = Path(raw).expanduser() if raw else Path.home() / ".hermes" / "browser-auth"
    (d / "synced").mkdir(parents=True, exist_ok=True)
    os.chmod(d, 0o700)
    os.chmod(d / "synced", 0o700)
    return d


def _wanted(cookie: Dict, domains: List[str]) -> bool:
    if cookie.get("partitionKey"):
        return False
    host = (cookie.get("domain") or "").lstrip(".").lower()
    return any(host == d or host.endswith("." + d) for d in domains)


def _key(c: Dict) -> str:
    return f"{c.get('domain')}|{c.get('path')}|{c.get('name')}"


def _expired(c: Dict, now: float) -> bool:
    exp = c.get("expires", -1)
    return not c.get("session") and isinstance(exp, (int, float)) and 0 < exp < now


def _read_json(path: Path) -> Dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _write_json(path: Path, data: Dict) -> None:
    tmp = path.with_suffix(".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


@contextmanager
def _file_lock(d: Path):
    with open(d / ".lock", "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _to_param(c: Dict) -> Dict:
    """Storage.getCookies Cookie -> Storage.setCookies CookieParam."""
    p = {"name": c["name"], "value": c["value"], "path": c.get("path") or "/",
         "secure": bool(c.get("secure")), "httpOnly": bool(c.get("httpOnly"))}
    domain = c.get("domain") or ""
    if domain.startswith("."):
        p["domain"] = domain
    else:
        # Host-only cookie: setting `domain` would widen it to subdomains.
        p["url"] = f"https://{domain}{p['path']}"
    for k in ("sameSite", "priority", "sourceScheme", "sourcePort"):
        if c.get(k) is not None:
            p[k] = c[k]
    if not c.get("session") and c.get("expires", -1) > 0:
        p["expires"] = c["expires"]
    return p


class _Browser:
    def __init__(self, cdp_port: int, timeout: float = 10.0):
        from websockets.sync.client import connect
        with urllib.request.urlopen(
                f"http://127.0.0.1:{cdp_port}/json/version", timeout=timeout) as r:
            ws_url = json.loads(r.read())["webSocketDebuggerUrl"]
        self._ws = connect(ws_url, max_size=None, open_timeout=timeout)
        self._timeout = timeout
        self._id = 0

    def call(self, method: str, params: Optional[Dict] = None) -> Dict:
        self._id += 1
        self._ws.send(json.dumps({"id": self._id, "method": method, "params": params or {}}))
        deadline = time.time() + self._timeout
        while True:
            msg = json.loads(self._ws.recv(timeout=max(0.1, deadline - time.time())))
            if msg.get("id") == self._id:
                if "error" in msg:
                    raise RuntimeError(f"{method}: {msg['error']}")
                return msg.get("result", {})

    def close(self) -> None:
        try:
            self._ws.close()
        except Exception:
            pass


def sync(container: str, cdp_port: Optional[int], force: bool = False) -> bool:
    """Two-way sync of shared login cookies with one webtop's Chrome.

    Throttled to once per ``_SYNC_INTERVAL`` per container unless ``force``.
    Best-effort: returns False on any failure and never raises.
    """
    if not enabled() or not container or not cdp_port:
        return False
    now = time.time()
    with _LOCK:
        if not force and now - _LAST_SYNC.get(container, 0) < _SYNC_INTERVAL:
            return True
        _LAST_SYNC[container] = now
    try:
        return _sync(container, int(cdp_port))
    except Exception as e:
        logger.debug("shared logins: sync %s failed: %s", container, e)
        return False


def _sync(container: str, cdp_port: int) -> bool:
    domains = _domains()
    d = _store_dir()
    browser = _Browser(cdp_port)
    try:
        cur = {_key(c): c for c in browser.call("Storage.getCookies").get("cookies", [])
               if _wanted(c, domains)}
        with _file_lock(d):
            store_path = d / "cookies.json"
            seen_path = d / "synced" / f"{container}.json"
            store = _read_json(store_path)
            seen = _read_json(seen_path)
            now = time.time()
            store = {k: c for k, c in store.items() if not _expired(c, now)}

            if seen_path.exists():
                gained = [k for k, c in cur.items() if seen.get(k) != c["value"]]
            else:
                # First sync of this profile: its copies may predate the store.
                gained = [k for k in cur if k not in store]
            for k in gained:
                store[k] = cur[k]
            push = [store[k] for k in store
                    if k not in gained and (k not in cur or cur[k]["value"] != store[k]["value"])]
            if push:
                browser.call("Storage.setCookies", {"cookies": [_to_param(c) for c in push]})

            new_seen = {k: c["value"] for k, c in cur.items()}
            new_seen.update({_key(c): c["value"] for c in push})
            if gained:
                _write_json(store_path, store)
            _write_json(seen_path, new_seen)
        if gained or push:
            logger.info("shared logins: %s +%d to store, %d set in browser",
                        container, len(gained), len(push))
        return True
    finally:
        browser.close()


def forget(container: str) -> None:
    """Drop the per-container sync record (container + profile deleted)."""
    with _LOCK:
        _LAST_SYNC.pop(container, None)
    try:
        (_store_dir() / "synced" / f"{container}.json").unlink(missing_ok=True)
    except Exception:
        pass
