"""Kasada KP_UIDz cookie minting — the startup-URL recipe (BREAKTHROUGH #4,
2026-07-10, reproducible on the pool).

ROOT CAUSE (decisive, reconstructed from the only genuine fresh solve at
transcript line 7121, 2026-07-09 16:40): on a FRESH profile, Kasada's full
challenge runs, and its verdict depends on whether a CDP WS client is
attached during the sensor window:

  - WS client attached during sensor (Page.navigate / Runtime.evaluate /
    humanize driving) on FRESH  -> sensor detects devtools activity -> REJECT
    -> only the pending 429 KP_UIDz (short) is set; page stays on the 679-byte
    bootstrap (l=0, k=true). This is why the prior google->click-thru->22s
    humanize flow (kasada_warmup's old `_flow`) NEVER minted a solving cookie
    (13:08 run, line 5976; all prior mint_test/kasada_warmup runs).
  - NO WS client during sensor (chrome launched with the target as its STARTUP
    URL, left undisturbed ~25-30s) -> sensor runs clean -> SOLVE -> the page
    loads real content (l~2286, k=false, title "Search for Real Estate...")
    and the validated KP_UIDz (encrypted, ~172 bytes via Network.getCookies) +
    KP_UIDz-ssn + reauid are minted into the profile. This is the 16:40 mint.

The `--remote-debugging-port` flag / listening devtools agent alone is NOT
detected (16:40 had the port open, no WS client, passed). Only an actual WS
client connection + CDP commands during the FRESH sensor triggers reject.
Once warm, the KP_UIDz short-circuits the full challenge -> CDP-attached
navigates PASS (the Hermes use case; verified post-mint 2026-07-10).

THE WORKING MINT RECIPE (replaces the google->click-thru->humanize flow):
  1. Fresh profile, --shm-size=2g, native fp (LD_PRELOAD fakemem.so dm=8/hc=8),
     Google Chrome 150 only, the pool's existing recipe flags.
  2. Launch chrome with `https://www.<domain>/` as the STARTUP URL (chrome's
     own initial navigation). The pool's chromium-cdp.sh accepts the URL as
     arg 1 and appends it to the chrome command.
  3. Do NOT connect any CDP WS client for ~25-30s. Let the sensor run clean.
  4. After the wait, attach CDP and verify: k=false AND title contains the
     domain AND l>1500 (NOT 2000 — /buy is ~1993) AND KP_UIDz present.

This module does ONLY the verify step (step 4): the pool
(browser_webtop_pool._prewarm_kasada) owns the container control (kill chrome,
relaunch with the startup URL, wait 28s no-CDP) and calls `verify_mint` here
to attach + read state + cookies. The google->click-thru->humanize `_flow` is
GONE — it was a false positive (set pending cookies, page never solved).

Public API:
  await verify_mint(cdp_http_url, domain, timeout=40) -> dict
      {solved, expires, len, k, title, cookies}. `solved` is True only when
      the page loaded real content (k=false, title~=domain, l>1500) AND a
      KP_UIDz cookie is present. `expires` is the KP_UIDz cookie's unix
      expiry (for the pool to set warm_expiry), or None.
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.request

logger = logging.getLogger(__name__)

_DEFAULT_SESSION_COOKIE = "reauid"  # domain-issued; proves the REAL site was reached


def _apex(domain: str) -> str:
    d = (domain or "").strip().lower().lstrip(".")
    if d.startswith("www."):
        d = d[4:]
    return d


def _http_json(url: str, timeout: int = 15) -> dict:
    return json.loads(urllib.request.urlopen(url, timeout=timeout).read().decode())


def _first_page_id(http_url: str) -> str | None:
    pages = [p for p in _http_json(http_url + "/json/list") if p.get("type") == "page"]
    return pages[0]["id"] if pages else None


def _load_humanize():
    """Import cdp_humanize, tolerating both package and by-path loader setups."""
    try:
        import cdp_humanize as H  # type: ignore
        return H
    except Exception:
        import importlib.util, os
        here = os.path.dirname(os.path.abspath(__file__))
        spec = importlib.util.spec_from_file_location("cdp_humanize", os.path.join(here, "cdp_humanize.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore
        return mod


async def verify_mint(
    cdp_http_url: str,
    domain: str,
    timeout: float = 40.0,
) -> dict:
    """Attach CDP AFTER the no-CDP sensor window and read the mint result.

    Callers MUST wait ~25-30s with NO CDP WS client connected (let the sensor
    run undisturbed on the fresh profile) BEFORE calling this. This function
    then attaches, reads page state (url, body innerText len, title, KPSDK
    presence) and cookies, and polls for up to `timeout` seconds (every 3s) in
    case the page is still settling. Returns a dict; `solved` is True only
    when the page genuinely loaded real content (k=false, title contains the
    domain apex, l>1500) AND a KP_UIDz cookie is present.

    Solve signal rationale (BREAKTHROUGH #3): `reauid` alone is NOT a solve
    signal (set during the challenge); `len>2000` alone is NOT a solve signal
    (/buy is ~1993; the bootstrap page text is ~2286 WITH k=true). The only
    reliable solve is `k=false` + a real title + non-trivial body length.
    """
    import websockets
    H = _load_humanize()
    cdp_http_url = cdp_http_url.rstrip("/")
    apex = _apex(domain)
    expr = (
        "JSON.stringify({u:location.href,l:document.body?document.body.innerText.length:0,"
        "t:document.title,k:/KPSDK|kpsdk/.test(document.documentElement.outerHTML.slice(0,2000))})"
    )
    deadline = asyncio.get_event_loop().time() + timeout
    t = 0
    last = {"solved": False, "expires": None, "len": 0, "k": True, "title": "", "cookies": []}
    while asyncio.get_event_loop().time() < deadline:
        try:
            ver = _http_json(cdp_http_url + "/json/version")
            bws = ver["webSocketDebuggerUrl"]
            async with websockets.connect(bws, max_size=None) as ws:
                tid = _first_page_id(cdp_http_url)
                if tid is None:
                    await asyncio.sleep(3); t += 3; continue
                r = await H._cdp(ws, "Target.attachToTarget", {"targetId": tid, "flatten": True})
                sid = r["result"]["sessionId"]
                await H._cdp(ws, "Page.enable", {}, sid)
                await H._cdp(ws, "Runtime.enable", {}, sid)
                await H._cdp(ws, "Network.enable", {}, sid)
                rv = await H._cdp(ws, "Runtime.evaluate", {"expression": expr, "returnByValue": True}, sid)
                val = rv.get("result", {}).get("result", {}).get("value", "")
                d = json.loads(val) if val else {}
                rc = await H._cdp(ws, "Network.getCookies", {"urls": [f"https://www.{apex}/"]}, sid)
                ck = [(c["name"], len(c.get("value", "")), c.get("expires", -1))
                      for c in rc.get("result", {}).get("cookies", [])
                      if c["name"] in ("KP_UIDz", "KP_UIDz-ssn", _DEFAULT_SESSION_COOKIE)]
            kp = [c for c in ck if c[0] == "KP_UIDz" and c[1] > 100]
            expires = max((c[2] for c in kp), default=-1)
            solved = (
                not d.get("k", True)
                and apex in (d.get("t", "") or "").lower()
                and int(d.get("l", 0)) > 1500
                and bool(kp)
            )
            last = {"solved": solved, "expires": (expires if expires > 0 else None),
                    "len": int(d.get("l", 0)), "k": bool(d.get("k", True)),
                    "title": d.get("t", ""), "cookies": ck}
            logger.info("kasada_warmup: verify +%ds solved=%s l=%d k=%s title=%r kp=%s",
                        t, solved, last["len"], last["k"], last["title"][:40], bool(kp))
            if solved:
                return last
        except Exception as e:
            logger.debug("kasada_warmup: verify +%ds err %s", t, repr(e)[:80])
        await asyncio.sleep(3)
        t += 3
    return last


# Back-compat shim: the old pool called `warm(cdp_url, domain)`. The startup-URL
# mint is now driven entirely by the pool (container control lives there), so
# `warm` is a no-op stub that returns False — callers should use the pool's
# `_prewarm_kasada` + this module's `verify_mint` instead. Kept so a stale
# import doesn't crash.
async def warm(cdp_http_url: str, domain: str = "realestate.com.au", humanize_secs: float = 22.0) -> bool:
    logger.warning("kasada_warmup.warm() is obsolete (startup-URL mint replaced it); "
                   "use browser_webtop_pool._prewarm_kasada + verify_mint")
    return False
