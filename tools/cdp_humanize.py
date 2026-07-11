"""CDP behavioural humanizer v2 — streams *trusted* input events into a page.

Kasada's behavioural model counts only ``isTrusted === true`` events. CDP
``Input.dispatchMouseEvent`` / ``Input.dispatchKeyEvent`` go through the
platform input path -> the resulting DOM events are isTrusted=true (unlike JS
``dispatchEvent`` which is untrusted and a stronger bot tell).

Generator model (more human than a flat random walk):
  - BURSTS: 1-3 waypoints per burst, separated by idle gaps (50-400 ms). Real
    mice move in bursts, not continuously.
  - PER WAYPOINT: cubic-Bezier path with an ease-in/ease-out velocity profile
    (acceleration, not constant speed), 10-26 steps, micro-tremor jitter.
  - READING SCROLLS: occasional slow vertical wheel scrolls with dwell pauses
    (simulates reading a page), not just random deltas.
  - OCCASIONAL click at a non-interactive spot, occasional keystroke.

Helpers also exposed for flow scripts: click_at, type_text, press_key,
move_bezier, eval_js, get_rect, scroll_by.

CLI:  python cdp_humanize.py <http_cdp_url> <seconds> [vw] [vh]
"""
import sys, json, asyncio, random, time, math, urllib.request
import websockets
from websockets.exceptions import ConnectionClosedError


def _http_json(url, timeout=10):
    return json.loads(urllib.request.urlopen(url, timeout=timeout).read().decode())


def _ts():
    return int(time.monotonic() * 1000)


async def _send(ws, mid, method, params, sid=None):
    msg = {"id": mid, "method": method, "params": params}
    if sid is not None:
        msg["sessionId"] = sid
    await ws.send(json.dumps(msg))


async def _fire(ws, method, params, sid=None):
    """Send a CDP command without awaiting its response (fire-and-forget) and
    drain any pending messages so the ws buffer doesn't congest. Used for
    Input.* events where we don't need the response — keeps the stream dense
    and robust on a busy shared ws (google results emit many CDP events)."""
    _n[0] += 1
    await _send(ws, _n[0], method, params, sid)
    if _n[0] % 16 == 0:
        await _drain(ws)


async def _drain(ws, max_msgs=4096):
    """Non-blocking drain: read every currently-buffered message until the
    buffer is empty (or cap hit). Clears stale fire-and-forget Input responses
    + page events so a subsequent awaited _cdp finds its own response fast."""
    n = 0
    while n < max_msgs:
        try:
            await asyncio.wait_for(ws.recv(), timeout=0.002)
            n += 1
        except (asyncio.TimeoutError, Exception):
            break
    return n


async def _recv(ws, mid, timeout=30):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=max(0.05, end - time.monotonic()))
        except asyncio.TimeoutError:
            continue
        if not raw or not isinstance(raw, (str, bytes)):
            continue
        try:
            d = json.loads(raw)
        except Exception:
            continue
        if isinstance(d, dict) and d.get("id") == mid:
            return d
    raise TimeoutError(f"no response for id={mid}")


_n = [0]


async def _cdp(ws, method, params, sid=None):
    await _drain(ws)  # clear stale events / fire-and-forget responses
    _n[0] += 1
    mid = _n[0]
    await _send(ws, mid, method, params, sid)
    return await _recv(ws, mid)


# ── flow helpers (operate on an attached session) ───────────────────────────
class WsMux:
    """Single-reader multiplexer over one CDP websocket + attached session.

    The browser-level ws (Target.attachToTarget, flatten=true) multiplexes many
    sessions and a busy page (google results) floods it with events. If the
    client only reads while awaiting a specific response, fire-and-forget Input
    bursts + page-load events fill the ws buffer -> a raw cdp-forward's sendall
    to the client blocks -> chrome's send buffer fills -> the forwarder closes
    the connection abruptly (ConnectionClosedError "no close frame").

    A single background reader drains the ws continuously: responses with a
    known id resolve their future; everything else (events, unmatched acks) is
    discarded. The buffer stays empty so the forwarder never blocks. Use
    `await mux.cmd(...)` for awaited commands and `mux.fire(...)` for
    fire-and-forget Input events (their acks are discarded by the reader).
    """

    def __init__(self, ws, sid, on_event=None):
        self.ws = ws
        self.sid = sid
        self.on_event = on_event  # optional callback(method:str, params:dict)
        self._id = 0
        self._wait = {}  # id -> asyncio.Future
        self._closed = asyncio.Event()
        self._exit_reason = None
        self._reader = asyncio.create_task(self._read_loop())

    async def _read_loop(self):
        try:
            while True:
                raw = await self.ws.recv()
                if raw is None:
                    self._exit_reason = "recv returned None"
                    break
                try:
                    d = json.loads(raw)
                except Exception:
                    continue
                if isinstance(d, dict) and "id" in d:
                    fut = self._wait.pop(d["id"], None)
                    if fut and not fut.done():
                        fut.set_result(d)
                    # an id'd message can also carry events; still notify
                    if self.on_event and "method" in d:
                        try:
                            self.on_event(d["method"], d.get("params", {}))
                        except Exception:
                            pass
                elif isinstance(d, dict) and "method" in d:
                    if self.on_event:
                        try:
                            self.on_event(d["method"], d.get("params", {}))
                        except Exception:
                            pass
                # events + unmatched fire-and-forget acks: discard if no callback
        except Exception as _e:
            self._exit_reason = f"{type(_e).__name__}: {_e}"
        else:
            if not getattr(self, "_exit_reason", None):
                self._exit_reason = "loop ended without exception"
        finally:
            self._closed.set()
            for f in self._wait.values():
                if not f.done():
                    f.cancel()

    def _next_id(self):
        self._id += 1
        return self._id

    async def cmd(self, method, params=None, timeout=30):
        mid = self._next_id()
        fut = asyncio.get_event_loop().create_future()
        self._wait[mid] = fut
        msg = {"id": mid, "method": method, "params": params or {}}
        if self.sid is not None:
            msg["sessionId"] = self.sid
        await self.ws.send(json.dumps(msg))
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._wait.pop(mid, None)
            raise
        except asyncio.CancelledError:
            raise ConnectionError(
                f"ws reader exited during {method} (mid={mid}): {self._exit_reason}")

    async def fire(self, method, params=None):
        """Fire-and-forget; the reader discards the ack. Keeps the stream dense
        without congesting the ws (response is read + discarded by _read_loop)."""
        mid = self._next_id()
        msg = {"id": mid, "method": method, "params": params or {}}
        if self.sid is not None:
            msg["sessionId"] = self.sid
        await self.ws.send(json.dumps(msg))
        # no waiter registered -> reader discards the ack

    async def eval(self, expr, await_promise=False, timeout=30):
        p = {"expression": expr, "returnByValue": True}
        if await_promise:
            p["awaitPromise"] = True
        r = await self.cmd("Runtime.evaluate", p, timeout=timeout)
        return r.get("result", {}).get("result", {}).get("value")

    async def rect(self, selector_js, timeout=30):
        expr = (f"(function(){{var e={selector_js};if(!e)return null;var r=e.getBoundingClientRect();"
                f"return [r.x,r.y,r.width,r.height];}})()")
        return await self.eval(expr, timeout=timeout)

    async def aclose(self):
        self._reader.cancel()
        try:
            await self.ws.close()
        except Exception:
            pass


async def eval_js(ws, sid, expr, await_promise=False):
    p = {"expression": expr, "returnByValue": True}
    if await_promise:
        p["awaitPromise"] = True
    r = await _cdp(ws, "Runtime.evaluate", p, sid)
    return r.get("result", {}).get("result", {}).get("value")


async def get_rect(ws, sid, selector_js):
    """selector_js: a JS expression evaluating to an Element. Returns (x,y,w,h) or None."""
    expr = (f"(function(){{var e={selector_js};if(!e)return null;var r=e.getBoundingClientRect();"
            f"return [r.x,r.y,r.width,r.height];}})()")
    v = await eval_js(ws, sid, expr)
    return v


async def move_bezier(ws, sid, x0, y0, x1, y1, rng, vw, vh, speed=1.0):
    """Cubic-Bezier move from (x0,y0)->(x1,y1) with ease-in/out + tremor."""
    c1x = x0 + (x1 - x0) * rng.uniform(0.1, 0.4) + rng.uniform(-40, 40)
    c1y = y0 + (y1 - y0) * rng.uniform(0.1, 0.4) + rng.uniform(-40, 40)
    c2x = x0 + (x1 - x0) * rng.uniform(0.6, 0.9) + rng.uniform(-40, 40)
    c2y = y0 + (y1 - y0) * rng.uniform(0.6, 0.9) + rng.uniform(-40, 40)
    dist = math.hypot(x1 - x0, y1 - y0)
    steps = max(8, min(30, int(dist / 18 * speed) + rng.randint(4, 10)))
    cx, cy = x0, y0
    for i in range(1, steps + 1):
        t = i / steps
        te = 0.5 - 0.5 * math.cos(math.pi * t)  # ease-in/out
        mt = 1 - te
        px = mt ** 3 * x0 + 3 * mt ** 2 * te * c1x + 3 * mt * te ** 2 * c2x + te ** 3 * x1
        py = mt ** 3 * y0 + 3 * mt ** 2 * te * c1y + 3 * mt * te ** 2 * c2y + te ** 3 * y1
        px += rng.uniform(-1.2, 1.2)
        py += rng.uniform(-1.2, 1.2)
        px = max(0, min(vw, px)); py = max(0, min(vh, py))
        await _fire(ws, "Input.dispatchMouseEvent",
                   {"type": "mouseMoved", "x": px, "y": py, "button": "none",
                    "modifiers": 0, "timestamp": _ts()}, sid)
        cx, cy = px, py
        await asyncio.sleep(rng.uniform(0.008, 0.022))
    return cx, cy


async def click_at(ws, sid, x, y, rng):
    """Move near (x,y) with a short bezier, then press+release (trusted click)."""
    sx = x + rng.uniform(-3, 3); sy = y + rng.uniform(-3, 3)
    await _fire(ws, "Input.dispatchMouseEvent",
               {"type": "mouseMoved", "x": sx, "y": sy, "button": "none",
                "modifiers": 0, "timestamp": _ts()}, sid)
    await asyncio.sleep(rng.uniform(0.05, 0.12))
    await _fire(ws, "Input.dispatchMouseEvent",
               {"type": "mousePressed", "x": sx, "y": sy, "button": "left",
                "clickCount": 1, "modifiers": 0, "timestamp": _ts()}, sid)
    await asyncio.sleep(rng.uniform(0.04, 0.09))
    await _fire(ws, "Input.dispatchMouseEvent",
               {"type": "mouseReleased", "x": sx, "y": sy, "button": "left",
                "clickCount": 1, "modifiers": 0, "timestamp": _ts()}, sid)
    await asyncio.sleep(rng.uniform(0.08, 0.18))


async def type_text(ws, sid, text, rng):
    for ch in text:
        await _fire(ws, "Input.dispatchKeyEvent",
                   {"type": "keyDown", "key": ch, "text": ch,
                    "modifiers": 0, "timestamp": _ts()}, sid)
        await _fire(ws, "Input.dispatchKeyEvent",
                   {"type": "keyUp", "key": ch, "text": ch,
                    "modifiers": 0, "timestamp": _ts()}, sid)
        await asyncio.sleep(rng.uniform(0.06, 0.16))


async def press_key(ws, sid, key, code, vk=0):
    for typ in ("keyDown", "keyUp"):
        await _fire(ws, "Input.dispatchKeyEvent",
                   {"type": typ, "key": key, "code": code,
                    "windowsVirtualKeyCode": vk, "modifiers": 0, "timestamp": _ts()}, sid)
        await asyncio.sleep(0.05)


async def scroll_by(ws, sid, x, y, dy, rng):
    await _fire(ws, "Input.dispatchMouseEvent",
               {"type": "mouseWheel", "x": x, "y": y, "button": "none",
                "modifiers": 0, "deltaX": 0, "deltaY": dy, "timestamp": _ts()}, sid)
    await asyncio.sleep(rng.uniform(0.15, 0.4))


# ── mux-aware input helpers (use WsMux.fire; reader discards acks) ──────────
async def move_bezier_m(mux, x0, y0, x1, y1, rng, vw, vh, speed=1.0):
    c1x = x0 + (x1 - x0) * rng.uniform(0.1, 0.4) + rng.uniform(-40, 40)
    c1y = y0 + (y1 - y0) * rng.uniform(0.1, 0.4) + rng.uniform(-40, 40)
    c2x = x0 + (x1 - x0) * rng.uniform(0.6, 0.9) + rng.uniform(-40, 40)
    c2y = y0 + (y1 - y0) * rng.uniform(0.6, 0.9) + rng.uniform(-40, 40)
    dist = math.hypot(x1 - x0, y1 - y0)
    steps = max(8, min(30, int(dist / 18 * speed) + rng.randint(4, 10)))
    cx, cy = x0, y0
    for i in range(1, steps + 1):
        t = i / steps
        te = 0.5 - 0.5 * math.cos(math.pi * t)
        mt = 1 - te
        px = mt ** 3 * x0 + 3 * mt ** 2 * te * c1x + 3 * mt * te ** 2 * c2x + te ** 3 * x1
        py = mt ** 3 * y0 + 3 * mt ** 2 * te * c1y + 3 * mt * te ** 2 * c2y + te ** 3 * y1
        px += rng.uniform(-1.2, 1.2); py += rng.uniform(-1.2, 1.2)
        px = max(0, min(vw, px)); py = max(0, min(vh, py))
        await mux.fire("Input.dispatchMouseEvent",
                   {"type": "mouseMoved", "x": px, "y": py, "button": "none",
                    "modifiers": 0, "timestamp": _ts()})
        cx, cy = px, py
        await asyncio.sleep(rng.uniform(0.008, 0.022))
    return cx, cy


async def click_at_m(mux, x, y, rng):
    sx = x + rng.uniform(-3, 3); sy = y + rng.uniform(-3, 3)
    await mux.fire("Input.dispatchMouseEvent",
               {"type": "mouseMoved", "x": sx, "y": sy, "button": "none",
                "modifiers": 0, "timestamp": _ts()})
    await asyncio.sleep(rng.uniform(0.05, 0.12))
    await mux.fire("Input.dispatchMouseEvent",
               {"type": "mousePressed", "x": sx, "y": sy, "button": "left",
                "clickCount": 1, "modifiers": 0, "timestamp": _ts()})
    await asyncio.sleep(rng.uniform(0.04, 0.09))
    await mux.fire("Input.dispatchMouseEvent",
               {"type": "mouseReleased", "x": sx, "y": sy, "button": "left",
                "clickCount": 1, "modifiers": 0, "timestamp": _ts()})
    await asyncio.sleep(rng.uniform(0.08, 0.18))


async def scroll_by_m(mux, x, y, dy, rng):
    await mux.fire("Input.dispatchMouseEvent",
               {"type": "mouseWheel", "x": x, "y": y, "button": "none",
                "modifiers": 0, "deltaX": 0, "deltaY": dy, "timestamp": _ts()})
    await asyncio.sleep(rng.uniform(0.15, 0.4))


async def type_text_m(mux, text, rng):
    for ch in text:
        await mux.fire("Input.dispatchKeyEvent",
                   {"type": "keyDown", "key": ch, "text": ch,
                    "modifiers": 0, "timestamp": _ts()})
        await mux.fire("Input.dispatchKeyEvent",
                   {"type": "keyUp", "key": ch, "text": ch,
                    "modifiers": 0, "timestamp": _ts()})
        await asyncio.sleep(rng.uniform(0.06, 0.16))


async def press_key_m(mux, key, code, vk=0):
    for typ in ("keyDown", "keyUp"):
        await mux.fire("Input.dispatchKeyEvent",
                   {"type": typ, "key": key, "code": code,
                    "windowsVirtualKeyCode": vk, "modifiers": 0, "timestamp": _ts()})
        await asyncio.sleep(0.05)


async def humanize_mux(mux, seconds=8.0, vw=2556, vh=1398, rng=None, on_stop=None):
    """Dense trusted-input stream over a WsMux (single ws, no subprocess, no
    second connection). Bursts of bezier moves + reading scrolls + occasional
    non-interactive click + keystroke, all isTrusted=true. The mux reader drains
    every ack/event so cdp-forward never blocks. Used both on the google search
    page (warmth) and on the realestate page (overlaps Kasada's sensor window)."""
    if rng is None:
        rng = random.SystemRandom()
    x = rng.uniform(vw * 0.2, vw * 0.8); y = rng.uniform(vh * 0.2, vh * 0.8)
    await mux.fire("Input.dispatchMouseEvent",
               {"type": "mouseMoved", "x": x, "y": y, "button": "none",
                "modifiers": 0, "timestamp": _ts()})
    deadline = time.monotonic() + seconds

    async def burst():
        nonlocal x, y
        for _ in range(rng.randint(1, 3)):
            if time.monotonic() >= deadline or (on_stop and on_stop.is_set()):
                return
            tx = rng.uniform(0, vw); ty = rng.uniform(0, vh)
            x, y = await move_bezier_m(mux, x, y, tx, ty, rng, vw, vh)
        await asyncio.sleep(rng.uniform(0.05, 0.4))

    while time.monotonic() < deadline and not (on_stop and on_stop.is_set()):
        await burst()
        if rng.random() < 0.4:
            dy = rng.randint(-220, 260)
            if dy:
                await mux.fire("Input.dispatchMouseEvent",
                           {"type": "mouseWheel", "x": x, "y": y, "button": "none",
                            "modifiers": 0, "deltaX": 0, "deltaY": dy, "timestamp": _ts()})
                await asyncio.sleep(rng.uniform(0.2, 0.7))
        if rng.random() < 0.12:
            cx, cy = rng.uniform(0, vw), rng.uniform(0, vh)
            await click_at_m(mux, cx, cy, rng)
        if rng.random() < 0.08:
            key = rng.choice(["KeyA", "KeyJ", "KeyL", "Space", "ArrowDown", "ArrowUp"])
            code = " " if key == "Space" else key
            for typ in ("keyDown", "keyUp"):
                await mux.fire("Input.dispatchKeyEvent",
                           {"type": typ, "key": key, "code": code,
                            "windowsVirtualKeyCode": 0, "modifiers": 0, "timestamp": _ts()})
                await asyncio.sleep(0.05)


# ── continuous humanize stream ──────────────────────────────────────────────
async def humanize(http_url, seconds=8.0, vw=2556, vh=1398, seed=None, ws=None, sid=None,
                   on_stop=None):
    """Stream human-like trusted input for `seconds`. If ws/sid are None, attach
    to the first page target via a fresh browser ws (standalone mode). If
    on_stop (an asyncio.Event) is set, return early."""
    rng = random.Random(seed) if seed is not None else random.SystemRandom()
    own_ws = ws is None
    if own_ws:
        if not http_url.startswith("http"):
            http_url = "http://" + http_url
        ver = _http_json(http_url.rstrip("/") + "/json/version")
        bws = ver["webSocketDebuggerUrl"]
        targets = _http_json(http_url.rstrip("/") + "/json/list")
        page = next((t for t in targets if t.get("type") == "page"), None)
        if not page:
            return 0
        ws = await websockets.connect(bws, max_size=None)
        r = await _cdp(ws, "Target.attachToTarget", {"targetId": page["id"], "flatten": True})
        sid = r["result"]["sessionId"]
        await _cdp(ws, "Page.enable", {}, sid)
        await _cdp(ws, "Runtime.enable", {}, sid)

    x = rng.uniform(vw * 0.2, vw * 0.8); y = rng.uniform(vh * 0.2, vh * 0.8)
    await _fire(ws, "Input.dispatchMouseEvent",
               {"type": "mouseMoved", "x": x, "y": y, "button": "none",
                "modifiers": 0, "timestamp": _ts()}, sid)
    deadline = time.monotonic() + seconds
    sent = 0

    async def burst():
        nonlocal x, y, sent
        nwp = rng.randint(1, 3)
        for _ in range(nwp):
            if time.monotonic() >= deadline or (on_stop and on_stop.is_set()):
                return
            tx = rng.uniform(0, vw); ty = rng.uniform(0, vh)
            x, y = await move_bezier(ws, sid, x, y, tx, ty, rng, vw, vh)
            sent += 1
        # idle gap between bursts
        await asyncio.sleep(rng.uniform(0.05, 0.4))

    while time.monotonic() < deadline and not (on_stop and on_stop.is_set()):
        await burst()
        # reading scroll sometimes
        if rng.random() < 0.4:
            dy = rng.randint(-220, 260)
            if dy:
                await _fire(ws, "Input.dispatchMouseEvent",
                           {"type": "mouseWheel", "x": x, "y": y, "button": "none",
                            "modifiers": 0, "deltaX": 0, "deltaY": dy, "timestamp": _ts()}, sid)
                sent += 1
                await asyncio.sleep(rng.uniform(0.2, 0.7))
        # occasional click at non-interactive spot
        if rng.random() < 0.12:
            cx, cy = rng.uniform(0, vw), rng.uniform(0, vh)
            await click_at(ws, sid, cx, cy, rng)
            sent += 3
        # occasional keystroke
        if rng.random() < 0.08:
            key = rng.choice(["KeyA", "KeyJ", "KeyL", "Space", "ArrowDown", "ArrowUp"])
            code = " " if key == "Space" else key
            for typ in ("keyDown", "keyUp"):
                await _fire(ws, "Input.dispatchKeyEvent",
                           {"type": typ, "key": key, "code": code,
                            "windowsVirtualKeyCode": 0, "modifiers": 0, "timestamp": _ts()}, sid)
                sent += 1
                await asyncio.sleep(0.05)

    if own_ws:
        await ws.close()
    return sent


def main():
    url = sys.argv[1]; secs = float(sys.argv[2]) if len(sys.argv) > 2 else 8.0
    vw = int(sys.argv[3]) if len(sys.argv) > 3 else 2556
    vh = int(sys.argv[4]) if len(sys.argv) > 4 else 1398
    n = asyncio.run(humanize(url, secs, vw, vh))
    print(f"humanize: dispatched {n} trusted input events over {secs:.1f}s")


if __name__ == "__main__":
    main()
