"""Kasada re-test: navigate headed Chrome to realestate.com.au/buy while streaming
trusted human-like input, then poll whether the challenge is solved.

Solved criteria:
  - document.body.innerText.length > 2000  (real listing content), AND/OR
  - a KP_UIDz cookie is set with a non-trivial value, AND/OR
  - title is non-empty and not the challenge stub.

Also reports window.outerWidth/innerWidth (headless tell) and navigator.webdriver
to confirm the Chrome-defaults flags took effect.
"""
import sys, json, time, asyncio, urllib.request
import websockets
import cdp_humanize


def _http_json(url, timeout=15):
    return json.loads(urllib.request.urlopen(url, timeout=timeout).read().decode())


PROBE = ('JSON.stringify({'
         'title:document.title,'
         'len:document.body?document.body.innerText.length:0,'
         'url:location.href,'
         'wd:typeof navigator.webdriver,'
         'ow:window.outerWidth,'
         'iw:window.innerWidth,'
         'oh:window.outerHeight,'
         'ih:window.innerHeight,'
         'kasada:/kpsdk|Kasada|KPSDK/.test(document.documentElement.outerHTML),'
         'snippet:(document.body?document.body.innerText.slice(0,160):"")'
         '})')


async def run(http_url, target_url, humanize_secs=8.0):
    if not http_url.startswith("http"):
        http_url = "http://" + http_url
    ver = _http_json(http_url.rstrip("/") + "/json/version")
    bws = ver["webSocketDebuggerUrl"]
    targets = _http_json(http_url.rstrip("/") + "/json/list")
    page = next((t for t in targets if t.get("type") == "page"), None)
    tid = page["id"] if page else None

    async with websockets.connect(bws, max_size=None) as ws:
        if tid is None:
            r = await cdp_humanize._cdp(ws, "Target.createTarget", {"url": "about:blank"})
            tid = r["result"]["targetId"]
        r = await cdp_humanize._cdp(ws, "Target.attachToTarget", {"targetId": tid, "flatten": True})
        sid = r["result"]["sessionId"]
        await cdp_humanize._cdp(ws, "Page.enable", {}, sid)
        await cdp_humanize._cdp(ws, "Runtime.enable", {}, sid)
        await cdp_humanize._cdp(ws, "Network.enable", {}, sid)
        await cdp_humanize._cdp(ws, "Page.navigate", {"url": target_url}, sid)

        # Stream trusted human input concurrently while the sensor collects.
        vw = _http_json(http_url.rstrip("/") + "/json/list")
        human_task = asyncio.create_task(cdp_humanize.humanize(http_url, humanize_secs))

        last = None
        for t in range(4, 45, 4):
            await asyncio.sleep(4)
            try:
                r = await cdp_humanize._cdp(ws, "Runtime.evaluate",
                                            {"expression": PROBE, "returnByValue": True}, sid, )
                v = r.get("result", {}).get("result", {}).get("value")
                d = json.loads(v) if v else {}
                cookies = await cdp_humanize._cdp(ws, "Network.getCookies", {"urls": [target_url]}, sid)
                ck = [c["name"] for c in cookies.get("result", {}).get("cookies", [])]
                kp = [c for c in cookies.get("result", {}).get("cookies", []) if c["name"].startswith("KP_UIDz")]
                line = (f"t+{t}s: title={d.get('title')!r} len={d.get('len')} "
                        f"kasada={d.get('kasada')} wd={d.get('wd')} "
                        f"outer={d.get('ow')}x{d.get('oh')} inner={d.get('iw')}x{d.get('ih')} "
                        f"cookies={ck}")
                print(line)
                last = d
                if kp:
                    print(f"  -> KP_UIDz cookie present (value len {len(kp[0].get('value',''))}) — challenge likely solved")
                if d.get("len", 0) > 2000:
                    print(f"  -> CONTENT LOADED (challenge solved). snippet: {d.get('snippet')!r}")
                    human_task.cancel()
                    break
            except Exception as e:
                print(f"t+{t}s: eval err {e}")
        try:
            n = await human_task
            print(f"humanize stream dispatched {n} trusted events")
        except asyncio.CancelledError:
            print("humanize stream cancelled (content loaded early)")
        return last


def main():
    url = sys.argv[1]
    target = sys.argv[2] if len(sys.argv) > 2 else "https://www.realestate.com.au/buy"
    secs = float(sys.argv[3]) if len(sys.argv) > 3 else 8.0
    asyncio.run(run(url, target, secs))


if __name__ == "__main__":
    main()
