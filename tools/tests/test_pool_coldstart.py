"""Regression test for the webtop-pool cold-start stall (2026-07-11).

The bug: the first browser call in a thread hung for minutes / forever because
_create_container ran the ~28s Kasada mint SYNCHRONOUSLY on the create path
(eager prewarm default ON), and persisted state only AFTER the mint — so a
slow/failed mint blocked the caller's get_cdp_url() and orphaned the container.

These tests lock in the fixes with docker + mint fully mocked (no containers):
  1. Prewarm defaults to LAZY (off) — no synchronous mint on the create path.
  2. _create_container persists state + marks running BEFORE any prewarm, and a
     prewarm that raises does NOT discard the container (still saved + usable).
  3. get_cdp_url() returns a real CDP URL for a cold task, promptly.

Run on the box:  venv/bin/python -m pytest tools/tests/test_pool_coldstart.py -q
"""
import os
import sys
import types

sys.path.insert(0, os.path.expanduser("~/.hermes/hermes-agent/tools"))


def _fresh_pool(monkeypatch, tmp_path, prewarm_env=None):
    """Import a fresh pool module with docker/waits/mint mocked out."""
    os.environ["HERMES_WEBTOP_POOL"] = "1"
    os.environ["HERMES_WEBTOP_POOL_CONFIG_ROOT"] = str(tmp_path)
    if prewarm_env is None:
        os.environ.pop("HERMES_WEBTOP_POOL_KASADA_PREWARM_ON_START", None)
    else:
        os.environ["HERMES_WEBTOP_POOL_KASADA_PREWARM_ON_START"] = prewarm_env
    for m in [k for k in list(sys.modules) if k == "browser_webtop_pool"]:
        del sys.modules[m]
    import browser_webtop_pool as b
    # Neutralise everything that would touch docker / the network / real waits.
    b._LOADED = True
    b._MAPPING = {}
    b._run = lambda *a, **k: types.SimpleNamespace(returncode=0, stdout="true", stderr="")
    b._used_host_ports = lambda: set()
    b._container_exists = lambda name: False
    b._container_running = lambda name: False
    b._ensure_seed = lambda *a, **k: None
    b._ensure_cua_driver_in_container = lambda *a, **k: None
    b._ensure_chrome_in_container = lambda *a, **k: None
    b._launch_chrome = lambda *a, **k: None
    b._wait_x_session = lambda *a, **k: True
    b._wait_desktop_ready = lambda *a, **k: True
    b._start_reaper = lambda: None
    b._save_state = lambda: None
    return b


def test_prewarm_defaults_to_lazy(monkeypatch, tmp_path):
    b = _fresh_pool(monkeypatch, tmp_path)
    assert b._prewarm_on_start() is False, "eager prewarm on create must NOT be the default"


def test_cold_start_does_not_run_mint_when_lazy(monkeypatch, tmp_path):
    b = _fresh_pool(monkeypatch, tmp_path)
    minted = {"called": False}

    def _boom_mint(*a, **k):
        minted["called"] = True
        raise AssertionError("mint must not run on the create path when lazy")

    b._prewarm_kasada = _boom_mint
    url = b.get_cdp_url("task-lazy")
    assert url.startswith("http://127.0.0.1:"), f"expected a pool CDP url, got {url!r}"
    assert minted["called"] is False
    # container recorded as running in the mapping
    e = b._MAPPING["task-lazy"]
    assert e["running"] is True and e["cdp_port"]


def test_failed_eager_mint_still_persists_container(monkeypatch, tmp_path):
    b = _fresh_pool(monkeypatch, tmp_path, prewarm_env="1")
    saved = {"count": 0}
    orig_save = b._save_state
    b._save_state = lambda: saved.__setitem__("count", saved["count"] + 1)

    def _raising_mint(*a, **k):
        raise RuntimeError("mint blew up")

    b._prewarm_kasada = _raising_mint
    # must NOT raise out of get_cdp_url even though the eager mint fails
    url = b.get_cdp_url("task-eager")
    assert url.startswith("http://127.0.0.1:")
    e = b._MAPPING["task-eager"]
    assert e["running"] is True, "container must stay usable despite mint failure"
    assert saved["count"] >= 1, "state must be persisted even when the mint raises"


if __name__ == "__main__":
    # Standalone runner (the box venv has no pytest). Each test gets a fresh
    # temp config root; monkeypatch is unused by these tests (they assign pool
    # attributes directly), so a stub is fine.
    import tempfile, traceback
    tests = [
        test_prewarm_defaults_to_lazy,
        test_cold_start_does_not_run_mint_when_lazy,
        test_failed_eager_mint_still_persists_container,
    ]
    failed = 0
    for t in tests:
        d = tempfile.mkdtemp(prefix="pooltest-")
        try:
            t(None, d)
            print(f"PASS  {t.__name__}")
        except Exception:
            failed += 1
            print(f"FAIL  {t.__name__}")
            traceback.print_exc()
    print(f"\n{'ALL PASSED' if not failed else str(failed)+' FAILED'} ({len(tests)-failed}/{len(tests)})")
    raise SystemExit(1 if failed else 0)
