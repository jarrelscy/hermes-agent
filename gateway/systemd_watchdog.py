"""systemd watchdog for the gateway event loop.

A tool that spins while holding the GIL (e.g. a catastrophically backtracking
regex in a memory plugin) freezes every platform at once while the process
still looks "active" to systemd. This module pings systemd's watchdog from
the event loop itself, so a stalled loop stops the pings and systemd kills
the gateway with SIGABRT and restarts it. ``faulthandler`` is enabled on
SIGABRT, so every thread's stack is written to ``logs/gateway-faults.log``
before the process dies, which shows what was spinning.

The watchdog is armed at runtime with ``WATCHDOG_USEC`` once the gateway is
up, so a slow startup can never trip it. It needs ``NotifyAccess=main`` (or
``all``) on the unit; without ``NOTIFY_SOCKET`` everything here is a no-op.
"""

import asyncio
import faulthandler
import logging
import os
import socket

logger = logging.getLogger(__name__)

_fault_file = None


def _notify(message: str) -> bool:
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return False
    if addr.startswith("@"):
        addr = "\0" + addr[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.connect(addr)
            sock.sendall(message.encode())
        return True
    except OSError as exc:
        logger.debug("systemd notify failed: %s", exc)
        return False


def enable_fault_dumps(log_dir) -> None:
    """Dump all thread stacks to ``<log_dir>/gateway-faults.log`` on SIGABRT/SIGSEGV."""
    global _fault_file
    if _fault_file is not None:
        return
    try:
        _fault_file = open(os.path.join(str(log_dir), "gateway-faults.log"), "a", buffering=1)
        faulthandler.enable(file=_fault_file, all_threads=True)
    except Exception as exc:
        logger.warning("Could not enable gateway fault dumps: %s", exc)


async def run_watchdog(timeout_seconds: int = 180, interval_seconds: int = 20) -> None:
    """Arm the systemd watchdog and ping it from the event loop until cancelled."""
    if not os.environ.get("NOTIFY_SOCKET"):
        return
    if not _notify(f"WATCHDOG_USEC={timeout_seconds * 1_000_000}\nWATCHDOG=1"):
        return
    logger.info("systemd watchdog armed (timeout=%ds, ping every %ds)",
                timeout_seconds, interval_seconds)
    while True:
        await asyncio.sleep(interval_seconds)
        _notify("WATCHDOG=1")
