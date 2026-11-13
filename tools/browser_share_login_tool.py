#!/usr/bin/env python3
"""Share a site's browser login across every webtop thread.

Each thread has its own Chrome profile. ``tools/webtop_shared_logins.py`` keeps
a host-side cookie store and syncs it with every webtop, but only for an
allowlist of domains. This tool lets the agent add a site to that list after
signing in (or remove one), and lists what the store holds. Cookie values are
never returned.
"""

import json

from tools.registry import registry, tool_error


def _mod():
    from tools import webtop_shared_logins as m
    return m


def browser_share_login(action: str = "list", domain: str = "", task_id=None) -> str:
    m = _mod()
    action = (action or "list").strip().lower()
    defaults = list(m._DEFAULT_DOMAINS)
    extras = m.extra_domains()

    if action == "list":
        return json.dumps({
            "built_in_domains": defaults,
            "added_domains": extras,
            "store": m.store_summary(),
        }, ensure_ascii=False)

    if action not in ("add", "remove"):
        return tool_error("action must be 'add', 'remove' or 'list'.")
    d = m.normalize_domain(domain)
    if not d:
        return tool_error(f"'{domain}' is not a usable site domain (give e.g. 'example.com.au'; "
                          "bare suffixes like 'com.au' are refused).")

    if action == "remove":
        if d in defaults:
            return tool_error(f"{d} is built in and cannot be removed here.")
        if d not in extras:
            return json.dumps({"success": True, "domain": d, "note": "was not shared"})
        m.set_extra_domains([x for x in extras if x != d])
        # Cookies already copied into other threads stay there; only future syncs stop.
        return json.dumps({"success": True, "domain": d, "removed": True,
                           "note": "Stopped sharing. Threads that already got the "
                                   "cookies keep them until they expire."})

    if d not in defaults and d not in extras:
        m.set_extra_domains(extras + [d])
    container = None
    try:
        from tools.browser_webtop_pool import sync_task_logins
        container = sync_task_logins(task_id)
    except Exception:
        pass
    return json.dumps({
        "success": True,
        "domain": d,
        "synced_from": container,
        "store": m.store_summary([d]),
        "note": ("Login cookies for this site are now kept in the shared store and set "
                 "in every webtop on its next browser call." if container else
                 "Domain added. No running webtop for this thread, so nothing was "
                 "copied yet; it will be on the next browser call."),
    }, ensure_ascii=False)


def _check() -> bool:
    try:
        from tools.browser_webtop_pool import pool_enabled
        return pool_enabled() and _mod().enabled()
    except Exception:
        return False


BROWSER_SHARE_LOGIN_SCHEMA = {
    "name": "browser_share_login",
    "description": (
        "Keep a website login across all browser threads. Each thread has its own "
        "browser profile; sites on the shared list have their cookies synced, so a "
        "login made in one thread works in every thread and cron job and survives "
        "restarts. Slack, Microsoft/Outlook, Google and Monash are built in. "
        "action='add' after you (or the user) sign in to another site the user will "
        "want logged in elsewhere, or when the user asks to keep a login; it also "
        "copies this thread's current login for that site into the store right away. "
        "Ask the user first for banking, payment, government or health sites. "
        "action='remove' stops sharing an added site. action='list' shows the shared "
        "sites and which cookie names the store holds (never values)."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["add", "remove", "list"],
                "description": "add or remove a site, or list what is shared.",
            },
            "domain": {
                "type": "string",
                "description": (
                    "Site domain or URL for add/remove, e.g. 'monash.edu' or "
                    "'https://portal.example.com.au/login'. Use the site's main domain "
                    "so its SSO/login subdomains are covered; add the SSO provider's "
                    "domain separately if it differs (e.g. 'example.okta.com')."
                ),
            },
        },
        "required": ["action"],
    },
}


registry.register(
    name="browser_share_login",
    toolset="browser",
    schema=BROWSER_SHARE_LOGIN_SCHEMA,
    handler=lambda args, **kw: browser_share_login(
        action=args.get("action", "list"),
        domain=args.get("domain", ""),
        task_id=kw.get("task_id"),
    ),
    check_fn=_check,
    emoji="🔑",
)
