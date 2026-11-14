"""Live connectivity checks for every external service (used by the Health tab and `python -m insider_intel check-keys`)."""
from __future__ import annotations

import time
from datetime import date, timedelta

import httpx

from insider_intel.config import settings


def _last_weekday(today: date | None = None) -> date:
    d = today or date.today()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def check_sec(cfg=settings, transport: httpx.BaseTransport | None = None) -> dict:
    """One tiny EDGAR request (HEAD of the latest weekday's daily index). No request if SEC_USER_AGENT is unset."""
    name = "SEC EDGAR"
    if not cfg.has_sec_user_agent:
        return {"service": name, "ok": False, "detail": "SEC_USER_AGENT not set"}
    from insider_intel.edgar.index import daily_index_url

    url = daily_index_url(_last_weekday())
    t0 = time.time()
    try:
        with httpx.Client(transport=transport, follow_redirects=True, timeout=20) as c:
            r = c.head(url, headers={"User-Agent": cfg.sec_user_agent})
        ms = int((time.time() - t0) * 1000)
        if r.status_code == 403:
            return {"service": name, "ok": False, "detail": "HTTP 403: SEC blocked the request (check SEC_USER_AGENT)"}
        if r.status_code in (200, 404):  # 404 = index not published yet / holiday; the host is reachable
            return {"service": name, "ok": True, "detail": f"reachable · {ms} ms"}
        return {"service": name, "ok": False, "detail": f"HTTP {r.status_code}"}
    except Exception as e:  # noqa: BLE001
        return {"service": name, "ok": False, "detail": str(e)[:100]}


def check_llm(llm) -> list[dict]:
    out = []
    for provider, res in llm.ping().items():
        extra = ""
        if provider == "openrouter" and res["ok"]:
            q = llm.openrouter_quota()
            if q.get("free_remaining") is not None:
                extra = f" · {q['free_remaining']}/{q['free_limit']} free requests left today"
        if provider == "groq" and llm.stats.groq_remaining_requests is not None:
            extra = f" · {llm.stats.groq_remaining_requests} requests left today"
        out.append({"service": f"LLM · {provider}", "ok": res["ok"], "detail": res["detail"] + extra})
    return out


def check_all(system) -> list[dict]:
    return [check_sec(), *check_llm(system.llm)]
