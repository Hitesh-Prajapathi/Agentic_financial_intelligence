"""Live connectivity checks for every external service (used by the Health tab and scripts/check_keys.py)."""
from __future__ import annotations

import time

import httpx

from config.settings import settings


def check_sec() -> dict:
    if not settings.has_key("sec_api_key"):
        return {"service": "sec-api.io", "ok": False, "detail": "SEC_API_KEY missing"}
    t0 = time.time()
    try:
        r = httpx.post("https://api.sec-api.io/insider-trading", params={"token": settings.sec_api_key}, timeout=20,
                       json={"query": 'documentType:"4"', "from": "0", "size": "1", "sort": [{"filedAt": {"order": "desc"}}]})
        if r.status_code in (401, 403):
            return {"service": "sec-api.io", "ok": False, "detail": f"key rejected (HTTP {r.status_code})"}
        r.raise_for_status()
        total = r.json().get("total", {}).get("value")
        return {"service": "sec-api.io", "ok": True, "detail": f"{total:,} Form 4s indexed · {int((time.time()-t0)*1000)} ms"}
    except Exception as e:  # noqa: BLE001
        return {"service": "sec-api.io", "ok": False, "detail": str(e)[:100]}


def check_apify(apify_tool) -> dict:
    if not settings.has_key("apify_api_token"):
        return {"service": "Apify", "ok": False, "detail": "APIFY_API_TOKEN missing"}
    u = apify_tool.usage()
    if not u.get("ok"):
        return {"service": "Apify", "ok": False, "detail": u.get("error", "unreachable")}
    return {"service": "Apify", "ok": True,
            "detail": f"${u['used_usd']:.2f} of ${min(u['plan_limit_usd'], settings.apify_monthly_budget_usd):.2f} budget used this month"}


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
    return [check_sec(), check_apify(system.apify), *check_llm(system.llm)]
