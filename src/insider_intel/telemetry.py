"""LLM call telemetry (P3): writes one `llm_calls` row per provider call via LLMTool's `on_call` hook.

Cost is NOTIONAL: tokens x `settings.llm_list_prices` (approximate public list prices, an ESTIMATE). Only free-tier
models are used, so nothing is billed; the number answers "what would this have cost at paid rates".
"""
from __future__ import annotations

import logging
from contextvars import ContextVar
from datetime import datetime

logger = logging.getLogger(__name__)

current_run_id: ContextVar[str | None] = ContextVar("insider_intel_run_id", default=None)
current_session_id: ContextVar[str | None] = ContextVar("insider_intel_session_id", default=None)


def estimate_cost_usd(cfg, model: str, prompt_tokens: int, completion_tokens: int) -> float:
    """Notional USD at list price (per 1M tokens, in/out); 0.0 for models without a listed price."""
    price = (getattr(cfg, "llm_list_prices", None) or {}).get(model)
    if not price:
        return 0.0
    return (prompt_tokens or 0) / 1e6 * float(price[0]) + (completion_tokens or 0) / 1e6 * float(price[1])


def make_llm_call_writer(db, cfg):
    """Return an `on_call` hook that inserts into llm_calls. Best effort: never raises; no-op on a read-only DB."""
    def write(rec: dict) -> None:
        if getattr(db, "read_only", False):
            return
        try:
            pt, ct = int(rec.get("prompt_tokens") or 0), int(rec.get("completion_tokens") or 0)
            ts = rec.get("ts")
            ts = datetime.fromisoformat(ts).replace(tzinfo=None) if isinstance(ts, str) else ts
            db.execute(
                """INSERT INTO llm_calls (ts, run_id, session_id, purpose, provider, model, prompt_hash, status, latency_ms,
                                          prompt_tokens, completion_tokens, n_tool_calls, est_cost_usd)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [ts, current_run_id.get(), current_session_id.get(), rec.get("purpose"), rec.get("provider"), rec.get("model"),
                 rec.get("prompt_hash"), rec.get("status"), int(rec.get("latency_ms") or 0), pt, ct,
                 int(rec.get("n_tool_calls") or 0), estimate_cost_usd(cfg, rec.get("model") or "", pt, ct)])
        except Exception:  # noqa: BLE001 - telemetry must never break a call
            logger.warning("llm_calls write failed", exc_info=True)
    return write


def llm_usage(db) -> dict:
    """Aggregates for the Health tab. Empty dict if there are no rows (or the table is missing)."""
    try:
        n, ok, p50, p95, ptok, ctok, cost = db.query(
            """SELECT COUNT(*), SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END), quantile_cont(latency_ms, 0.5),
                      quantile_cont(latency_ms, 0.95), SUM(prompt_tokens), SUM(completion_tokens), SUM(est_cost_usd)
               FROM llm_calls WHERE status = 'ok'""")[0]
        total, failed = db.query("SELECT COUNT(*), SUM(CASE WHEN status <> 'ok' THEN 1 ELSE 0 END) FROM llm_calls")[0]
        if not total:
            return {}
        by_model = db.query_df("""SELECT provider || ':' || model AS model, COUNT(*) AS calls,
                                         SUM(CASE WHEN status <> 'ok' THEN 1 ELSE 0 END) AS failed,
                                         SUM(COALESCE(prompt_tokens,0) + COALESCE(completion_tokens,0)) AS tokens
                                  FROM llm_calls GROUP BY 1 ORDER BY calls DESC""")
        by_purpose = db.query_df("""SELECT purpose, COUNT(*) AS calls, SUM(CASE WHEN status <> 'ok' THEN 1 ELSE 0 END) AS failed
                                    FROM llm_calls GROUP BY 1 ORDER BY calls DESC""")
        return {"calls": int(total), "failed": int(failed or 0), "failure_rate": (failed or 0) / total,
                "p50_ms": float(p50 or 0), "p95_ms": float(p95 or 0), "prompt_tokens": int(ptok or 0),
                "completion_tokens": int(ctok or 0), "est_cost_usd": float(cost or 0), "by_model": by_model,
                "by_purpose": by_purpose}
    except Exception:  # noqa: BLE001
        logger.warning("llm_usage failed", exc_info=True)
        return {}
