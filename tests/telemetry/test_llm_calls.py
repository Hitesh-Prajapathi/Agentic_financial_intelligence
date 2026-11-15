"""llm_calls writer (on_call hook), notional cost, read-only skip, degraded-retrieval filtering."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from insider_intel.chat.retrieval import Retriever
from insider_intel.config import Settings
from insider_intel.store.db import DBTool
from insider_intel.telemetry import current_run_id, estimate_cost_usd, llm_usage, make_llm_call_writer

REC = {
    "ts": "2026-11-14T10:00:00+00:00",
    "purpose": "agent_round_1",
    "provider": "groq",
    "model": "openai/gpt-oss-120b",
    "status": "ok",
    "latency_ms": 800,
    "prompt_tokens": 1_000_000,
    "completion_tokens": 500_000,
    "n_tool_calls": 1,
    "prompt_hash": "h1",
}


def test_writer_inserts_row_with_notional_cost_and_run_id(tmp_path):
    db, cfg = DBTool(str(tmp_path / "a.duckdb")), Settings()
    w = make_llm_call_writer(db, cfg)
    tok = current_run_id.set("run-1")
    try:
        w(REC)
        w({**REC, "status": "error", "model": "unknown/model", "latency_ms": 100})
    finally:
        current_run_id.reset(tok)
    rows = db.query("SELECT run_id, purpose, status, prompt_hash, est_cost_usd FROM llm_calls ORDER BY id")
    assert rows[0][:4] == ("run-1", "agent_round_1", "ok", "h1") and abs(rows[0][4] - (0.15 + 0.30)) < 1e-9
    assert rows[1][4] == 0.0  # unknown model -> 0
    u = llm_usage(db)
    assert u["calls"] == 2 and u["failed"] == 1 and u["failure_rate"] == 0.5
    assert estimate_cost_usd(cfg, "nope", 10, 10) == 0.0


def test_writer_never_raises_and_skips_read_only(tmp_path):
    p = str(tmp_path / "b.duckdb")
    DBTool(p).close()
    ro = DBTool(p, read_only=True)
    make_llm_call_writer(ro, Settings())(REC)  # silently skipped
    assert ro.query("SELECT COUNT(*) FROM llm_calls") == [(0,)]
    broken = SimpleNamespace(read_only=False, execute=lambda *a, **k: 1 / 0)
    make_llm_call_writer(broken, Settings())(REC)  # exception swallowed
    ro.close()


def test_llmtool_on_call_hook_gets_prompt_hash(tmp_path):
    from insider_intel.llm.client import LLMTool

    seen = []
    llm = LLMTool(Settings(), on_call=seen.append)
    llm._note_prompt([{"role": "user", "content": "hi"}])
    llm._emit("p", "groq", "m", "ok")
    assert seen[0]["prompt_hash"] and len(seen[0]["prompt_hash"]) == 16


def test_degraded_retrieval_excludes_non_open_market_and_superseded(tmp_path):
    db, cfg = DBTool(str(tmp_path / "c.duckdb")), Settings()
    now = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=1)
    db.insert_filings(
        [
            {
                "accession_number": "A-1",
                "filing_date": now,
                "issuer_ticker": "ACME",
                "issuer_name": "Acme",
                "insider_name": "Dana",
            }
        ]
    )
    rows = [(0, "S", 100, False), (1, "A", 999_999, False), (2, "S", 777_777, True), (3, "P", 50, False)]
    db.insert_transactions(
        [
            {"accession_number": "A-1", "row_idx": i, "code": c, "shares": sh, "price_per_share": 10.0}
            for i, c, sh, _ in rows
        ]
    )
    db.execute("UPDATE transactions SET superseded = TRUE WHERE row_idx = 2")
    db.execute(
        "INSERT INTO top_trades (run_id, run_date, rank, issuer_ticker, net_dollar_value, buy_value, sell_value) "
        "VALUES ('r', CURRENT_DATE, 1, 'ACME', 1, 1, 0)"
    )
    ctx, _ = Retriever(db, cfg).sql_context(["ACME"])
    assert "100 sh" in ctx and "50 sh" in ctx
    assert "999,999" not in ctx and "777,777" not in ctx
