import json
import re

import pytest
from chat_testkit import MemDB

from insider_intel.chat.tools import ToolContext, build_registry

REG = build_registry()


def run(db, name, args=None, **ctx_kw):
    return REG.call(ToolContext(db, **ctx_kw), name, args or {})


def sources(o):
    return [r["source"] for r in o.result["rows"]]


def test_schemas_are_plain_json_schema():
    tools = REG.openai_tools()
    assert [t["function"]["name"] for t in tools] == [
        "top_trades",
        "filings_for",
        "insider_history",
        "cluster_events",
        "insider_network",
        "co_insiders",
        "sentiment",
        "data_status",
    ]
    for t in tools:
        blob = json.dumps(t)
        assert "$ref" not in blob and "$defs" not in blob and "anyOf" not in blob and '"title"' not in blob
        assert t["function"]["parameters"]["type"] == "object"
    props = {t["function"]["name"]: t["function"]["parameters"]["properties"] for t in tools}
    assert props["top_trades"]["direction"]["enum"] == ["BUY", "SELL"]
    assert props["filings_for"]["days"]["default"] == 14


def test_top_trades_latest_direction_and_sources(memdb):
    o = run(memdb, "top_trades")
    assert o.ok and o.n_rows == 3 and o.result["total_matched"] == 3
    assert sources(o) == ["RANK 2026-11-13 #1", "RANK 2026-11-13 #2", "RANK 2026-11-13 #3"]
    assert o.result["rows"][1]["largest_trade_pct_of_position"] == 43.0
    sell = run(memdb, "top_trades", {"direction": "sell", "n": 1})
    assert sell.n_rows == 1 and sell.result["total_matched"] == 2 and sell.result["truncated"] is True


def test_top_trades_date_and_days(memdb):
    assert sources(run(memdb, "top_trades", {"date": "2026-11-12"})) == ["RANK 2026-11-12 #1"]
    assert run(memdb, "top_trades", {"days": 2, "n": 20}).n_rows == 4
    empty = run(memdb, "top_trades", {"date": "2020-01-01"})
    assert empty.ok and empty.n_rows == 0 and "No ranking" in empty.result["note"]


def test_filings_for_ticker_has_totals_and_sources(memdb):
    o = run(memdb, "filings_for", {"ticker": "$glbx"})
    assert o.n_rows == 3 and o.result["totals"]["n_filings"] == 3
    assert o.result["totals"]["buy_value"] == pytest.approx(60000 * 18.25 + 25000 * 18.30 + 15000 * 18.10)
    assert o.result["totals"]["sell_value"] is None
    assert all(re.fullmatch(r"SEC Form 4 0001-26-00000\d", s) for s in sources(o))
    assert o.result["rows"][0]["dollar_value"] >= o.result["rows"][1]["dollar_value"]
    assert o.result["window"]["to"] == "2026-11-13"  # anchored on the data, not on today's date


def test_filings_for_window_codes_and_owner_word_order(memdb):
    assert (
        run(memdb, "filings_for", {"owner": "Dana Whitfield", "days": 5}).n_rows == 1
    )  # Oct 20 filing is outside 5 days
    assert run(memdb, "filings_for", {"owner": "whitfield dana", "days": 30}).n_rows == 2
    assert run(memdb, "filings_for", {"ticker": "ACME", "days": 30, "codes": "P"}).n_rows == 0
    assert run(memdb, "filings_for", {"ticker": "ACME", "days": 30, "codes": ["S"]}).n_rows == 3


def test_empty_result_is_explicit(memdb):
    o = run(memdb, "filings_for", {"ticker": "ZZZZ"})
    assert o.ok and o.n_rows == 0 and "No matching" in o.result["note"]


def test_insider_history_groups_by_issuer(memdb):
    o = run(memdb, "insider_history", {"owner": "Priya Natarajan"})
    assert o.n_rows == 2 and {r["ticker"] for r in o.result["by_issuer"]} == {"GLBX", "QRTZ"}
    assert o.result["matched_insiders"] == ["Natarajan Priya"]


def test_cluster_events(memdb):
    o = run(memdb, "cluster_events", {"days": 30})
    by = {(r["ticker"], r["direction"]): r for r in o.result["rows"]}
    assert by[("GLBX", "BUY")]["n_insiders"] == 3 and by[("ACME", "SELL")]["n_insiders"] == 2
    assert sources(o)[0].startswith("CLUSTER ")
    assert run(memdb, "cluster_events", {"direction": "BUY", "min_insiders": 4}).n_rows == 0


def test_sentiment_aggregates_only(memdb):
    o = run(memdb, "sentiment", {"ticker": "GLBX"})
    r = o.result["rows"][0]
    assert r["posts"] == 20 and r["bullish"] == 14 and r["source"] == "SENT GLBX" and "text" not in r
    assert run(memdb, "sentiment", {"ticker": "ACME"}).n_rows == 0


def test_data_status_reports_age(memdb):
    from datetime import datetime

    o = run(memdb, "data_status", rank_window_days=14, now=lambda: datetime(2026, 11, 20, 12))
    r = o.result["rows"][0]
    assert r["source"] == "RUN run1" and r["filings_total"] == 8
    assert r["newest_filing_date"] == "2026-11-13" and r["newest_filing_age_days"] == 7
    assert r["latest_ranking_age_days"] == 7 and r["ranking_window_days"] == 14
    assert "twice a month" in r["freshness_note"]


def test_data_status_on_empty_db():
    o = run(MemDB().empty(), "data_status")
    assert o.ok and o.result["rows"][0]["filings_total"] == 0


# ── graph tools: with and without the optional tables ────────────────────────
def test_network_tools_degrade_to_name_matching_without_graph_tables(memdb):
    n = run(memdb, "insider_network", {"owner": "Natarajan"})
    assert n.ok and n.result["match"] == "name" and {r["ticker"] for r in n.result["rows"]} == {"GLBX", "QRTZ"}
    assert sources(n)[0].startswith("GRAPH Natarajan")
    c = run(memdb, "co_insiders", {"ticker": "GLBX"})
    assert c.n_rows == 3 and "name" in c.result["note"]


def test_network_tools_use_insider_edges_when_present(graphdb):
    n = run(graphdb, "insider_network", {"owner": "Natarajan"})
    assert {r["ticker"] for r in n.result["rows"]} == {"GLBX", "QRTZ"} and set(sources(n)) == {"GRAPH 0000000003"}
    assert run(graphdb, "insider_network", {"owner": "0000000003"}).n_rows == 2
    c = run(graphdb, "co_insiders", {"ticker": "GLBX"})
    by = {r["insider"]: r for r in c.result["rows"]}
    assert by["Natarajan Priya"]["n_other_companies"] == 1 and by["Okafor Tom"]["n_other_companies"] == 0
    assert by["Okafor Tom"]["source"] == "GRAPH 0000000004"


def test_owner_by_cik_and_superseded_rows_with_v3_columns(graphdb):
    assert run(graphdb, "filings_for", {"owner": "0000000003", "days": 30}).n_rows == 2
    graphdb.conn.execute("UPDATE transactions SET superseded = TRUE WHERE accession_number = '0001-26-000003'")
    o = run(graphdb, "filings_for", {"owner": "0000000003", "days": 30})
    assert o.n_rows == 1 and o.result["rows"][0]["ticker"] == "QRTZ"


def test_missing_tables_report_not_available():
    class Bare(MemDB):
        def __init__(self):
            import duckdb

            self.conn, self.sql_log = duckdb.connect(":memory:"), []

    b = Bare()
    for name, args in [
        ("top_trades", {}),
        ("filings_for", {"ticker": "A"}),
        ("sentiment", {"ticker": "A"}),
        ("insider_network", {"owner": "Bob"}),
        ("cluster_events", {}),
    ]:
        o = run(b, name, args)
        assert o.ok and o.result["available"] is False and o.n_rows == 0, name


# ── validation / safety ──────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "name,args",
    [
        ("filings_for", {}),
        ("filings_for", {"ticker": "BAD TICKER!"}),
        ("filings_for", {"ticker": "A", "days": 0}),
        ("filings_for", {"ticker": "A", "codes": ["PP"]}),
        ("top_trades", {"n": 99}),
        ("top_trades", {"date": "11/12/2026"}),
        ("top_trades", {"date": "2026-11-12", "days": 3}),
        ("top_trades", {"direction": "HOLD"}),
        ("top_trades", {"bogus": 1}),
        ("cluster_events", {"min_insiders": 1}),
        ("insider_history", {"owner": "x"}),
        ("sentiment", {}),
    ],
)
def test_invalid_args_become_error_outcomes(memdb, name, args):
    o = run(memdb, name, args)
    assert not o.ok and o.error.startswith("invalid arguments") and json.loads(o.message)["error"] == o.error


def test_unknown_tool_and_bad_json(memdb):
    o = REG.call(ToolContext(memdb), "drop_tables", {})
    assert not o.ok and "unknown tool" in o.error and "top_trades" in o.error
    assert "not valid JSON" in REG.call(ToolContext(memdb), "top_trades", "{oops").error
    assert REG.call(ToolContext(memdb), "top_trades", "").ok  # empty arguments string == {}


def test_sql_injection_text_is_only_ever_a_bound_parameter(memdb):
    evil = "x'; DROP TABLE filings; --"
    o = run(memdb, "filings_for", {"owner": evil})
    assert o.ok and o.n_rows == 0
    assert memdb.query("SELECT COUNT(*) FROM filings")[0][0] == 8
    assert run(memdb, "insider_history", {"owner": "100%_\\"}).n_rows == 0  # LIKE wildcards are escaped
    assert not run(memdb, "filings_for", {"ticker": "A'; DROP TABLE filings;--"}).ok
    for sql, _params in memdb.sql_log:
        assert "DROP" not in sql.upper() and "x'" not in sql


def test_only_select_statements_are_issued(memdb):
    ctx = ToolContext(memdb)
    for name, args in [
        ("top_trades", {}),
        ("filings_for", {"ticker": "GLBX"}),
        ("insider_history", {"owner": "Priya"}),
        ("cluster_events", {}),
        ("insider_network", {"owner": "Priya"}),
        ("co_insiders", {"ticker": "GLBX"}),
        ("sentiment", {"ticker": "GLBX"}),
        ("data_status", {}),
    ]:
        REG.call(ctx, name, args)
    assert memdb.sql_log
    for sql, _ in memdb.sql_log:
        assert re.match(r"\s*(SELECT|WITH)\b", sql, re.I), sql


def test_results_are_capped_in_size(memdb):
    for i in range(200):
        memdb.conn.execute(
            "INSERT INTO filings (accession_number, filing_date, issuer_ticker, insider_name) VALUES (?,?,?,?)",
            [f"BIG{i}", "2026-11-13 10:00", "BIG", "Name " * 12 + str(i)],
        )
        memdb.conn.execute(
            "INSERT INTO transactions (id, accession_number, row_idx, transaction_date, code, shares, price_per_share, dollar_value) "
            "VALUES (?,?,?,?,?,?,?,?)",
            [1000 + i, f"BIG{i}", 0, "2026-11-12", "P", 1.0, 1.0, 1.0],
        )
    o = run(memdb, "filings_for", {"ticker": "BIG", "limit": 30})
    assert len(o.message) <= 6200 and o.result["truncated"] and o.result["total_matched"] == 200
