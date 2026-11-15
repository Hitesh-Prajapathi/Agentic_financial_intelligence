"""Shared helpers for the chat tests: a tiny in-memory DuckDB with the v2 tables (own DDL, independent of migrations),
optional v3 tables, and a recording shim that exposes the two methods the tools use."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import duckdb

BASE_DDL = """
CREATE TABLE filings (accession_number TEXT PRIMARY KEY, filing_date TIMESTAMP, issuer_name TEXT, issuer_ticker TEXT,
    issuer_cik TEXT, insider_name TEXT, insider_title TEXT, is_officer BOOLEAN DEFAULT FALSE, is_director BOOLEAN DEFAULT FALSE,
    is_ten_pct BOOLEAN DEFAULT FALSE, is_10b5_1 BOOLEAN DEFAULT FALSE, form_type TEXT DEFAULT '4');
CREATE TABLE transactions (id INTEGER, accession_number TEXT, row_idx INTEGER, transaction_date DATE, security_title TEXT,
    code TEXT, acquired_disposed TEXT, shares DOUBLE, price_per_share DOUBLE, dollar_value DOUBLE, ownership_nature TEXT,
    post_transaction_shares DOUBLE);
CREATE TABLE top_trades (id INTEGER, run_id TEXT, run_date DATE, rank INTEGER, issuer_ticker TEXT, issuer_name TEXT,
    insider_name TEXT, insider_title TEXT, net_dollar_value DOUBLE, buy_value DOUBLE, sell_value DOUBLE,
    transaction_count INTEGER, insider_count INTEGER, pct_holdings DOUBLE, planned_pct DOUBLE, dominant_direction TEXT,
    signal_tags TEXT, validation_checksum TEXT);
CREATE TABLE sentiment_summary (ticker TEXT, summary_date DATE, total_tweets INTEGER, bullish_count INTEGER,
    bearish_count INTEGER, neutral_count INTEGER, avg_confidence DOUBLE, sentiment_index DOUBLE);
CREATE TABLE pipeline_runs (run_id TEXT PRIMARY KEY, started_at TIMESTAMP, completed_at TIMESTAMP, status TEXT,
    trigger TEXT, stages JSON, filings_ingested INTEGER DEFAULT 0, trades_ranked INTEGER DEFAULT 0, error_message TEXT);
CREATE TABLE query_log (id INTEGER PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, question TEXT, intent TEXT,
    confidence TEXT, latency_ms INTEGER, model TEXT, rows_used INTEGER, feedback INTEGER);
CREATE SEQUENCE qlog_seq;
"""

GRAPH_DDL = """
ALTER TABLE filings ADD COLUMN owner_ciks TEXT;
ALTER TABLE transactions ADD COLUMN superseded BOOLEAN DEFAULT FALSE;
CREATE TABLE reporting_owners (accession_number TEXT NOT NULL, owner_cik TEXT NOT NULL, owner_name TEXT,
    is_director BOOLEAN DEFAULT FALSE, is_officer BOOLEAN DEFAULT FALSE, is_ten_pct BOOLEAN DEFAULT FALSE,
    is_other BOOLEAN DEFAULT FALSE, officer_title TEXT, PRIMARY KEY (accession_number, owner_cik));
CREATE TABLE insider_edges (owner_cik TEXT NOT NULL, owner_name TEXT, issuer_cik TEXT NOT NULL, issuer_ticker TEXT,
    issuer_name TEXT, is_director BOOLEAN, is_officer BOOLEAN, is_ten_pct BOOLEAN, officer_title TEXT,
    first_accepted_at TIMESTAMP, last_accepted_at TIMESTAMP, n_filings INTEGER, buy_value DOUBLE, sell_value DOUBLE,
    n_buys INTEGER, n_sells INTEGER, PRIMARY KEY (owner_cik, issuer_cik));
"""

# (accession, filed, ticker, company, cik, insider, title, officer, director, ten_pct, plan, [(code, trade_date, shares, price)])
FILINGS = [
    (
        "0001-26-000001",
        "2026-11-10 14:00",
        "ACME",
        "Acme Robotics Inc.",
        "111",
        "Whitfield Dana",
        "Chief Executive Officer",
        True,
        True,
        False,
        True,
        [("S", "2026-11-09", 180000, 52.40)],
    ),
    (
        "0001-26-000002",
        "2026-11-10 15:00",
        "ACME",
        "Acme Robotics Inc.",
        "111",
        "Lee Marcus",
        "Chief Financial Officer",
        True,
        False,
        False,
        True,
        [("S", "2026-11-09", 40000, 52.10)],
    ),
    (
        "0001-26-000003",
        "2026-11-12 13:00",
        "GLBX",
        "Globex Energy Corp.",
        "222",
        "Natarajan Priya",
        "Director",
        False,
        True,
        False,
        False,
        [("P", "2026-11-11", 60000, 18.25)],
    ),
    (
        "0001-26-000004",
        "2026-11-12 16:00",
        "GLBX",
        "Globex Energy Corp.",
        "222",
        "Okafor Tom",
        "Chief Operating Officer",
        True,
        False,
        False,
        False,
        [("P", "2026-11-11", 25000, 18.30)],
    ),
    (
        "0001-26-000005",
        "2026-11-13 09:00",
        "GLBX",
        "Globex Energy Corp.",
        "222",
        "Brandt Helen",
        "Director",
        False,
        True,
        False,
        False,
        [("P", "2026-11-12", 15000, 18.10)],
    ),
    (
        "0001-26-000006",
        "2026-11-13 10:00",
        "NOVA",
        "Nova Biotherapeutics",
        "333",
        "Ortiz Samuel",
        "10% Owner",
        False,
        False,
        True,
        False,
        [("S", "2026-11-12", 250000, 7.80)],
    ),
    (
        "0001-26-000007",
        "2026-11-13 11:00",
        "QRTZ",
        "Quartz Semiconductor",
        "444",
        "Natarajan Priya",
        "Director",
        False,
        True,
        False,
        False,
        [("P", "2026-11-12", 3000, 140.00)],
    ),
    (
        "0001-26-000008",
        "2026-10-20 11:00",
        "ACME",
        "Acme Robotics Inc.",
        "111",
        "Whitfield Dana",
        "Chief Executive Officer",
        True,
        True,
        False,
        False,
        [("S", "2026-10-19", 10000, 49.00)],
    ),
]
# owner cik per insider name (for the optional graph tables)
CIKS = {
    "Whitfield Dana": "0000000001",
    "Lee Marcus": "0000000002",
    "Natarajan Priya": "0000000003",
    "Okafor Tom": "0000000004",
    "Brandt Helen": "0000000005",
    "Ortiz Samuel": "0000000006",
}


class MemDB:
    """Minimal DB handle for the tools: query / query_df; records every SQL string issued."""

    def __init__(self, graph: bool = False):
        self.conn = duckdb.connect(":memory:")
        self.sql_log: list[tuple[str, list]] = []
        self.conn.execute(BASE_DDL)
        if graph:
            self.conn.execute(GRAPH_DDL)
        self._seed(graph)

    def _seed(self, graph: bool):
        n = 0
        for acc, filed, tk, co, cik, who, title, off, dirc, ten, plan, txns in FILINGS:
            self.conn.execute(
                "INSERT INTO filings (accession_number, filing_date, issuer_name, issuer_ticker, issuer_cik, insider_name, "
                "insider_title, is_officer, is_director, is_ten_pct, is_10b5_1) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [acc, datetime.fromisoformat(filed), co, tk, cik, who, title, off, dirc, ten, plan],
            )
            for i, (code, td, sh, px) in enumerate(txns):
                n += 1
                self.conn.execute(
                    "INSERT INTO transactions (id, accession_number, row_idx, transaction_date, security_title, code, acquired_disposed, shares, price_per_share, dollar_value, ownership_nature, post_transaction_shares) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    [
                        n,
                        acc,
                        i,
                        date.fromisoformat(td),
                        "Common Stock",
                        code,
                        "A" if code == "P" else "D",
                        float(sh),
                        px,
                        round(sh * px, 2),
                        "D",
                        None,
                    ],
                )
            if graph:
                self.conn.execute(
                    "INSERT INTO reporting_owners VALUES (?,?,?,?,?,?,FALSE,?)",
                    [acc, CIKS[who], who, dirc, off, ten, title],
                )
        if graph:
            self.conn.execute("""INSERT INTO insider_edges
                SELECT ro.owner_cik, ro.owner_name, f.issuer_cik, f.issuer_ticker, f.issuer_name, BOOL_OR(ro.is_director), BOOL_OR(ro.is_officer),
                       BOOL_OR(ro.is_ten_pct), MAX(ro.officer_title), MIN(f.filing_date), MAX(f.filing_date), COUNT(DISTINCT f.accession_number),
                       SUM(CASE WHEN t.code='P' THEN t.dollar_value END), SUM(CASE WHEN t.code='S' THEN t.dollar_value END),
                       SUM(CASE WHEN t.code='P' THEN 1 ELSE 0 END), SUM(CASE WHEN t.code='S' THEN 1 ELSE 0 END)
                FROM reporting_owners ro JOIN filings f USING (accession_number) JOIN transactions t USING (accession_number)
                GROUP BY ro.owner_cik, ro.owner_name, f.issuer_cik, f.issuer_ticker, f.issuer_name""")
        # rankings: two days
        tt = [
            (
                "2026-11-13",
                1,
                "NOVA",
                "Nova Biotherapeutics",
                "Ortiz Samuel",
                "10% Owner",
                -1950000.0,
                0.0,
                1950000.0,
                1,
                1,
                0.33,
                0.0,
                "SELL",
                "LARGE_STAKE",
            ),
            (
                "2026-11-13",
                2,
                "GLBX",
                "Globex Energy Corp.",
                "Natarajan Priya",
                "Director",
                1821500.0,
                1821500.0,
                0.0,
                3,
                3,
                0.43,
                0.0,
                "BUY",
                "CLUSTER_BUY",
            ),
            (
                "2026-11-13",
                3,
                "ACME",
                "Acme Robotics Inc.",
                "Whitfield Dana",
                "Chief Executive Officer",
                -9500000.0,
                0.0,
                9500000.0,
                2,
                2,
                0.2,
                1.0,
                "SELL",
                "PLANNED",
            ),
            (
                "2026-11-12",
                1,
                "ACME",
                "Acme Robotics Inc.",
                "Whitfield Dana",
                "Chief Executive Officer",
                -17375692.0,
                0.0,
                17375692.0,
                2,
                2,
                0.2,
                1.0,
                "SELL",
                "PLANNED",
            ),
        ]
        for i, t in enumerate(tt, 1):
            self.conn.execute(
                "INSERT INTO top_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [i, "run1", date.fromisoformat(t[0]), *t[1:], "chk"],
            )
        self.conn.execute(
            "INSERT INTO sentiment_summary VALUES ('GLBX','2026-11-12',10,6,1,3,0.8,0.5),('GLBX','2026-11-13',10,8,0,2,0.8,0.8)"
        )
        self.conn.execute(
            "INSERT INTO pipeline_runs VALUES ('run1','2026-11-13 08:00','2026-11-13 08:03','success','manual','{}',8,3,NULL)"
        )

    # tool-facing API
    def query(self, sql, params=None):
        self.sql_log.append((sql, list(params or [])))
        return self.conn.execute(sql, params or []).fetchall()

    def query_df(self, sql, params=None):
        self.sql_log.append((sql, list(params or [])))
        return self.conn.execute(sql, params or []).df()

    def data_as_of(self):
        return datetime.utcnow() - timedelta(hours=2)

    def empty(self):
        for t in ("filings", "transactions", "top_trades", "sentiment_summary", "pipeline_runs"):
            self.conn.execute(f"DELETE FROM {t}")
        return self

    # service-facing API (query_log)
    def log_query(self, question, intent, confidence, latency_ms, model, rows_used, **_extra):
        return self.conn.execute(
            "INSERT INTO query_log (id, question, intent, confidence, latency_ms, model, rows_used) "
            "VALUES (nextval('qlog_seq'),?,?,?,?,?,?) RETURNING id",
            [question[:500], intent, confidence, latency_ms, model, rows_used],
        ).fetchone()[0]

    def set_feedback(self, qid, value):
        self.conn.execute("UPDATE query_log SET feedback=? WHERE id=?", [value, qid])

    def latest_top_trades(self, limit=20):
        return self.query_df(
            "SELECT * FROM top_trades WHERE run_date=(SELECT MAX(run_date) FROM top_trades) ORDER BY rank LIMIT ?",
            [limit],
        )


# ── scripted fake LLM with the tool API ──────────────────────────────────────
from insider_intel.llm.client import LLMError, LLMResult, NoToolSupportError, ToolCall  # noqa: E402


def call(name, args, cid=None):
    import json

    return ToolCall(cid or f"c_{name}", name, args if isinstance(args, str) else json.dumps(args))


def reply(text="", *calls):
    return LLMResult(text, "fake", "tool-model", 1, 10, 5, list(calls))


class FakeToolLLM:
    """chat_tools() pops scripted replies; stream() is the degraded path. Records every request."""

    last_label = "fake:tool-model"

    def __init__(self, script=(), no_tools=False, fail=False, stream_text="ok"):
        self.script, self.no_tools, self.fail, self.stream_text = list(script), no_tools, fail, stream_text
        self.requests: list[dict] = []

    def supports_tools(self):
        return not self.no_tools

    def chat_tools(self, messages, tools, tool_choice="auto", **kw):
        self.requests.append(
            {"messages": [dict(m) for m in messages], "tool_choice": tool_choice, "tools": tools, **kw}
        )
        if self.no_tools:
            raise NoToolSupportError("none")
        if self.fail:
            raise LLMError("down")
        return self.script.pop(0)

    def stream(self, messages, **kw):
        if self.fail:
            raise LLMError("down")
        yield from (w + " " for w in self.stream_text.split(" "))

    def chain(self):
        return []
