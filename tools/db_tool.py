"""Thread-safe DuckDB access.

DuckDB allows a single read/write connection per process; Streamlit runs many threads
(UI, background pipeline, chat), so every call goes through one re-entrant lock.
"""
from __future__ import annotations

import json
import os
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = ROOT / "db" / "schema.sql"


class DBTool:
    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.conn = duckdb.connect(path)
        self._lock = threading.RLock()
        self.init_db()

    # ── core ────────────────────────────────────────────────────────────────
    def init_db(self) -> None:
        with self._lock:
            self.conn.execute(SCHEMA_PATH.read_text())

    def query(self, sql: str, params: list | None = None) -> list[tuple]:
        with self._lock:
            return self.conn.execute(sql, params or []).fetchall()

    def execute(self, sql: str, params: list | None = None) -> None:
        with self._lock:
            self.conn.execute(sql, params or [])

    def query_df(self, sql: str, params: list | None = None) -> pd.DataFrame:
        with self._lock:
            return self.conn.execute(sql, params or []).df()

    def executemany(self, sql: str, rows: list) -> None:
        if not rows:
            return
        with self._lock:
            self.conn.executemany(sql, rows)

    @contextmanager
    def transaction(self):
        """Atomic multi-statement write; rolls back on any exception."""
        with self._lock:
            self.conn.execute("BEGIN TRANSACTION")
            try:
                yield self
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    # ── filings / transactions ──────────────────────────────────────────────
    def existing_accessions(self, accession_numbers: list[str]) -> set[str]:
        if not accession_numbers:
            return set()
        ph = ",".join("?" * len(accession_numbers))
        rows = self.query(f"SELECT accession_number FROM filings WHERE accession_number IN ({ph})", accession_numbers)
        return {r[0] for r in rows}

    def insert_filings(self, filings: list[dict]) -> None:
        self.executemany(
            """INSERT INTO filings (accession_number, filing_date, issuer_name, issuer_ticker, issuer_cik,
                   insider_name, insider_title, is_officer, is_director, is_ten_pct, is_10b5_1, form_type)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
            [(f["accession_number"], f.get("filing_date"), f.get("issuer_name"), f.get("issuer_ticker"),
              f.get("issuer_cik"), f.get("insider_name"), f.get("insider_title"),
              bool(f.get("is_officer")), bool(f.get("is_director")), bool(f.get("is_ten_pct")),
              bool(f.get("is_10b5_1")), f.get("form_type", "4")) for f in filings],
        )

    def insert_transactions(self, txns: list[dict]) -> None:
        self.executemany(
            """INSERT INTO transactions (accession_number, row_idx, transaction_date, security_title, code,
                   acquired_disposed, shares, price_per_share, dollar_value, ownership_nature, post_transaction_shares)
               VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
            [(t["accession_number"], t["row_idx"], t.get("transaction_date"), t.get("security_title"), t.get("code"),
              t.get("acquired_disposed"), t.get("shares"), t.get("price_per_share"), t.get("dollar_value"),
              t.get("ownership_nature"), t.get("post_transaction_shares")) for t in txns],
        )

    # ── ranking output ──────────────────────────────────────────────────────
    def replace_top_trades(self, run_id: str, run_date: str, trades: list[dict]) -> None:
        """Idempotent per day: a later run on the same date replaces the earlier ranking."""
        with self.transaction():
            self.execute("DELETE FROM top_trades WHERE run_date = ?", [run_date])
            self.executemany(
                """INSERT INTO top_trades (run_id, run_date, rank, issuer_ticker, issuer_name, insider_name,
                       insider_title, net_dollar_value, buy_value, sell_value, transaction_count, insider_count,
                       pct_holdings, planned_pct, dominant_direction, signal_tags, validation_checksum)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [(run_id, run_date, t["rank"], t["issuer_ticker"], t.get("issuer_name"), t.get("insider_name"),
                  t.get("insider_title"), t["net_dollar_value"], t["buy_value"], t["sell_value"],
                  t["transaction_count"], t["insider_count"], t.get("pct_holdings"), t.get("planned_pct"),
                  t["dominant_direction"], t.get("signal_tags", ""), t.get("validation_checksum")) for t in trades],
            )

    def latest_top_trades(self, limit: int = 20) -> pd.DataFrame:
        return self.query_df(
            """SELECT * FROM top_trades
               WHERE run_date = (SELECT MAX(run_date) FROM top_trades)
               ORDER BY rank LIMIT ?""", [limit])

    # ── tweets / sentiment ──────────────────────────────────────────────────
    def insert_tweets(self, tweets: list[dict]) -> int:
        before = self.query("SELECT COUNT(*) FROM tweets")[0][0]
        self.executemany(
            """INSERT INTO tweets (tweet_id, ticker, text, author_username, author_followers, created_at,
                   retweet_count, like_count, reply_count) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
            [(t["tweet_id"], t["ticker"], t["text"], t.get("author_username"), t.get("author_followers"),
              t.get("created_at"), t.get("retweet_count", 0), t.get("like_count", 0), t.get("reply_count", 0))
             for t in tweets],
        )
        return self.query("SELECT COUNT(*) FROM tweets")[0][0] - before

    def insert_sentiments(self, rows: list[dict]) -> None:
        now = datetime.utcnow()
        self.executemany(
            """INSERT INTO tweet_sentiments (tweet_id, ticker, sentiment_label, confidence_score, model, reviewed, classified_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT (tweet_id) DO UPDATE SET sentiment_label = EXCLUDED.sentiment_label,
                   confidence_score = EXCLUDED.confidence_score, model = EXCLUDED.model,
                   reviewed = EXCLUDED.reviewed, classified_at = EXCLUDED.classified_at""",
            [(r["tweet_id"], r["ticker"], r["sentiment_label"], r["confidence_score"], r.get("model"),
              bool(r.get("reviewed")), now) for r in rows],
        )

    def refresh_sentiment_summary(self, tickers: list[str] | None = None) -> None:
        """Recompute per-ticker, per-tweet-day aggregates straight from the labelled tweets."""
        where, params = "", []
        if tickers:
            where = f"WHERE t.ticker IN ({','.join('?' * len(tickers))})"
            params = list(tickers)
        self.execute(
            f"""INSERT OR REPLACE INTO sentiment_summary
                SELECT t.ticker, CAST(t.created_at AS DATE) AS d,
                       COUNT(*),
                       SUM(CASE WHEN s.sentiment_label='bullish' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN s.sentiment_label='bearish' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN s.sentiment_label='neutral' THEN 1 ELSE 0 END),
                       AVG(s.confidence_score),
                       (SUM(CASE WHEN s.sentiment_label='bullish' THEN 1 ELSE 0 END)
                        - SUM(CASE WHEN s.sentiment_label='bearish' THEN 1 ELSE 0 END)) * 1.0 / COUNT(*)
                FROM tweets t JOIN tweet_sentiments s USING (tweet_id)
                {where}
                GROUP BY t.ticker, CAST(t.created_at AS DATE)""",
            params,
        )

    # ── pipeline bookkeeping ────────────────────────────────────────────────
    def start_run(self, run_id: str, started_at: datetime, trigger: str) -> None:
        self.execute("INSERT INTO pipeline_runs (run_id, started_at, status, trigger, stages) VALUES (?,?,?,?,?)",
                     [run_id, started_at, "running", trigger, "{}"])

    def update_run(self, run_id: str, **fields) -> None:
        if not fields:
            return
        if "stages" in fields and not isinstance(fields["stages"], str):
            fields["stages"] = json.dumps(fields["stages"], default=str)
        cols = ", ".join(f"{k} = ?" for k in fields)
        self.execute(f"UPDATE pipeline_runs SET {cols} WHERE run_id = ?", [*fields.values(), run_id])

    # ── chat telemetry ──────────────────────────────────────────────────────
    def log_query(self, question: str, intent: str, confidence: str, latency_ms: int, model: str | None,
                  used_lightrag: bool, rows_used: int) -> int:
        with self._lock:
            return self.conn.execute(
                """INSERT INTO query_log (question, intent, confidence, latency_ms, model, used_lightrag, rows_used)
                   VALUES (?,?,?,?,?,?,?) RETURNING id""",
                [question[:500], intent, confidence, latency_ms, model, used_lightrag, rows_used]).fetchone()[0]

    def set_feedback(self, query_id: int, value: int) -> None:
        self.execute("UPDATE query_log SET feedback = ? WHERE id = ?", [value, query_id])

    def close(self) -> None:
        with self._lock:
            self.conn.close()


_instances: dict[str, DBTool] = {}
_instances_lock = threading.Lock()


def get_db(path: str) -> DBTool:
    """One DBTool per database file for the life of the process."""
    key = os.path.abspath(path) if path != ":memory:" else path
    with _instances_lock:
        if key not in _instances:
            _instances[key] = DBTool(path)
        return _instances[key]
