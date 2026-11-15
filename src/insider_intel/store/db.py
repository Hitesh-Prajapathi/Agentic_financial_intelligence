"""Thread-safe DuckDB access with numbered migrations and an optional read-only mode.

DuckDB allows a single read/write connection per process; Streamlit runs many threads
(UI, background pipeline, chat), so every call goes through one re-entrant lock.

Schema management: `migrations/NNN_name.sql` files are applied in order on open (read/write mode) and
recorded in `schema_version`. Migrations are additive and idempotent, so a v2 database (tables exist,
no `schema_version`) is upgraded in place without losing data.

Request path (UI / chat): `get_read_only_db(path)` opens DuckDB with `read_only=True`; any write raises
`ReadOnlyError`. DuckDB cannot hold a read-only and a read/write connection to one file in the same
process, so if this process already holds the read/write instance, the helper returns a read-only *view*
that shares its connection.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
_MIGRATION_RE = re.compile(r"^(\d{3})_(.+)\.sql$")
EDGAR_SOURCE = "edgar_form4"


class ReadOnlyError(RuntimeError):
    """A write was attempted through a read-only DBTool."""


class SchemaOutOfDateError(RuntimeError):
    """A read-only open found a database that is behind the code's migrations."""


def list_migrations() -> list[tuple[int, str, Path]]:
    out = []
    for p in sorted(MIGRATIONS_DIR.glob("*.sql")):
        m = _MIGRATION_RE.match(p.name)
        if m:
            out.append((int(m.group(1)), m.group(2), p))
    return out


def latest_version() -> int:
    return max((v for v, _, _ in list_migrations()), default=0)


def _applied_versions(conn) -> set[int]:
    has = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'schema_version'").fetchone()[
        0
    ]
    if not has:
        return set()
    return {r[0] for r in conn.execute("SELECT version FROM schema_version").fetchall()}


def _apply_pending(conn) -> list[int]:
    conn.execute("""CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER PRIMARY KEY, name TEXT, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    applied = _applied_versions(conn)
    done = []
    for version, name, path in list_migrations():
        if version in applied:
            continue
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(path.read_text())
            conn.execute("INSERT INTO schema_version (version, name) VALUES (?, ?)", [version, name])
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        logger.info("applied migration %03d_%s", version, name)
        done.append(version)
    return done


def migrate_file(path: str) -> list[int]:
    """Open `path` read/write, apply pending migrations, close. Returns the versions applied."""
    conn = duckdb.connect(path)
    try:
        return _apply_pending(conn)
    finally:
        conn.close()


class DBTool:
    def __init__(self, path: str, read_only: bool = False, *, migrate: bool = True):
        self.path = path
        self.read_only = read_only
        self._lock = threading.RLock()
        if read_only:
            if path == ":memory:" or not os.path.exists(path):
                raise FileNotFoundError(f"cannot open {path!r} read-only: database file does not exist")
            self.conn = duckdb.connect(path, read_only=True)
            behind = self.pending_migrations()
            if behind:
                have = self.schema_version()
                self.conn.close()
                raise SchemaOutOfDateError(
                    f"{path} is at schema version {have} but the code expects {latest_version()}; "
                    "run `python -m insider_intel migrate` first"
                )
        else:
            if path != ":memory:":
                os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            self.conn = duckdb.connect(path)
            if migrate:
                self.init_db()

    @classmethod
    def _readonly_view(cls, parent: DBTool) -> DBTool:
        """Read-only facade over an already-open read/write instance (same connection and lock)."""
        v = cls.__new__(cls)
        v.path, v.read_only, v.conn, v._lock = parent.path, True, parent.conn, parent._lock
        return v

    # ── core ────────────────────────────────────────────────────────────────
    def init_db(self) -> list[int]:
        """Apply pending migrations (no-op on a database that is up to date)."""
        self._check_writable()
        with self._lock:
            return _apply_pending(self.conn)

    def schema_version(self) -> int:
        with self._lock:
            return max(_applied_versions(self.conn), default=0)

    def pending_migrations(self) -> list[int]:
        with self._lock:
            applied = _applied_versions(self.conn)
        return [v for v, _, _ in list_migrations() if v not in applied]

    def _check_writable(self) -> None:
        if self.read_only:
            raise ReadOnlyError("database opened read-only")

    def query(self, sql: str, params: list | None = None) -> list[tuple]:
        with self._lock:
            return self.conn.execute(sql, params or []).fetchall()

    def execute(self, sql: str, params: list | None = None) -> None:
        self._check_writable()
        with self._lock:
            self.conn.execute(sql, params or [])

    def query_df(self, sql: str, params: list | None = None) -> pd.DataFrame:
        with self._lock:
            return self.conn.execute(sql, params or []).df()

    def executemany(self, sql: str, rows: list) -> None:
        self._check_writable()
        if not rows:
            return
        with self._lock:
            self.conn.executemany(sql, rows)

    @contextmanager
    def transaction(self):
        """Atomic multi-statement write; rolls back on any exception."""
        self._check_writable()
        with self._lock:
            self.conn.execute("BEGIN TRANSACTION")
            try:
                yield self
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def checkpoint(self) -> None:
        """Flush the WAL into the main file (needed before a file-level upload of the .duckdb)."""
        self._check_writable()
        with self._lock:
            self.conn.execute("CHECKPOINT")

    def data_as_of(self) -> datetime | None:
        """When the data was last refreshed: last good pipeline run, else the newest filing."""
        r = self.query("SELECT MAX(completed_at) FROM pipeline_runs WHERE status IN ('success','partial')")[0][0]
        return r or self.query("SELECT MAX(filing_date) FROM filings")[0][0]

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
                   insider_name, insider_title, is_officer, is_director, is_ten_pct, is_10b5_1, form_type,
                   accepted_at, owner_ciks, price_in_footnote, is_late, date_of_original_submission)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
            [
                (
                    f["accession_number"],
                    f.get("filing_date"),
                    f.get("issuer_name"),
                    f.get("issuer_ticker"),
                    f.get("issuer_cik"),
                    f.get("insider_name"),
                    f.get("insider_title"),
                    bool(f.get("is_officer")),
                    bool(f.get("is_director")),
                    bool(f.get("is_ten_pct")),
                    bool(f.get("is_10b5_1")),
                    f.get("form_type", "4"),
                    f.get("accepted_at"),
                    f.get("owner_ciks"),
                    bool(f.get("price_in_footnote")),
                    bool(f.get("is_late")),
                    f.get("date_of_original_submission"),
                )
                for f in filings
            ],
        )

    def insert_transactions(self, txns: list[dict]) -> None:
        self.executemany(
            """INSERT INTO transactions (accession_number, row_idx, transaction_date, security_title, code,
                   acquired_disposed, shares, price_per_share, dollar_value, ownership_nature, post_transaction_shares,
                   price_footnote_ids)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
            [
                (
                    t["accession_number"],
                    t["row_idx"],
                    t.get("transaction_date"),
                    t.get("security_title"),
                    t.get("code"),
                    t.get("acquired_disposed"),
                    t.get("shares"),
                    t.get("price_per_share"),
                    t.get("dollar_value"),
                    t.get("ownership_nature"),
                    t.get("post_transaction_shares"),
                    t.get("price_footnote_ids"),
                )
                for t in txns
            ],
        )

    def insert_reporting_owners(self, owners: list[dict]) -> None:
        self.executemany(
            """INSERT INTO reporting_owners (accession_number, owner_cik, owner_name, is_director, is_officer,
                   is_ten_pct, is_other, officer_title) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
            [
                (
                    o["accession_number"],
                    o["owner_cik"],
                    o.get("owner_name"),
                    bool(o.get("is_director")),
                    bool(o.get("is_officer")),
                    bool(o.get("is_ten_pct")),
                    bool(o.get("is_other")),
                    o.get("officer_title"),
                )
                for o in owners
            ],
        )

    def insert_footnotes(self, rows: list[dict]) -> None:
        self.executemany(
            "INSERT INTO footnotes (accession_number, footnote_id, text) VALUES (?,?,?) ON CONFLICT DO NOTHING",
            [(r["accession_number"], r["footnote_id"], r.get("text")) for r in rows],
        )

    # ── ingest watermark ────────────────────────────────────────────────────
    def get_watermark(self, source: str = EDGAR_SOURCE) -> date | None:
        rows = self.query("SELECT last_index_date FROM ingest_watermark WHERE source = ?", [source])
        return rows[0][0] if rows else None

    def set_watermark(self, d: date, source: str = EDGAR_SOURCE) -> None:
        self.execute(
            """INSERT INTO ingest_watermark (source, last_index_date, updated_at) VALUES (?,?,CURRENT_TIMESTAMP)
               ON CONFLICT (source) DO UPDATE SET last_index_date = EXCLUDED.last_index_date,
                                                  updated_at = EXCLUDED.updated_at""",
            [source, d],
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
                [
                    (
                        run_id,
                        run_date,
                        t["rank"],
                        t["issuer_ticker"],
                        t.get("issuer_name"),
                        t.get("insider_name"),
                        t.get("insider_title"),
                        t["net_dollar_value"],
                        t["buy_value"],
                        t["sell_value"],
                        t["transaction_count"],
                        t["insider_count"],
                        t.get("pct_holdings"),
                        t.get("planned_pct"),
                        t["dominant_direction"],
                        t.get("signal_tags", ""),
                        t.get("validation_checksum"),
                    )
                    for t in trades
                ],
            )

    def latest_top_trades(self, limit: int = 20) -> pd.DataFrame:
        return self.query_df(
            """SELECT * FROM top_trades
               WHERE run_date = (SELECT MAX(run_date) FROM top_trades)
               ORDER BY rank LIMIT ?""",
            [limit],
        )

    # ── tweets / sentiment ──────────────────────────────────────────────────
    def insert_tweets(self, tweets: list[dict]) -> int:
        before = self.query("SELECT COUNT(*) FROM tweets")[0][0]
        self.executemany(
            """INSERT INTO tweets (tweet_id, ticker, text, author_username, author_followers, created_at,
                   retweet_count, like_count, reply_count) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
            [
                (
                    t["tweet_id"],
                    t["ticker"],
                    t["text"],
                    t.get("author_username"),
                    t.get("author_followers"),
                    t.get("created_at"),
                    t.get("retweet_count", 0),
                    t.get("like_count", 0),
                    t.get("reply_count", 0),
                )
                for t in tweets
            ],
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
            [
                (
                    r["tweet_id"],
                    r["ticker"],
                    r["sentiment_label"],
                    r["confidence_score"],
                    r.get("model"),
                    bool(r.get("reviewed")),
                    now,
                )
                for r in rows
            ],
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
        self.execute(
            "INSERT INTO pipeline_runs (run_id, started_at, status, trigger, stages) VALUES (?,?,?,?,?)",
            [run_id, started_at, "running", trigger, "{}"],
        )

    def update_run(self, run_id: str, **fields) -> None:
        if not fields:
            return
        if "stages" in fields and not isinstance(fields["stages"], str):
            fields["stages"] = json.dumps(fields["stages"], default=str)
        cols = ", ".join(f"{k} = ?" for k in fields)
        self.execute(f"UPDATE pipeline_runs SET {cols} WHERE run_id = ?", [*fields.values(), run_id])

    # ── chat telemetry ──────────────────────────────────────────────────────
    def log_query(
        self,
        question: str,
        intent: str,
        confidence: str,
        latency_ms: int,
        model: str | None,
        rows_used: int,
        *,
        mode: str | None = None,
        tool_calls: int | None = None,
        unsupported_numbers: int | None = None,
        prompt_hash: str | None = None,
    ) -> int:
        self._check_writable()
        with self._lock:
            return self.conn.execute(
                """INSERT INTO query_log (question, intent, confidence, latency_ms, model, rows_used,
                                          mode, tool_calls, unsupported_numbers, prompt_hash)
                   VALUES (?,?,?,?,?,?,?,?,?,?) RETURNING id""",
                [
                    question[:500],
                    intent,
                    confidence,
                    latency_ms,
                    model,
                    rows_used,
                    mode,
                    tool_calls,
                    unsupported_numbers,
                    prompt_hash,
                ],
            ).fetchone()[0]

    def set_feedback(self, query_id: int, value: int) -> None:
        self.execute("UPDATE query_log SET feedback = ? WHERE id = ?", [value, query_id])

    def close(self) -> None:
        with self._lock:
            self.conn.close()


_instances: dict[str, DBTool] = {}
_ro_instances: dict[str, DBTool] = {}
_instances_lock = threading.Lock()


def _key(path: str) -> str:
    return os.path.abspath(path) if path != ":memory:" else path


def get_db(path: str) -> DBTool:
    """One read/write DBTool per database file for the life of the process (migrates on first open)."""
    key = _key(path)
    with _instances_lock:
        if key in _ro_instances:
            raise RuntimeError(f"{path} is already open read-only in this process; DuckDB cannot mix modes")
        if key not in _instances:
            _instances[key] = DBTool(path)
        return _instances[key]


def get_read_only_db(path: str, *, auto_migrate: bool = True) -> DBTool:
    """Read-only DBTool for the UI/chat request path.

    * Same process already holds the read/write instance -> a read-only view over its connection.
    * Otherwise opens with DuckDB `read_only=True`. If the file is behind the code's migrations and
      `auto_migrate` is set, it is migrated once (brief read/write open) before the read-only open.
    """
    key = _key(path)
    with _instances_lock:
        if key in _instances:
            return DBTool._readonly_view(_instances[key])
        if key in _ro_instances:
            return _ro_instances[key]
        if auto_migrate and os.path.exists(path):
            conn = duckdb.connect(path)
            try:
                if set(v for v, _, _ in list_migrations()) - _applied_versions(conn):
                    _apply_pending(conn)
            finally:
                conn.close()
        _ro_instances[key] = DBTool(path, read_only=True)
        return _ro_instances[key]
