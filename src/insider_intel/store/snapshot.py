"""Operational exports and checks over a DuckDB file (used by the CLI and the GitHub workflows).

Both functions open the database read-only with plain DuckDB, so they work on a snapshot that is behind the
code's migrations (no migration, no write, no WAL left behind).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

SNAPSHOT_TABLES = ["top_trades", "insider_edges", "pipeline_runs", "filings", "transactions", "reporting_owners",
                   "sentiment_summary"]


def _tables(con) -> set[str]:
    return {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}


def _schema_version(con, tables: set[str]) -> int:
    if "schema_version" not in tables:
        return 0
    return con.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version").fetchone()[0]


def publish_snapshot(db_path: str, out_dir: str, now: datetime | None = None) -> dict:
    """Write each existing derived/read-side table to `<out_dir>/<table>.parquet` plus `manifest.json`.

    Rows are exported in a deterministic order (`ORDER BY ALL`). Missing tables are skipped.
    Returns the manifest dict: {tables: {name: rows}, schema_version, generated_at}.
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"database not found: {db_path}")
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path, read_only=True)
    try:
        have = _tables(con)
        counts: dict[str, int] = {}
        for t in SNAPSHOT_TABLES:
            if t not in have:
                continue
            target = str(out / f"{t}.parquet").replace("'", "''")
            con.execute(f"COPY (SELECT * FROM {t} ORDER BY ALL) TO '{target}' (FORMAT parquet)")
            counts[t] = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        manifest = {
            "tables": counts,
            "schema_version": _schema_version(con, have),
            "generated_at": (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    finally:
        con.close()
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def weekdays_between(newest: datetime, now: datetime) -> int:
    """Weekdays strictly after `newest`'s date up to and including `now`'s date (no holiday calendar)."""
    d, n = newest.date(), 0
    while d < now.date():
        d += timedelta(days=1)
        n += d.weekday() < 5
    return n


def check_freshness(db_path: str, max_business_days: int = 5, now: datetime | None = None) -> list[str]:
    """Problems that breach the freshness SLO (empty list = fresh).

    * newest filing time (`accepted_at`, falling back to `filing_date` on an un-migrated snapshot) is more
      than `max_business_days` weekdays old
    * the most recent pipeline run ended `failed` or `quarantined`
    """
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    if not os.path.exists(db_path):
        return [f"database not found: {db_path}"]
    con = duckdb.connect(db_path, read_only=True)
    problems: list[str] = []
    try:
        have = _tables(con)
        if "filings" not in have:
            return ["no filings table in snapshot"]
        cols = {r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'filings'").fetchall()}
        col = "accepted_at" if "accepted_at" in cols else "filing_date"
        newest = con.execute(f"SELECT MAX(COALESCE({col}, filing_date)) FROM filings").fetchone()[0]
        if newest is None:
            problems.append("no filings in snapshot")
        else:
            n = weekdays_between(newest, now)
            if n > max_business_days:
                problems.append(f"newest {col} is {newest:%Y-%m-%d %H:%M} UTC, {n} business days old (limit {max_business_days})")
        if "pipeline_runs" in have:
            row = con.execute("SELECT status, started_at FROM pipeline_runs ORDER BY started_at DESC LIMIT 1").fetchone()
            if row and row[0] in ("failed", "quarantined"):
                problems.append(f"last pipeline run {row[1]:%Y-%m-%d %H:%M} UTC ended with status {row[0]}")
    finally:
        con.close()
    return problems
