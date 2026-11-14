"""Numbered migrations, v2 upgrade, read-only mode. Only temp databases; data/ is never touched."""
from pathlib import Path

import duckdb
import pytest

from insider_intel.store import db as dbmod
from insider_intel.store.db import (DBTool, ReadOnlyError, SchemaOutOfDateError, get_db, get_read_only_db,
                                    latest_version, migrate_file)

V2_SCHEMA = Path(__file__).resolve().parent.parent / "fixtures" / "v2_schema.sql"


def make_v2_db(path):
    """A database exactly as v2 left it (no schema_version, legacy columns/tables, a few rows)."""
    con = duckdb.connect(str(path))
    con.execute(V2_SCHEMA.read_text())
    con.execute("""INSERT INTO filings (accession_number, filing_date, issuer_ticker, issuer_cik, insider_name, form_type)
                   VALUES ('A-1', TIMESTAMP '2026-11-05 15:00:00', 'ACME', '123', 'Dana Whitfield', '4')""")
    con.execute("""INSERT INTO transactions (accession_number, row_idx, transaction_date, code, shares, price_per_share, dollar_value)
                   VALUES ('A-1', 0, DATE '2026-11-04', 'S', 100, 10, 1000)""")
    con.execute("INSERT INTO pipeline_runs (run_id, status, tweets_scraped) VALUES ('r1', 'success', 7)")
    con.execute("INSERT INTO query_log (question, used_lightrag) VALUES ('q', TRUE)")
    con.close()


def cols(db, table):
    return {r[0] for r in db.query("SELECT column_name FROM information_schema.columns WHERE table_name = ?", [table])}


def test_fresh_database_gets_all_migrations(tmp_path):
    d = DBTool(str(tmp_path / "x.duckdb"))
    assert d.schema_version() == latest_version() == 4 and d.pending_migrations() == []
    assert {"accepted_at", "owner_ciks", "price_in_footnote", "is_late", "date_of_original_submission"} <= cols(d, "filings")
    assert {"price_footnote_ids", "superseded"} <= cols(d, "transactions")
    tables = {r[0] for r in d.query("SELECT table_name FROM information_schema.tables")}
    assert {"reporting_owners", "footnotes", "ingest_watermark", "insider_edges", "schema_version"} <= tables
    d.close()


def test_v2_database_is_upgraded_in_place_without_data_loss(tmp_path):
    p = tmp_path / "live_copy.duckdb"
    make_v2_db(p)
    d = DBTool(str(p))
    assert d.schema_version() == 4
    assert d.query("SELECT accession_number, issuer_ticker FROM filings") == [("A-1", "ACME")]
    assert d.query("SELECT code, shares, superseded FROM transactions") == [("S", 100.0, False)]
    assert d.query("SELECT tweets_scraped FROM pipeline_runs") == [(7,)]          # legacy columns kept
    assert d.query("SELECT used_lightrag FROM query_log") == [(True,)]
    assert "scrape_log" in {r[0] for r in d.query("SELECT table_name FROM information_schema.tables")}
    d.close()
    d2 = DBTool(str(p))                                                           # reopen: nothing pending, still works
    assert d2.pending_migrations() == [] and d2.query("SELECT COUNT(*) FROM filings")[0][0] == 1
    d2.close()


def test_failed_migration_rolls_back_and_is_not_recorded(tmp_path, monkeypatch):
    mig = tmp_path / "migs"
    mig.mkdir()
    (mig / "001_a.sql").write_text("CREATE TABLE t1 (x INT);")
    (mig / "002_bad.sql").write_text("CREATE TABLE t2 (x INT); SELECT * FROM does_not_exist;")
    monkeypatch.setattr(dbmod, "MIGRATIONS_DIR", mig)
    with pytest.raises(Exception):
        DBTool(str(tmp_path / "y.duckdb"))
    con = duckdb.connect(str(tmp_path / "y.duckdb"))
    assert con.execute("SELECT version FROM schema_version").fetchall() == [(1,)]
    assert con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 't2'").fetchone()[0] == 0


def test_read_only_blocks_writes(tmp_path):
    p = str(tmp_path / "z.duckdb")
    DBTool(p).close()
    ro = DBTool(p, read_only=True)
    assert ro.query("SELECT COUNT(*) FROM filings") == [(0,)]
    for fn in (lambda: ro.execute("DELETE FROM filings"), lambda: ro.insert_filings([{"accession_number": "x"}]),
               lambda: ro.log_query("q", "chat", "high", 1, None, 0), lambda: ro.init_db(), lambda: ro.checkpoint()):
        with pytest.raises(ReadOnlyError):
            fn()
    ro.close()


def test_read_only_open_of_stale_or_missing_database(tmp_path):
    with pytest.raises(FileNotFoundError):
        DBTool(str(tmp_path / "nope.duckdb"), read_only=True)
    p = tmp_path / "old.duckdb"
    make_v2_db(p)
    with pytest.raises(SchemaOutOfDateError, match="migrate"):
        DBTool(str(p), read_only=True)
    assert migrate_file(str(p)) == [1, 2, 3, 4]
    DBTool(str(p), read_only=True).close()


def test_get_read_only_db_auto_migrates_then_opens_read_only(tmp_path):
    p = tmp_path / "stale.duckdb"
    make_v2_db(p)
    ro = get_read_only_db(str(p))
    assert ro.read_only and ro.schema_version() == 4 and ro.query("SELECT COUNT(*) FROM filings") == [(1,)]
    with pytest.raises(ReadOnlyError):
        ro.execute("DELETE FROM filings")
    assert get_read_only_db(str(p)) is ro
    with pytest.raises(RuntimeError, match="read-only"):
        get_db(str(p))                    # DuckDB cannot mix modes in one process


def test_read_only_view_shares_an_open_read_write_instance(tmp_path):
    p = str(tmp_path / "rw.duckdb")
    rw = get_db(p)
    rw.execute("INSERT INTO ingest_watermark (source, last_index_date) VALUES ('s', DATE '2026-01-02')")
    ro = get_read_only_db(p)
    assert ro.read_only and ro.query("SELECT COUNT(*) FROM ingest_watermark") == [(1,)]
    with pytest.raises(ReadOnlyError):
        ro.execute("DELETE FROM ingest_watermark")


def test_migration_004_fresh_and_upgraded(tmp_path):
    fresh = DBTool(str(tmp_path / "f.duckdb"))
    up = tmp_path / "u.duckdb"
    make_v2_db(up)
    upgraded = DBTool(str(up))
    for d in (fresh, upgraded):
        assert {"id", "ts", "run_id", "session_id", "purpose", "provider", "model", "prompt_hash", "status", "latency_ms",
                "prompt_tokens", "completion_tokens", "n_tool_calls", "est_cost_usd"} == cols(d, "llm_calls")
        assert {"mode", "tool_calls", "unsupported_numbers", "prompt_hash"} <= cols(d, "query_log")
        qid = d.log_query("q", "chat", "high", 5, "m", 1, mode="agent", tool_calls=2, unsupported_numbers=1, prompt_hash="abc")
        assert d.query("SELECT mode, tool_calls, unsupported_numbers, prompt_hash FROM query_log WHERE id = ?", [qid]) == [("agent", 2, 1, "abc")]
        d.close()
