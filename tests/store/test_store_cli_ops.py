"""CLI: publish-snapshot, freshness, run --since / exit codes. Offline, temp databases."""
import json
from datetime import date, datetime

import duckdb
import pytest
from ingest_testkit import FakeSource, day_ts, make_filing, recent_weekdays

from insider_intel import cli
from insider_intel.config import settings
from insider_intel.pipeline.ingest import ingest
from insider_intel.pipeline.run import PipelineRunner
from insider_intel.store.db import DBTool
from insider_intel.store.snapshot import check_freshness, publish_snapshot, weekdays_between

NOW = datetime(2026, 11, 13, 12)  # Friday


def seeded_db(path, cfg, accepted=datetime(2026, 11, 12, 15), status=None):
    d = DBTool(str(path))
    d0 = accepted.date()
    f = make_filing("0000000001-26-000001", accepted=accepted, txns=[("P", 100, 10.0, d0)])
    ingest(FakeSource([(d0, f)]), d, cfg, today=date(2026, 11, 13), since=d0)
    if status:
        d.start_run("r1", accepted, "manual")
        d.update_run("r1", status=status, completed_at=accepted)
    d.close()
    return str(path)


def test_publish_snapshot_exports_parquet_and_manifest(tmp_path, cfg):
    p = seeded_db(tmp_path / "live.duckdb", cfg, status="success")
    out = tmp_path / "snap"
    m = publish_snapshot(p, str(out), now=NOW)
    assert m["schema_version"] == 4 and m["generated_at"] == "2026-11-13T12:00:00Z"
    assert m["tables"]["filings"] == 1 and m["tables"]["transactions"] == 1 and m["tables"]["insider_edges"] == 1
    assert "sentiment_summary" in m["tables"] and "tweets" not in m["tables"]
    assert json.loads((out / "manifest.json").read_text()) == m
    con = duckdb.connect()
    assert con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}/filings.parquet')").fetchone()[0] == 1
    assert con.execute(f"SELECT owner_cik FROM read_parquet('{out}/insider_edges.parquet')").fetchone()[0] == "500"


def test_publish_snapshot_skips_missing_tables_on_old_db(tmp_path):
    con = duckdb.connect(str(tmp_path / "v.duckdb"))
    con.execute("CREATE TABLE filings (accession_number TEXT, filing_date TIMESTAMP)")
    con.close()
    m = publish_snapshot(str(tmp_path / "v.duckdb"), str(tmp_path / "o"))
    assert m["tables"] == {"filings": 0} and m["schema_version"] == 0


def test_weekdays_between():
    assert weekdays_between(datetime(2026, 11, 13, 20), datetime(2026, 11, 16, 9)) == 1     # Fri -> Mon
    assert weekdays_between(datetime(2026, 11, 9), datetime(2026, 11, 13)) == 4


def test_freshness_ok_stale_and_failed_run(tmp_path, cfg):
    fresh = seeded_db(tmp_path / "a.duckdb", cfg, status="success")
    assert check_freshness(fresh, 5, now=NOW) == []
    stale = seeded_db(tmp_path / "b.duckdb", cfg, accepted=datetime(2026, 10, 20, 15))
    problems = check_freshness(stale, 5, now=NOW)
    assert len(problems) == 1 and "business days old" in problems[0]
    failed = seeded_db(tmp_path / "c.duckdb", cfg, status="quarantined")
    assert "quarantined" in check_freshness(failed, 5, now=NOW)[0]
    assert "not found" in check_freshness(str(tmp_path / "missing.duckdb"), 5)[0]


def test_freshness_tolerates_unmigrated_snapshot(tmp_path):
    con = duckdb.connect(str(tmp_path / "old.duckdb"))
    con.execute("CREATE TABLE filings (accession_number TEXT, filing_date TIMESTAMP)")
    con.execute("INSERT INTO filings VALUES ('a', TIMESTAMP '2026-11-12 10:00:00')")
    con.close()
    assert check_freshness(str(tmp_path / "old.duckdb"), 5, now=NOW) == []
    assert check_freshness(str(tmp_path / "old.duckdb"), 0, now=NOW) != []


def test_cli_commands_and_exit_codes(tmp_path, cfg, capsys):
    p = seeded_db(tmp_path / "live.duckdb", cfg, status="success")
    assert cli.main(["freshness", "--db", p, "--max-business-days", "1000"]) == 0
    assert capsys.readouterr().out.strip() == "OK"
    assert cli.main(["freshness", "--db", p, "--max-business-days", "0"]) == 1          # fixture data is in the past
    assert capsys.readouterr().out.startswith("STALE:")
    assert cli.main(["publish-snapshot", "--db", p, "--out", str(tmp_path / "s")]) == 0
    assert (tmp_path / "s" / "manifest.json").exists() and (tmp_path / "s" / "top_trades.parquet").exists()
    # default --max-business-days is 5
    assert cli.main(["freshness", "--db", p]) == (0 if weekdays_between(datetime(2026, 11, 12, 15), datetime.utcnow()) <= 5 else 1)


class _Sys:
    def __init__(self, db, runner, source):
        self.db, self.pipeline, self.sec = db, runner, source


def _patch_system(monkeypatch, tmp_path, cfg, ingest_fn=None, rank_fn=None):
    db = DBTool(str(tmp_path / "run.duckdb"))
    src = FakeSource([])
    kw = {}
    if ingest_fn:
        kw["ingest_fn"] = ingest_fn
    if rank_fn:
        kw["rank_fn"] = rank_fn
    runner = PipelineRunner(db, src, cfg, **kw)
    monkeypatch.setattr("insider_intel.system.build_system", lambda *a, **k: _Sys(db, runner, src))
    monkeypatch.setattr(settings, "sec_user_agent", "unit-test-agent contact@test.invalid")
    return db, runner


def last_status(path):
    con = duckdb.connect(str(path), read_only=True)  # the CLI closes its connection when done
    try:
        return con.execute("SELECT status FROM pipeline_runs ORDER BY started_at DESC").fetchone()[0]
    finally:
        con.close()


def _fake_ingest(status_extra=None, seen=None):
    def fn(source, db, cfg, **kwargs):
        if seen is not None:
            seen.update(kwargs)
        res = {"filings_ingested": 0, "accessions": [], "expected_txns": 0}
        res.update(status_extra or {})
        return res
    return fn


def test_run_exit_codes_and_since_flag(monkeypatch, tmp_path, cfg):
    seen = {}
    _patch_system(monkeypatch, tmp_path, cfg, ingest_fn=_fake_ingest(seen=seen))
    assert cli.main(["run", "--since", "2026-11-01", "--max-filings", "50"]) == 0
    assert seen == {"since": date(2026, 11, 1), "max_filings": 50}
    assert last_status(tmp_path / "run.duckdb") == "success"


def test_run_partial_exits_zero_and_failed_or_quarantined_exit_nonzero(monkeypatch, tmp_path, cfg):
    _patch_system(monkeypatch, tmp_path, cfg, ingest_fn=_fake_ingest({"stopped_early": "filing cap reached"}))
    assert cli.main(["run"]) == 0

    def boom(*a, **k):
        raise RuntimeError("EDGAR down")
    _patch_system(monkeypatch, tmp_path / "f", cfg, ingest_fn=boom)
    assert cli.main(["run"]) == 1

    def bad_rank(db, c):
        from insider_intel.pipeline.rank import rank
        r = rank(db, c)
        r["checksum"] = "f" * 64
        return r
    # non-empty ranking so the corrupted checksum is actually verified
    d = recent_weekdays(1)[0]
    db, runner = _patch_system(monkeypatch, tmp_path / "q", cfg, rank_fn=bad_rank)
    runner.sec_tool.add(d, make_filing("0000000001-26-000001", accepted=day_ts(d), txns=[("P", 100, 10.0, d)]))
    runner.ingest_kwargs = {"today": date.today()}
    assert cli.main(["run"]) == 1
    assert last_status(tmp_path / "q" / "run.duckdb") == "quarantined"


def test_run_without_user_agent_fails_before_touching_anything(monkeypatch, capsys):
    monkeypatch.setattr(settings, "sec_user_agent", "")
    assert cli.main(["run"]) == 2
    assert "SEC_USER_AGENT" in capsys.readouterr().err


def test_bad_since_date_is_rejected():
    with pytest.raises(SystemExit):
        cli.main(["run", "--since", "yesterday"])
