"""Governance gate over EDGAR-ingested data (offline)."""
from datetime import date

import pytest

from insider_intel.pipeline.gate import run_all_checks
from insider_intel.pipeline.ingest import ingest
from insider_intel.pipeline.rank import rank


@pytest.fixture
def run(db, fixture_source, cfg):
    sec = ingest(fixture_source, db, cfg, today=date.today())
    return sec, rank(db, cfg), cfg


def _gate(db, sec, rk, cfg, **over):
    args = dict(accessions=sec["accessions"], expected=sec["expected_txns"], ranked=rk["ranked_df"], cs=rk["checksum"], stats=rk["stats"])
    args.update(over)
    return run_all_checks(db, rk["window_start"], args["accessions"], args["expected"], args["ranked"], args["cs"], args["stats"], cfg.top_n_trades)


def test_clean_run_passes_gate(db, run):
    sec, rk, cfg = run
    assert not rk["ranked_df"].empty
    ok, results = _gate(db, sec, rk, cfg)
    assert ok, [(r.name, r.detail) for r in results if not r.passed]


def test_ingest_is_idempotent(db, fixture_source, cfg):
    ingest(fixture_source, db, cfg, today=date.today())
    again = ingest(fixture_source, db, cfg, today=date.today(), since=fixture_source.days[0])
    assert again["filings_ingested"] == 0 and again["duplicates_skipped"] == 24


def test_non_signal_codes_are_stored_but_do_not_break_reconciliation(db, run):
    sec, rk, cfg = run
    assert db.query("SELECT COUNT(*) FROM transactions WHERE code NOT IN ('P','S')")[0][0] > 0
    assert sec["expected_txns"] == db.query("SELECT COUNT(*) FROM transactions")[0][0]


def test_tampered_ranking_is_caught(db, run):
    sec, rk, cfg = run
    bad = rk["ranked_df"].copy()
    bad.loc[0, "net_dollar_value"] = bad.loc[0, "net_dollar_value"] * 2
    ok, results = _gate(db, sec, rk, cfg, ranked=bad)
    assert not ok and {"ranking_integrity", "checksum"} & {r.name for r in results if not r.passed}


def test_wrong_checksum_is_caught(db, run):
    sec, rk, cfg = run
    ok, results = _gate(db, sec, rk, cfg, cs="0" * 64)
    assert not ok and [r.name for r in results if not r.passed] == ["checksum"]


def test_missing_rows_are_caught(db, run):
    sec, rk, cfg = run
    db.execute("DELETE FROM transactions WHERE id = (SELECT MIN(id) FROM transactions)")  # silent data loss
    ok, results = _gate(db, sec, rk, cfg)
    assert not ok and "ingest_reconciliation" in [r.name for r in results if not r.passed]


def test_notional_drift_is_caught(db, run):
    sec, rk, cfg = run
    stats = dict(rk["stats"], notional_valid=rk["stats"]["notional_valid"] + 5_000)
    ok, results = _gate(db, sec, rk, cfg, stats=stats)
    assert not ok and "notional_reconciliation" in [r.name for r in results if not r.passed]
