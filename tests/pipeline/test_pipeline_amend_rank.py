"""Amendments, joint-filer dedup via CIKs, gate reconciliation and the full runner. Offline."""

from datetime import date, timedelta

from ingest_testkit import FakeSource, day_ts, make_filing, recent_weekdays
from insider_intel.pipeline.gate import run_all_checks
from insider_intel.pipeline.ingest import ingest, mark_superseded
from insider_intel.pipeline.rank import rank
from insider_intel.pipeline.run import PipelineRunner


def _gate(db, sec, rk, cfg):
    return run_all_checks(
        db,
        rk["window_start"],
        sec["accessions"],
        sec["expected_txns"],
        rk["ranked_df"],
        rk["checksum"],
        rk["stats"],
        cfg.top_n_trades,
    )


def _amend_source():
    """Original sale of 1000 @ 10, then a 4/A correcting it to 800 @ 10, then a second 4/A to 700 @ 10."""
    d1, d2, d3 = recent_weekdays(3)
    orig = make_filing("0000000001-26-000001", accepted=day_ts(d1), txns=[("S", 1000, 10.0, d1), ("P", 50, 9.0, d1)])
    am1 = make_filing("0000000001-26-000002", form="4/A", accepted=day_ts(d2), txns=[("S", 800, 10.0, d1)])
    am2 = make_filing("0000000001-26-000003", form="4/A", accepted=day_ts(d3), txns=[("S", 700, 10.0, d1)])
    return FakeSource([(d1, orig), (d2, am1), (d3, am2)])


def test_latest_4a_supersedes_earlier_versions(db, cfg):
    ingest(_amend_source(), db, cfg, today=date.today())
    active = db.query("""SELECT accession_number, code, shares FROM transactions WHERE NOT superseded
                         ORDER BY accession_number, row_idx""")
    # the S group is replaced by the latest 4/A; the P row of the original is a different group and stays
    assert active == [("0000000001-26-000001", "P", 50.0), ("0000000001-26-000003", "S", 700.0)]
    assert db.query("SELECT COUNT(*) FROM transactions")[0][0] == 4  # nothing is deleted
    assert db.query("SELECT COUNT(*) FROM transactions WHERE superseded")[0][0] == 2


def test_ranking_uses_latest_amendment_and_gate_reconciles(db, cfg):
    sec = ingest(_amend_source(), db, cfg, today=date.today())
    rk = rank(db, cfg)
    t = rk["top_trades"][0]
    assert t["sell_value"] == 7000.0 and t["buy_value"] == 450.0
    ok, results = _gate(db, sec, rk, cfg)
    assert ok, [(r.name, r.detail) for r in results if not r.passed]


def test_amendment_marking_is_idempotent_and_plain_later_form4_survives(db, cfg):
    d1, d2, d3 = recent_weekdays(3)
    a = make_filing("0000000001-26-000001", accepted=day_ts(d1), txns=[("S", 100, 10.0, d1)])
    am = make_filing("0000000001-26-000002", form="4/A", accepted=day_ts(d2), txns=[("S", 90, 10.0, d1)])
    later = make_filing(
        "0000000001-26-000003", accepted=day_ts(d3), txns=[("S", 40, 10.0, d1)]
    )  # plain 4 after the 4/A
    ingest(FakeSource([(d1, a), (d2, am), (d3, later)]), db, cfg, today=date.today())
    first = db.query("SELECT accession_number, superseded FROM transactions ORDER BY 1")
    assert first == [("0000000001-26-000001", True), ("0000000001-26-000002", False), ("0000000001-26-000003", False)]
    mark_superseded(db)
    assert db.query("SELECT accession_number, superseded FROM transactions ORDER BY 1") == first


def test_different_owner_set_is_not_an_amendment(db, cfg):
    d1, d2 = recent_weekdays(2)
    a = make_filing("0000000001-26-000001", accepted=day_ts(d1), txns=[("S", 100, 10.0, d1)])
    other = make_filing(
        "0000000001-26-000002", form="4/A", accepted=day_ts(d2), owners=(("777", "Zed Q"),), txns=[("S", 100, 10.0, d1)]
    )
    ingest(FakeSource([(d1, a), (d2, other)]), db, cfg, today=date.today())
    assert db.query("SELECT COUNT(*) FROM transactions WHERE superseded")[0][0] == 0


def test_joint_filer_dedup_keys_on_owner_ciks(db, cfg):
    d1 = recent_weekdays(1)[0]
    # same joint filing listed under two accessions (same owner-CIK set): counted once
    j1 = make_filing(
        "0000000001-26-000001",
        accepted=day_ts(d1),
        owners=(("10", "Fund LP"), ("11", "Fund GP")),
        txns=[("P", 1000, 10.0, d1)],
    )
    j2 = make_filing(
        "0000000001-26-000002",
        accepted=day_ts(d1),
        owners=(("11", "Fund GP"), ("10", "Fund LP")),
        txns=[("P", 1000, 10.0, d1)],
    )
    # identical trade by a DIFFERENT owner: a different key, kept
    other = make_filing(
        "0000000001-26-000003", accepted=day_ts(d1), owners=(("12", "Other Person"),), txns=[("P", 1000, 10.0, d1)]
    )
    sec = ingest(FakeSource([(d1, j1), (d1, j2), (d1, other)]), db, cfg, today=date.today())
    rk = rank(db, cfg)
    assert rk["stats"]["dropped_joint_dups"] == 1 and rk["top_trades"][0]["buy_value"] == 20_000.0
    ok, results = _gate(db, sec, rk, cfg)
    assert ok, [(r.name, r.detail) for r in results if not r.passed]


def test_rank_window_is_days_on_acceptance_time(db, cfg):
    d = recent_weekdays(1)[0]
    old = make_filing(
        "0000000001-26-000001",
        accepted=day_ts(d) - timedelta(days=20),
        txns=[("S", 1000, 10.0, d)],
        ticker="OLD",
        issuer_cik="2000",
    )
    new = make_filing(
        "0000000001-26-000002", accepted=day_ts(d), txns=[("P", 100, 10.0, d)], ticker="NEW", issuer_cik="3000"
    )
    ingest(FakeSource([(d, old), (d, new)]), db, cfg, today=date.today(), since=d)
    assert [t["issuer_ticker"] for t in rank(db, cfg)["top_trades"]] == ["NEW"]
    wide = cfg.model_copy(update={"rank_window_days": 30})
    assert {t["issuer_ticker"] for t in rank(db, wide)["top_trades"]} == {"NEW", "OLD"}


def test_runner_end_to_end_and_partial_status(db, fixture_source, cfg):
    runner = PipelineRunner(db, fixture_source, cfg, ingest_kwargs={"today": date.today()})
    out = runner.run()
    assert out["status"] == "success", out
    assert db.query("SELECT COUNT(*) FROM top_trades")[0][0] > 0
    assert db.query("SELECT filings_ingested FROM pipeline_runs")[0][0] == 24

    d = recent_weekdays(1)[0]
    flaky = FakeSource([(d, make_filing("0000000002-26-000001", accepted=day_ts(d), txns=[("P", 10, 5.0, d)]))])
    flaky.fail = {"0000000002-26-000001"}
    out2 = PipelineRunner(db, flaky, cfg, ingest_kwargs={"today": date.today(), "since": d}).run()
    assert out2["status"] == "partial"  # published, but flagged incomplete


def _bad_checksum_rank(db, cfg):
    res = rank(db, cfg)
    res["checksum"] = "f" * 64
    return res


def test_quarantine_status_survives_and_nothing_is_published(db, fixture_source, cfg):
    """Regression: the old code raised after quarantining and the generic handler overwrote the status."""
    runner = PipelineRunner(db, fixture_source, cfg, rank_fn=_bad_checksum_rank, ingest_kwargs={"today": date.today()})
    out = runner.run()
    assert out["status"] == "quarantined"
    assert db.query("SELECT status FROM pipeline_runs")[0][0] == "quarantined"
    assert db.query("SELECT COUNT(*) FROM top_trades")[0][0] == 0  # ranking was never published
    stages = runner.progress.snapshot()["stages"]
    assert stages["gate"]["status"] == "failed" and stages["publish"]["status"] == "pending"


def test_rerunning_does_not_duplicate_rankings(db, fixture_source, cfg):
    runner = PipelineRunner(db, fixture_source, cfg, ingest_kwargs={"today": date.today()})
    runner.run()
    n = db.query("SELECT COUNT(*) FROM top_trades")[0][0]
    assert n > 0 and runner.run()["status"] == "success"
    assert db.query("SELECT COUNT(*) FROM top_trades")[0][0] == n
