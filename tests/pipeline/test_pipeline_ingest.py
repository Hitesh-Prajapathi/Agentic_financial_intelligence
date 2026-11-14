"""EDGAR ingestion: mapping, watermark, resumability, caps, idempotency. Fully offline (FakeSource)."""
from datetime import date, datetime, timedelta

import pytest

from insider_intel.edgar.client import EdgarConfigError
from insider_intel.pipeline.ingest import EdgarSource, filing_to_db_rows, ingest
from insider_intel.pipeline.rank import rank
from insider_intel.store.db import DBTool
from ingest_testkit import FakeSource, day_ts, make_entry, make_filing, recent_weekdays


def derived(db):
    """Derived tables with run-specific / surrogate columns excluded."""
    edges = db.query("SELECT * FROM insider_edges ORDER BY owner_cik, issuer_cik")
    txn = db.query("""SELECT accession_number, row_idx, code, shares, price_per_share, dollar_value, superseded
                      FROM transactions ORDER BY accession_number, row_idx""")
    fil = db.query("SELECT accession_number, filing_date, accepted_at, owner_ciks, form_type FROM filings ORDER BY 1")
    own = db.query("SELECT * FROM reporting_owners ORDER BY 1, 2")
    return edges, txn, fil, own


def test_ingest_maps_all_codes_and_owners(db, fixture_source, cfg):
    today = date.today()
    res = ingest(fixture_source, db, cfg, today=today)
    assert res["filings_ingested"] == 24 and res["parse_errors"] == 0
    # every transaction code is stored, not just P/S
    codes = {r[0] for r in db.query("SELECT DISTINCT code FROM transactions")}
    assert {"P", "S"} <= codes and codes - {"P", "S"}
    # reconciliation inputs: every stored row accounted for
    assert res["expected_txns"] == db.query("SELECT COUNT(*) FROM transactions")[0][0]
    # filing_date mirrors accepted_at; owner CIKs recorded for joint filers
    assert db.query("SELECT COUNT(*) FROM filings WHERE filing_date IS NULL OR accepted_at IS NULL OR filing_date <> accepted_at")[0][0] == 0
    assert db.query("SELECT COUNT(*) FROM filings WHERE owner_ciks LIKE '%|%'")[0][0] >= 1       # synthetic joint filing
    assert db.query("SELECT COUNT(*) FROM reporting_owners")[0][0] > db.query("SELECT COUNT(*) FROM filings")[0][0] - 1
    assert db.query("SELECT COUNT(*) FROM filings WHERE form_type = '4/A'")[0][0] == 1
    assert db.query("SELECT COUNT(*) FROM filings WHERE date_of_original_submission IS NOT NULL")[0][0] == 1
    assert db.query("SELECT COUNT(*) FROM filings WHERE price_in_footnote")[0][0] >= 1
    assert db.query("SELECT COUNT(*) FROM filings WHERE is_10b5_1")[0][0] >= 1
    assert db.query("SELECT COUNT(*) FROM transactions WHERE price_footnote_ids IS NOT NULL")[0][0] >= 1
    assert db.query("SELECT COUNT(*) FROM transactions WHERE ownership_nature = 'I'")[0][0] >= 1
    assert db.query("SELECT COUNT(*) FROM footnotes")[0][0] > 0
    assert db.query("SELECT COUNT(*) FROM insider_edges")[0][0] > 0


def test_price_less_rows_stored_but_not_ranked(db, cfg):
    d = recent_weekdays(1)[0]
    f = make_filing("0000000001-26-000001", accepted=day_ts(d), txns=[("P", 100, None, d), ("P", 100, 10.0, d), ("A", 50, 0.0, d)])
    ingest(FakeSource([(d, f)]), db, cfg, today=date.today())
    assert db.query("SELECT COUNT(*) FROM transactions")[0][0] == 3
    assert db.query("SELECT COUNT(*) FROM transactions WHERE dollar_value IS NULL")[0][0] == 2
    r = rank(db, cfg)
    assert r["top_trades"][0]["buy_value"] == 1000.0 and r["stats"]["rows_in"] == 2 and r["stats"]["dropped_invalid"] == 1


def test_watermark_advances_over_complete_past_days_only(db, fixture_source, cfg):
    today = date.today()
    d1, d2, d3 = fixture_source.days
    ingest(fixture_source, db, cfg, today=today)
    assert db.get_watermark() == d3                      # last complete weekday strictly before today
    fixture_source.index_calls.clear()
    res = ingest(fixture_source, db, cfg, today=today)
    assert res["filings_ingested"] == 0
    assert all(d > d3 for d in fixture_source.index_calls)  # resumes after the watermark, no re-listing of old days


def test_first_run_starts_initial_lookback_days_back(db, cfg):
    src = FakeSource([])
    today = date(2026, 11, 13)  # Friday
    res = ingest(src, db, cfg, today=today)
    assert res["start_date"] == (today - timedelta(days=cfg.edgar_initial_lookback_days)).isoformat()
    assert min(src.index_calls) >= today - timedelta(days=14) and all(d.weekday() < 5 for d in src.index_calls)


def test_empty_yesterday_index_does_not_advance_watermark(db, cfg):
    today = date(2026, 11, 13)  # Friday
    ingest(FakeSource([]), db, cfg, today=today)
    # yesterday (Thu 12th) had no index: could be "not published yet", so the watermark stops before it
    assert db.get_watermark() == date(2026, 11, 11)


def test_rerun_same_window_gives_identical_derived_tables(fixture_source, cfg):
    a, b = DBTool(":memory:"), DBTool(":memory:")
    ingest(fixture_source, a, cfg, today=date.today())
    ingest(fixture_source, b, cfg, today=date.today())
    ingest(fixture_source, b, cfg, today=date.today())                       # second run over the same data
    ingest(fixture_source, b, cfg, today=date.today(), since=fixture_source.days[0])  # forced re-scan of the whole window
    assert derived(a) == derived(b)
    ra, rb = rank(a, cfg), rank(b, cfg)
    assert ra["checksum"] == rb["checksum"] and ra["top_trades"] == rb["top_trades"]
    a.close(), b.close()


def test_transient_failure_blocks_watermark_and_resumes(db, cfg):
    d1, d2, d3 = recent_weekdays(3)
    f1 = make_filing("0000000001-26-000001", accepted=day_ts(d1), txns=[("P", 100, 10.0, d1)])
    f2 = make_filing("0000000001-26-000002", accepted=day_ts(d2), txns=[("P", 100, 10.0, d2)], owners=(("501", "Bo Ray"),))
    f3 = make_filing("0000000001-26-000003", accepted=day_ts(d3), txns=[("P", 100, 11.0, d3)], owners=(("502", "Cy Poe"),))
    src = FakeSource([(d1, f1), (d2, f2), (d3, f3)])
    src.fail = {f2.accession_number}
    res = ingest(src, db, cfg, today=date.today())
    assert res["fetch_errors"] == 1 and db.get_watermark() == d1       # day 2 incomplete: watermark stops at day 1
    assert db.query("SELECT COUNT(*) FROM filings")[0][0] == 2           # day 3 still stored (not lost)
    src.fail.clear()
    res = ingest(src, db, cfg, today=date.today())
    assert res["filings_ingested"] == 1 and db.get_watermark() == d3     # the missing filing is picked up, days 2-3 complete


def test_filing_cap_stops_cleanly_and_next_run_resumes(db, cfg):
    d1, d2 = recent_weekdays(2)
    fs = [make_filing(f"0000000001-26-00000{i}", accepted=day_ts(d1 if i < 3 else d2), txns=[("P", 100, 10.0 + i, d1)],
                      owners=((str(500 + i), f"Owner {i}"),)) for i in range(1, 5)]
    src = FakeSource([(d1 if i < 3 else d2, f) for i, f in enumerate(fs, start=1)])
    res = ingest(src, db, cfg, today=date.today(), max_filings=3)
    assert res["stopped_early"] == "filing cap reached" and res["filings_ingested"] == 3
    assert db.get_watermark() == d1                                       # day 1 (2 filings) complete, day 2 partial
    res = ingest(src, db, cfg, today=date.today(), max_filings=3)
    assert res["filings_ingested"] == 1 and db.get_watermark() == d2 and not res["stopped_early"]


def test_time_budget_stops_cleanly(db, cfg):
    d1 = recent_weekdays(1)[0]
    f = make_filing("0000000001-26-000001", accepted=day_ts(d1), txns=[("P", 100, 10.0, d1)])
    cfg2 = cfg.model_copy(update={"edgar_max_runtime_minutes": 1})
    ticks = iter([0.0, 1000.0, 1000.0, 1000.0])
    res = ingest(FakeSource([(d1, f)]), db, cfg2, today=date.today(), clock=lambda: next(ticks))
    assert res["stopped_early"] == "time budget reached" and res["filings_ingested"] == 0
    assert db.get_watermark() < d1  # the interrupted day is not marked complete


def test_unlimited_by_default_for_scheduled_runs(db, cfg):
    d1, d2 = recent_weekdays(2)
    src = FakeSource([(d1, make_filing(f"0000000001-26-{i:06d}", accepted=day_ts(d1), owners=((str(i), f"O {i}"),),
                                       txns=[("P", 10, 5.0, d1)])) for i in range(1, 60)])
    ingest(src, db, cfg, today=date.today())
    res = ingest(src, db, cfg, today=date.today(), since=d1)
    assert db.query("SELECT COUNT(*) FROM filings")[0][0] == 59 and res["stopped_early"] is None


def test_since_overrides_watermark_but_never_moves_it_back(db, cfg):
    d1, d2, d3 = recent_weekdays(3)
    src = FakeSource([(d, make_filing(f"0000000001-26-00000{i}", accepted=day_ts(d), txns=[("P", 10, 5.0, d)],
                                      owners=((str(500 + i), f"O {i}"),))) for i, d in enumerate((d1, d2, d3), 1)])
    ingest(src, db, cfg, today=date.today())
    assert db.get_watermark() == d3
    ingest(src, db, cfg, today=date.today(), since=d1)
    assert db.get_watermark() == d3


def test_parse_failures_do_not_block_the_watermark(db, cfg):
    from insider_intel.edgar.form4 import Form4ParseError

    d1 = recent_weekdays(1)[0]
    good = make_filing("0000000001-26-000001", accepted=day_ts(d1), txns=[("P", 10, 5.0, d1)])
    bad = make_filing("0000000001-26-000002", accepted=day_ts(d1))

    class Src(FakeSource):
        def load(self, entry):
            if entry.accession == bad.accession_number:
                raise Form4ParseError("garbage")
            return super().load(entry)

    res = ingest(Src([(d1, good), (d1, bad)]), db, cfg, today=date.today())
    assert res["parse_errors"] == 1 and res["filings_ingested"] == 1 and db.get_watermark() == d1


def test_late_flag_and_fallback_filing_date(db):
    d = date(2026, 11, 2)  # Monday
    f = make_filing("0000000001-26-000009", accepted=datetime(2026, 11, 9, 15), txns=[("P", 1, 1.0, d)])
    row = filing_to_db_rows(f)["filing"]
    assert row["filing_date"] == datetime(2026, 11, 9, 15) and row["is_late"]
    g = make_filing("0000000001-26-000010", accepted=None, txns=[("P", 1, 1.0, d)])
    row = filing_to_db_rows(g, make_entry(g, date(2026, 11, 3)))["filing"]
    assert row["filing_date"] == datetime(2026, 11, 3) and row["accepted_at"] is None


def test_edgar_source_is_lazy_and_requires_user_agent(cfg):
    from insider_intel.config import Settings

    src = EdgarSource(Settings(_env_file=None, sec_user_agent=""))          # constructing never raises
    with pytest.raises(EdgarConfigError, match="SEC_USER_AGENT"):
        src.client
    placeholder = EdgarSource(Settings(_env_file=None, sec_user_agent="your-app-name your-email@example.com"))
    with pytest.raises(EdgarConfigError):
        placeholder.client
