from datetime import datetime, timedelta

from agents.ranking_agent import RankingAgent
from agents.sec_data_agent import SecDataAgent
from config.settings import settings
from skills.integrity_checks import run_all_checks
from tools.sec_api_tool import SecApiTool


class FakeSec(SecApiTool):
    def __init__(self, raw):
        super().__init__()
        self._raw = raw

    def fetch_recent_filings(self, *a, **k):
        return self._raw


def _setup(db, raw):
    now = datetime.utcnow()
    for r in raw:  # fixtures are older than 24h; move them into the window
        r["filedAt"] = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    sec = SecDataAgent(FakeSec(raw), db, settings).run()
    rk = RankingAgent(db, settings).run()
    return sec, rk


def _gate(db, sec, rk, **over):
    args = dict(accessions=sec["accessions"], expected=sec["expected_txns"], ranked=rk["ranked_df"], cs=rk["checksum"], stats=rk["stats"])
    args.update(over)
    return run_all_checks(db, rk["window_start"], args["accessions"], args["expected"], args["ranked"], args["cs"], args["stats"], settings.top_n_trades)


def test_clean_run_passes_gate(db, raw_filings):
    sec, rk = _setup(db, [dict(r) for r in raw_filings])
    ok, results = _gate(db, sec, rk)
    assert ok, [(r.name, r.detail) for r in results if not r.passed]


def test_ingest_is_idempotent(db, raw_filings):
    raw = [dict(r) for r in raw_filings]
    _setup(db, raw)
    again = SecDataAgent(FakeSec(raw), db, settings).run()
    assert again["filings_ingested"] == 0 and again["duplicates_skipped"] == len(raw)


def test_tampered_ranking_is_caught(db, raw_filings):
    sec, rk = _setup(db, [dict(r) for r in raw_filings])
    bad = rk["ranked_df"].copy()
    bad.loc[0, "net_dollar_value"] = bad.loc[0, "net_dollar_value"] * 2
    ok, results = _gate(db, sec, rk, ranked=bad)
    assert not ok and {"ranking_integrity", "checksum"} & {r.name for r in results if not r.passed}


def test_wrong_checksum_is_caught(db, raw_filings):
    sec, rk = _setup(db, [dict(r) for r in raw_filings])
    ok, results = _gate(db, sec, rk, cs="0" * 64)
    assert not ok and [r.name for r in results if not r.passed] == ["checksum"]


def test_missing_rows_are_caught(db, raw_filings):
    sec, rk = _setup(db, [dict(r) for r in raw_filings])
    db.execute("DELETE FROM transactions WHERE id = (SELECT MIN(id) FROM transactions)")  # silent data loss
    ok, results = _gate(db, sec, rk)
    assert not ok and "ingest_reconciliation" in [r.name for r in results if not r.passed]


def test_notional_drift_is_caught(db, raw_filings):
    sec, rk = _setup(db, [dict(r) for r in raw_filings])
    stats = dict(rk["stats"], notional_valid=rk["stats"]["notional_valid"] + 5_000)
    ok, results = _gate(db, sec, rk, stats=stats)
    assert not ok and "notional_reconciliation" in [r.name for r in results if not r.passed]
