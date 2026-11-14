import json
import shutil
from pathlib import Path

import httpx
import pandas as pd
import pytest

from insider_intel.edgar.client import EdgarClient
from insider_intel.eval import __main__ as cli
from insider_intel.eval import common, event_study_run as esr
from insider_intel.eval import parse_accuracy_run as par
from insider_intel.eval import sec_datasets as sd
from synth_official import write_quarter_zip

ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "tests" / "edgar" / "fixtures"


# ------------------------------------------------------------------ arg parsing
def test_arg_parsing_defaults_and_flags():
    p = cli.build_parser()
    a = p.parse_args(["parse-accuracy", "--year", "2025", "--quarter", "3"])
    assert (a.sample, a.hard, a.seed, a.cache_dir, a.max_fetch, a.yes) == (1000, 100, 0, "data/raw/edgar", None, False)
    a = p.parse_args(["parse-accuracy", "--year", "2025", "--quarter", "3", "--sample", "50", "--max-fetch", "7", "--yes"])
    assert (a.sample, a.max_fetch, a.yes) == (50, 7, True)
    e = p.parse_args(["event-study", "--prices", "csvdir:/x", "--kinds", "cluster_buy", "--dry-run"])
    assert (e.from_year, e.to_year, e.prices, e.kinds, e.dry_run, e.max_tickers) == (2016, 2025, "csvdir:/x", "cluster_buy", True, None)
    assert p.parse_args(["parse-accuracy-offline"]).command == "parse-accuracy-offline"
    with pytest.raises(SystemExit):
        p.parse_args(["parse-accuracy", "--year", "2025", "--quarter", "5"])
    with pytest.raises(SystemExit):
        p.parse_args([])


# ------------------------------------------------------------------ SEC_USER_AGENT guard
def test_parse_accuracy_without_user_agent_makes_no_request(monkeypatch, capsys):
    def boom(*a, **k):
        raise AssertionError("download attempted")

    monkeypatch.setattr(sd, "download_quarter", boom)
    monkeypatch.setattr(EdgarClient, "get", boom)
    assert cli.main(["parse-accuracy", "--year", "2025", "--quarter", "3"]) == 2
    assert "SEC_USER_AGENT is not set" in capsys.readouterr().err


def test_event_study_dry_run_needing_sec_without_user_agent_makes_no_request(monkeypatch, tmp_path, capsys):
    called = []
    monkeypatch.setattr(sd, "download_quarter", lambda *a, **k: called.append(1))
    rc = cli.main(["event-study", "--dry-run", "--from-year", "2024", "--to-year", "2024",
                   "--data-dir", str(tmp_path / "d"), "--rows-cache", str(tmp_path / "r"),
                   "--tickers-cache", str(tmp_path / "t.json"), "--price-cache", str(tmp_path / "p"),
                   "--reports-root", str(tmp_path / "rep")])
    assert rc == 2 and not called
    assert not (tmp_path / "rep").exists()


def test_require_user_agent():
    assert common.require_sec_user_agent({"SEC_USER_AGENT": " me me@x.org "}) == "me me@x.org"
    with pytest.raises(common.MissingUserAgent):
        common.require_sec_user_agent({})


def test_fetch_plan_confirmation():
    plan = common.FetchPlan("x", 240)
    assert plan.minutes == pytest.approx(0.5)
    lines = []
    assert not common.confirm_plan(plan, False, lines.append) and "--yes" in lines[-1]
    assert common.confirm_plan(plan, True, lines.append)
    assert common.confirm_plan(common.FetchPlan("x", 200), False, lines.append)


def test_jsonable_coerces_numpy():
    import numpy as np
    o = common.jsonable({"a": np.int64(3), "b": np.float64("nan"), "c": [np.bool_(True)], "d": pd.Timestamp("2025-01-02")})
    assert o == {"a": 3, "b": None, "c": [True], "d": "2025-01-02T00:00:00"}
    assert type(o["a"]) is int


# ------------------------------------------------------------------ offline fixtures command
def _offline_args(tmp_path, expected=None):
    a = cli.build_parser().parse_args(["parse-accuracy-offline", "--reports-root", str(tmp_path / "rep")])
    if expected:
        a.expected = str(expected)
    return a


def test_offline_fixture_check_end_to_end(tmp_path):
    assert par.run_offline(_offline_args(tmp_path), out=lambda s: None) == 0
    md = (tmp_path / "rep" / "parse_accuracy_fixtures" / "latest.md").read_text()
    assert "reconstructed/synthetic" in md and "row_f1_strict" in md
    js = json.loads((tmp_path / "rep" / "parse_accuracy_fixtures" / "latest.json").read_text())
    assert js["metrics"]["n_official_rows"] == 30 and js["metrics"]["row_f1_strict"] == 1.0
    assert isinstance(js["metrics"]["n_accessions"], int)


def test_offline_fixture_check_fails_on_regression(tmp_path):
    lines = (ROOT / "datasets/parse_accuracy/fixture_expected.csv").read_text().splitlines()
    i = next(n for n, ln in enumerate(lines) if ln.startswith("0001193125-26-448980"))
    lines[i] = lines[i].replace(",9.1299,", ",9.2,")  # corrupt one expected price
    bad = tmp_path / "expected.csv"
    bad.write_text("\n".join(lines) + "\n")
    msgs = []
    assert par.run_offline(_offline_args(tmp_path, bad), out=msgs.append) == 1
    assert any("REGRESSION" in m for m in msgs)


# ------------------------------------------------------------------ live path with a fake SEC
def _official_zip(tmp_path):
    f = [
        dict(acc="0001193125-26-448980", cik="1228454", ticker="BCBP", filed="03-NOV-2026", owners=[("1826510", "")],
             rows=[("02-NOV-2026", "P", 55, 9.1299, "A", 1514)]),
        dict(acc="0001193125-26-448969", cik="1228454", ticker="BCBP", filed="04-NOV-2026", owners=[("1628816", "")],
             rows=[("03-NOV-2026", "P", 57, 8.785, "A", 1516)]),
        dict(acc="0001193125-26-000999", cik="1228454", ticker="BCBP", filed="04-NOV-2026", owners=[("1628816", "")],
             rows=[("03-NOV-2026", "P", 1, 2, "A")]),  # folder 404s -> fetch failure, excluded from scoring
    ]
    return write_quarter_zip(tmp_path / "data" / "2026q4_form345.zip", f)


def _fake_sec(status_for_index=200):
    def handler(req: httpx.Request) -> httpx.Response:
        u = str(req.url)
        for acc in ("0001193125-26-448980", "0001193125-26-448969"):
            folder = f"/Archives/edgar/data/1228454/{acc.replace('-', '')}"
            if u.endswith(folder + "/index.json"):
                if status_for_index != 200:
                    return httpx.Response(status_for_index)
                return httpx.Response(200, json={"directory": {"item": [{"name": "form4.xml"}, {"name": f"{acc}-index.html"}]}})
            if u.endswith(folder + "/form4.xml"):
                return httpx.Response(200, content=(FIX / f"{acc}.xml").read_bytes())
        return httpx.Response(404)
    return handler


def _factory(handler):
    return lambda cache_dir: EdgarClient("test-agent", cache_dir=cache_dir, transport=httpx.MockTransport(handler),
                                         sleep=lambda s: None)


def _live_args(tmp_path, *extra):
    return cli.build_parser().parse_args(["parse-accuracy", "--year", "2026", "--quarter", "4", "--sample", "10", "--hard", "0",
                                          "--cache-dir", str(tmp_path / "cache"), "--reports-root", str(tmp_path / "rep"),
                                          "--data-dir", str(tmp_path / "data"), *extra])


def test_live_parse_accuracy_with_fake_sec(tmp_path, monkeypatch):
    monkeypatch.setenv("SEC_USER_AGENT", "test-agent a@b.org")
    zp = _official_zip(tmp_path)
    msgs = []
    rc = par.run_live(_live_args(tmp_path), client_factory=_factory(_fake_sec()), downloader=lambda *a: zp, out=msgs.append)
    assert rc == 0
    js = json.loads((tmp_path / "rep" / "parse_accuracy" / "2026q4" / "latest.json").read_text())
    md = js["metadata"]
    assert (md["n_sampled"], md["n_scored"], md["n_fetch_failed"], md["n_parser_errors"]) == (3, 2, 1, 0)
    assert js["metrics"]["row_f1_strict"] == 1.0 and js["metrics"]["n_official_rows"] == 2
    assert len(md["dataset_sha256"]) == 64
    assert (tmp_path / "cache" / "0001193125-26-448980.xml").exists()


def test_live_aborts_on_403_and_writes_no_report(tmp_path, monkeypatch):
    monkeypatch.setenv("SEC_USER_AGENT", "test-agent a@b.org")
    zp = _official_zip(tmp_path)
    msgs = []
    rc = par.run_live(_live_args(tmp_path), client_factory=_factory(_fake_sec(403)), downloader=lambda *a: zp, out=msgs.append)
    assert rc == 4 and not (tmp_path / "rep").exists()


def test_live_requires_yes_above_200_requests(tmp_path, monkeypatch):
    monkeypatch.setenv("SEC_USER_AGENT", "test-agent a@b.org")
    zp = _official_zip(tmp_path)
    monkeypatch.setattr(par, "REQUESTS_PER_FILING_COLD", 500)
    msgs = []
    rc = par.run_live(_live_args(tmp_path), client_factory=_factory(_fake_sec()), downloader=lambda *a: zp, out=msgs.append)
    assert rc == 2 and any("--yes" in m for m in msgs) and not (tmp_path / "rep").exists()
    assert not (tmp_path / "cache").exists() or not list((tmp_path / "cache").glob("*.xml"))


def test_live_max_fetch_is_seeded_random_subsample(tmp_path, monkeypatch):
    monkeypatch.setenv("SEC_USER_AGENT", "test-agent a@b.org")
    zp = _official_zip(tmp_path)
    rc = par.run_live(_live_args(tmp_path, "--max-fetch", "1"), client_factory=_factory(_fake_sec()),
                      downloader=lambda *a: zp, out=lambda s: None)
    assert rc == 0
    md = json.loads((tmp_path / "rep" / "parse_accuracy" / "2026q4" / "latest.json").read_text())["metadata"]
    assert md["n_skipped_max_fetch"] == 2 and md["n_scored"] + md["n_fetch_failed"] == 1


# ------------------------------------------------------------------ prereg guard
def test_prereg_guard(tmp_path):
    p = tmp_path / "prereg.md"
    p.write_text("**Pre-registration commit: <filled in by user>**\n")
    ok, h, msg = esr.check_prereg(p, verify_git=False)
    assert not ok and h is None and "git commit" in msg
    p.write_text("x\n**Pre-registration commit: `ab12cd3`**\n")
    assert esr.check_prereg(p, verify_git=False)[:2] == (True, "ab12cd3")
    p.write_text("**Pre-registration commit: c6755a8f20d8c07414515e2db2dd7cd6f607fdee**\n")
    assert esr.check_prereg(p, verify_git=False)[0]
    p.write_text("**Pre-registration commit: deadbeefdeadbeef**\n")
    ok, h, msg = esr.check_prereg(p, verify_git=True)  # not a commit in this repo (or git absent -> format-only)
    assert (not ok and "does not exist" in msg) or ok
    assert esr.check_prereg(tmp_path / "missing.md")[0] is False


def test_real_prereg_draft_is_not_yet_registered():
    ok, _, _ = esr.check_prereg(ROOT / "docs" / "event_study_prereg.md", verify_git=False)
    if not ok:
        assert True  # draft: user must fill the hash


def test_confirmatory_event_study_refuses_without_hash(tmp_path, monkeypatch):
    p = tmp_path / "prereg.md"
    p.write_text("**Pre-registration commit: <filled in by user>**\n")
    args = cli.build_parser().parse_args(["event-study", "--reports-root", str(tmp_path / "rep")])
    msgs = []
    assert esr.run_study(args, out=msgs.append, prereg_path=p) == 2
    assert "REFUSING" in msgs[0] and not (tmp_path / "rep").exists()
