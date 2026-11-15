import json
from datetime import date

import numpy as np
import pandas as pd
import pytest
from synth_official import write_quarter_zip

from insider_intel.eval import __main__ as cli
from insider_intel.eval import event_inputs as ei
from insider_intel.eval import event_study_run as esr
from insider_intel.eval import ticker_map as tm
from insider_intel.market import event_study as es
from insider_intel.market import prices as pr


# ------------------------------------------------------------------ ticker map
def test_parse_company_tickers_shapes():
    d = {
        "0": {"cik_str": 1652044, "ticker": "GOOGL", "title": "Alphabet"},
        "1": {"cik_str": 1652044, "ticker": "GOOG", "title": "Alphabet"},
        "2": {"cik_str": "0000320193", "ticker": "aapl", "title": "Apple"},
    }
    m = tm.parse_company_tickers(d)
    assert m == {"1652044": ["GOOGL", "GOOG"], "320193": ["AAPL"]}
    ex = {"fields": ["cik", "name", "ticker", "exchange"], "data": [[320193, "Apple", "AAPL", "Nasdaq"]]}
    assert tm.parse_company_tickers(ex) == {"320193": ["AAPL"]}
    assert tm.parse_company_tickers([{"cik_str": 5, "ticker": "x"}]) == {"5": ["X"]}


def test_map_ciks_rules_and_coverage():
    sec = {"1652044": ["GOOGL", "GOOG"], "320193": ["AAPL"]}
    out, cov = tm.map_ciks({"1652044": "GOOG", "320193": "OLD", "999": "DEAD", "888": None}, sec)
    assert out["1652044"]["ticker"] == "GOOG" and out["1652044"]["source"] == "cik_current"  # share class kept
    assert out["320193"]["ticker"] == "AAPL" and out["320193"]["changed"]
    assert out["999"]["source"] == "filing_fallback" and out["888"]["ticker"] is None
    assert cov["by_source"] == {"cik_current": 2, "filing_fallback": 1, "unmapped": 1}


def test_load_company_tickers_cache_and_no_client(tmp_path):
    p = tmp_path / "ct.json"
    with pytest.raises(RuntimeError):
        tm.load_company_tickers(p, client=None)

    class C:
        calls = 0

        def get_json(self, url):
            C.calls += 1
            return {"0": {"cik_str": 1, "ticker": "AA"}}

    assert tm.load_company_tickers(p, client=C()) == {"1": ["AA"]}
    assert tm.load_company_tickers(p, client=C()) == {"1": ["AA"]} and C.calls == 1


# ------------------------------------------------------------------ historical event-input builder
FILINGS = [
    dict(
        acc="0000000001-25-000001",
        cik="0000111",
        ticker="AAA",
        filed="23-MAY-2025",
        aff="",
        owners=[("0000901", "Chief Executive Officer"), ("0000902", "")],
        rows=[("21-MAY-2025", "P", 100, 10, "A"), ("21-MAY-2025", "M", 5, 1, "A")],
    ),
    dict(
        acc="0000000001-25-000002",
        cik="0000111",
        ticker="AAA",
        filed="24-MAY-2025",
        aff="1",
        owners=[("0000903", "CFO")],
        rows=[("22-MAY-2025", "S", 50, 12, "D")],
    ),
    dict(
        acc="0000000001-25-000003",
        cik="0000111",
        ticker="AAA",
        filed="25-MAY-2025",
        aff="0",
        owners=[("0000903", "CFO")],
        rows=[("22-MAY-2025", "S", 20, 12, "D")],
    ),
    dict(
        acc="0000000001-25-000004",
        cik="0000111",
        ticker="AAA",
        filed="26-MAY-2025",
        aff="",
        owners=[("0000903", "CFO")],
        rows=[("22-MAY-2025", "S", 20, 12, "D")],
    ),  # blank flag -> None, never False
    dict(
        acc="0000000001-25-000005",
        form="4/A",
        cik="0000111",
        ticker="AAA",
        filed="26-MAY-2025",
        aff="",
        owners=[("0000903", "")],
        rows=[("22-MAY-2025", "P", 1, 10, "A")],
    ),
    dict(
        acc="0000000001-25-000006",
        cik="0000222",
        ticker="",
        filed="23-MAY-2025",
        aff="",
        owners=[("0000904", "")],
        rows=[("22-MAY-2025", "P", 10, 5, "A")],
    ),  # no ticker anywhere
    dict(
        acc="0000000001-25-000007",
        cik="0000333",
        ticker="dead",
        filed="23-MAY-2025",
        aff="",
        owners=[("0000905", "")],
        rows=[("22-MAY-2025", "P", 10, 5, "A")],
    ),  # filing ticker fallback
]


def test_extract_and_build_event_rows(tmp_path):
    z = write_quarter_zip(tmp_path / "2025q2_form345.zip", FILINGS, with_aff=True)
    df, info = ei.load_quarter_rows(2025, 2, tmp_path, tmp_path / "rc", downloader=lambda *a: z)
    assert info["aff10b5one_column_present"] and info["quarter"] == "2025q2" and len(info["zip_sha256"]) == 64
    assert set(df["code"]) == {"P", "S"}  # M row filtered
    # cache round trip: no downloader needed the second time
    assert ei.quarter_status(2025, 2, tmp_path, tmp_path / "rc") == "cached"
    df2, _ = ei.load_quarter_rows(2025, 2, tmp_path, tmp_path / "rc", downloader=lambda *a: 1 / 0)
    assert len(df2) == len(df)

    rows, ei_info = ei.build_event_rows([df2], {"111": ["AAA"]}, [info])
    f = ei_info["funnel"]
    assert f["rows_4A_excluded"] == 1 and f["rows_no_ticker_dropped"] == 1
    assert rows["accession_number"].str.endswith(("1", "2", "3", "4", "7")).all()
    by = rows.set_index("accession_number")
    assert by.loc["0000000001-25-000002", "is_10b5_1"] is True
    assert by.loc["0000000001-25-000003", "is_10b5_1"] is False
    assert by.loc["0000000001-25-000004", "is_10b5_1"] is None
    assert f["sell_rows"] == 3 and f["sell_rows_10b5_1_unknown_excluded_from_sell_kinds"] == 1
    # owners joint-normalised, titles joined, ticker sources recorded
    r1 = by.loc["0000000001-25-000001"]
    assert r1["owner_ciks"] == "901|902" and "Chief Executive Officer" in r1["officer_title"]
    assert (
        by.loc["0000000001-25-000007", "ticker_source"] == "filing_fallback"
        and by.loc["0000000001-25-000007", "issuer_ticker"] == "DEAD"
    )

    ev = es.build_events(rows, "officer_buy", min_price=1.0)
    assert list(ev["ticker"]) == ["AAA"] and ev.loc[0, "n_owners"] == 2
    assert list(es.build_events(rows, "planned_sell")["ticker"]) == ["AAA"]
    assert list(es.build_events(rows, "discretionary_sell")["ticker"]) == ["AAA"]  # only the flag=0 filing


def test_missing_aff_column_gives_unknown_not_false(tmp_path):
    z = write_quarter_zip(tmp_path / "2019q1_form345.zip", FILINGS[:4], with_aff=False)
    df, info = ei.load_quarter_rows(2019, 1, tmp_path, tmp_path / "rc", downloader=lambda *a: z)
    assert not info["aff10b5one_column_present"]
    assert df["aff_10b5_1"].isna().all()
    rows, _ = ei.build_event_rows([df], {"111": ["AAA"]})
    assert es.build_events(rows, "discretionary_sell").empty and es.build_events(rows, "planned_sell").empty


def test_parse_aff():
    assert [ei.parse_aff(v) for v in ("1", "true", "Y", "0", "false", "", None, "?")] == [
        True,
        True,
        True,
        False,
        False,
        None,
        None,
        None,
    ]


def _cal_with_holiday():
    cal = pd.bdate_range("2025-01-01", "2025-12-31")
    return cal.delete(cal.get_loc(pd.Timestamp("2025-05-26")))  # Memorial Day


@pytest.mark.parametrize(
    "filed,expected",
    [
        ("2025-05-23", "2025-05-27"),  # Friday before a Monday holiday
        ("2025-03-07", "2025-03-10"),  # Friday, EST
        ("2025-03-10", "2025-03-11"),  # first Monday of EDT
        ("2025-10-31", "2025-11-03"),  # Friday before DST ends
        ("2025-11-03", "2025-11-04"),  # first Monday of EST
        ("2025-06-03", "2025-06-04"),
        ("2025-06-07", "2025-06-09"),  # Saturday filing date
    ],
)
def test_filing_date_stamp_gives_strictly_next_session(filed, expected):
    stamp = ei.info_timestamp(pd.Series([pd.Timestamp(filed)])).iloc[0]
    assert es.event_day(stamp, _cal_with_holiday()) == pd.Timestamp(expected)


# ------------------------------------------------------------------ offline dry run end to end
def _prices_csv(path, cal, seed, p0=50.0, sd=0.012):
    rng = np.random.default_rng(seed)
    r = rng.normal(0.0004, sd, len(cal))
    r[0] = 0
    p = p0 * np.cumprod(1 + r)
    pd.DataFrame({"date": cal, "open": p, "high": p, "low": p, "close": p, "adj_close": p, "volume": 1e6}).to_csv(
        path, index=False
    )


def test_dry_run_end_to_end_offline(tmp_path, monkeypatch):
    data, rc_, pc, csv = tmp_path / "data", tmp_path / "rc", tmp_path / "px", tmp_path / "csv"
    csv.mkdir()
    tickers = [f"T{i}" for i in range(6)]
    filings = []
    for i, t in enumerate(tickers):
        for n, (d, acc_n) in enumerate([("14-APR-2025", 1), ("22-APR-2025", 2)]):
            filings.append(
                dict(
                    acc=f"00000000{i}{n}-25-00000{acc_n}",
                    cik=str(1000 + i),
                    ticker=t,
                    filed=d,
                    aff="0",
                    owners=[(f"{5000 + i * 10 + n}", "Chief Executive Officer" if n == 0 else "Director")],
                    rows=[("10-APR-2025", "P", 100, 20 + i, "A"), ("11-APR-2025", "S", 50, 20 + i, "D")],
                )
            )
    write_quarter_zip(data / "2025q2_form345.zip", filings[:6])
    write_quarter_zip(data / "2025q3_form345.zip", filings[6:])
    cal = pd.bdate_range("2023-01-02", "2025-10-14")
    for i, t in enumerate(tickers + ["SPY"]):
        _prices_csv(csv / f"{t}.csv", cal, i)
    ct = tmp_path / "ct.json"
    ct.write_text(json.dumps({str(i): {"cik_str": 1000 + i, "ticker": t, "title": t} for i, t in enumerate(tickers)}))
    ffd = pd.bdate_range("2023-01-02", "2025-09-30")
    rng = np.random.default_rng(1)
    pr.write_parquet(
        pd.DataFrame(
            {
                "date": ffd,
                "mkt_rf": rng.normal(0, 0.01, len(ffd)),
                "smb": rng.normal(0, 0.005, len(ffd)),
                "hml": rng.normal(0, 0.005, len(ffd)),
                "rf": 0.0001,
            }
        ),
        pc / "ff3_daily.parquet",
    )
    args = cli.build_parser().parse_args(
        [
            "event-study",
            "--dry-run",
            "--from-year",
            "2025",
            "--to-year",
            "2025",
            "--prices",
            f"csvdir:{csv}",
            "--data-dir",
            str(data),
            "--rows-cache",
            str(rc_),
            "--tickers-cache",
            str(ct),
            "--price-cache",
            str(pc),
            "--reports-root",
            str(tmp_path / "rep"),
            "--n-boot",
            "50",
        ]
    )
    msgs = []
    rc = esr.run_study(args, today=date(2025, 10, 15), out=msgs.append)
    assert rc == 0, msgs
    d = tmp_path / "rep" / "event_study_dryrun"
    js = json.loads((d / "latest.json").read_text())
    assert js["metadata"]["confirmatory"] is False and js["metadata"]["prereg_commit"] is None
    assert "NON-CONFIRMATORY" in (d / "latest.md").read_text()
    ids = [r["id"] for r in js["metrics"]["primary"]]
    assert ids == ["H1", "H2", "H3"]
    assert js["metrics"]["price_coverage"]["found"] == 7  # six tickers + SPY
    assert "Pre-registered items: status" in js["tables"] and any(
        r["status"] == "deferred" for r in js["tables"]["Pre-registered items: status"]
    )
    assert js["metrics"]["events_built"]["single_buy"] > 0
    assert not (tmp_path / "rep" / "event_study").exists()


def test_price_pass_with_provider_errors_is_a_failed_pass(tmp_path, monkeypatch):
    data, csv = tmp_path / "data", tmp_path / "csv"
    csv.mkdir()
    write_quarter_zip(data / "2025q2_form345.zip", FILINGS[:1])
    write_quarter_zip(data / "2025q3_form345.zip", FILINGS[:1])
    ct = tmp_path / "ct.json"
    ct.write_text(json.dumps({"0": {"cik_str": 111, "ticker": "AAA"}}))

    class Blocked(pr.PriceProvider):
        name = "x"

        def daily(self, t, s, e):
            raise pr.ProviderBlocked("429")

    monkeypatch.setattr(esr, "parse_provider", lambda spec, mi=0.5: (Blocked(), 0.0, "yahoo_chart"))
    args = cli.build_parser().parse_args(
        [
            "event-study",
            "--dry-run",
            "--from-year",
            "2025",
            "--to-year",
            "2025",
            "--yes",
            "--data-dir",
            str(data),
            "--rows-cache",
            str(tmp_path / "rc"),
            "--tickers-cache",
            str(ct),
            "--price-cache",
            str(tmp_path / "px"),
            "--reports-root",
            str(tmp_path / "rep"),
        ]
    )
    msgs = []
    assert esr.run_study(args, today=date(2025, 10, 15), out=msgs.append) == 3
    assert any("INCOMPLETE" in m for m in msgs) and not (tmp_path / "rep").exists()


def test_analyze_mechanical_verdicts():
    # synthetic results with a clear positive cluster effect and nothing else
    rng = np.random.default_rng(0)
    cal = pd.bdate_range("2015-01-01", periods=1500)
    rm = rng.normal(0.0004, 0.01, len(cal))
    rm[0] = 0
    mk = pd.DataFrame({"date": cal, "adj_close": 100 * np.cumprod(1 + rm)})
    prices, ev = {}, []
    for i in range(60):
        r = 1.0 * rm + rng.normal(0, 0.012, len(cal))
        r[0] = 0
        p0 = int(rng.integers(300, 1300))
        r[p0 : p0 + 20] += 0.01
        prices[f"S{i}"] = pd.DataFrame({"date": cal, "adj_close": 50 * np.cumprod(1 + r)})
        ev.append({"ticker": f"S{i}", "event_day": cal[p0]})
    res = {"cluster_buy": es.run_event_study(pd.DataFrame(ev), prices, mk, n_boot=200)}
    a = esr.analyze(res)
    h1 = a["primary"][0]
    assert h1["id"] == "H1" and h1["verdict"] == "supported" and h1["p_holm"] < 0.05
    assert a["primary"][1]["verdict"] == "no data" and a["primary"][2]["verdict"] == "no data"
    assert a["n_tests_family1"] == 1


def test_preregistered_quarter_range_is_exact():
    from datetime import date

    from insider_intel.eval.event_study_run import quarters_in_range

    qs = quarters_in_range(2016, 2026, date(2026, 11, 14), from_quarter=2, to_quarter=1)
    assert qs[0] == (2016, 2) and qs[-1] == (2026, 1) and len(qs) == 40
    # never beyond the last complete calendar quarter
    assert quarters_in_range(2026, 2026, date(2026, 5, 1))[-1] == (2026, 1)


def test_confirmatory_run_refuses_unregistered_range(tmp_path):
    import argparse

    from insider_intel.eval import event_study_run as esr

    pre = tmp_path / "prereg.md"
    pre.write_text("**Pre-registration commit: 0123456789abcdef0123456789abcdef01234567**\n")
    args = argparse.Namespace(
        from_year=2020,
        from_quarter=1,
        to_year=2021,
        to_quarter=4,
        dry_run=False,
        max_tickers=None,
        kinds="single_buy",
        prices="csvdir:/nonexistent",
        yes=True,
    )
    log: list[str] = []
    assert esr.run_study(args, out=log.append, prereg_path=pre, verify_git=False) == 2
    assert any("pre-registered sample" in m for m in log)
