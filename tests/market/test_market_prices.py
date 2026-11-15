import json
from datetime import date

import pandas as pd
import pytest

from insider_intel.market.prices import (
    CSVDirProvider,
    PriceCache,
    PriceProvider,
    ProviderBlocked,
    ProviderError,
    empty_prices,
    parse_ff_daily_csv,
    parse_stooq_csv,
    parse_yahoo_chart,
    read_parquet,
    trading_calendar,
    write_parquet,
)

STOOQ = """Date,Open,High,Low,Close,Volume
2024-01-02,100.0,101.0,99.0,100.5,1000
2024-01-03,100.5,102.0,100.0,101.5,2000
2024-01-03,100.5,102.0,100.0,101.7,2100
"""

FF = """This file was created by using the 202609 CRSP database.
The Tbill return is the simple daily rate.

,Mkt-RF,SMB,HML,RF
19260701,    0.09,   -0.23,   -0.28,    0.01
19260702,    0.45,   -0.34,   -0.03,    0.01
20260930,   -0.26,    0.32,   -0.88,    0.01

Copyright 2026 Eugene F. Fama and Kenneth R. French
"""


def test_parse_stooq_csv_and_adjustment_note():
    df = parse_stooq_csv(STOOQ)
    assert list(df.columns) == ["date", "open", "high", "low", "close", "adj_close", "volume"]
    assert len(df) == 2  # duplicate date collapsed (last wins)
    assert df["adj_close"].tolist() == df["close"].tolist() == [100.5, 101.7]
    assert df["date"].iloc[0] == pd.Timestamp("2024-01-02")


def test_stooq_no_data_and_html_challenge():
    assert parse_stooq_csv("No data").empty
    with pytest.raises(ProviderBlocked):
        parse_stooq_csv(
            "<!DOCTYPE html><html><body>This site requires JavaScript to verify your browser.</body></html>"
        )


def test_parse_yahoo_chart():
    payload = {
        "chart": {
            "result": [
                {
                    "meta": {"gmtoffset": -18000},
                    "timestamp": [1699972200, 1700058600],
                    "indicators": {
                        "quote": [
                            {"open": [1, 2], "high": [2, 3], "low": [0.5, 1], "close": [1.5, 2.5], "volume": [10, 20]}
                        ],
                        "adjclose": [{"adjclose": [1.4, 2.4]}],
                    },
                }
            ],
            "error": None,
        }
    }
    df = parse_yahoo_chart(payload)
    assert df["date"].tolist() == [pd.Timestamp("2023-11-14"), pd.Timestamp("2023-11-15")]
    assert df["adj_close"].tolist() == [1.4, 2.4] and df["close"].tolist() == [1.5, 2.5]
    nf = {"chart": {"result": None, "error": {"code": "Not Found", "description": "x"}}}
    assert parse_yahoo_chart(nf).empty


def test_parse_ff_daily_csv_header_footer_and_units():
    df = parse_ff_daily_csv(FF)
    assert len(df) == 3
    assert df.loc[0, "date"] == pd.Timestamp("1926-07-01")
    assert df.loc[0, "mkt_rf"] == pytest.approx(0.0009)  # percent -> decimal
    assert df.loc[2, "hml"] == pytest.approx(-0.0088)
    with pytest.raises(ProviderError):
        parse_ff_daily_csv("nothing here")


def test_parquet_roundtrip_via_duckdb(tmp_path):
    df = parse_stooq_csv(STOOQ)
    write_parquet(df, tmp_path / "x.parquet")
    back = read_parquet(tmp_path / "x.parquet")
    assert back["adj_close"].tolist() == df["adj_close"].tolist()
    assert pd.to_datetime(back["date"]).tolist() == df["date"].tolist()


def test_trading_calendar():
    df = pd.DataFrame({"date": ["2024-01-03", "2024-01-02", "2024-01-03"], "adj_close": [1, 2, 3]})
    cal = trading_calendar(df)
    assert list(cal) == [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")]


def test_csv_dir_provider(tmp_path):
    (tmp_path / "AAA.csv").write_text(STOOQ)
    p = CSVDirProvider(tmp_path)
    assert len(p.daily("AAA", "2024-01-01", "2024-01-31")) == 2
    assert len(p.daily("AAA", "2024-01-03", "2024-01-31")) == 1
    assert p.daily("ZZZ", "2024-01-01", "2024-01-31").empty


class FakeProvider(PriceProvider):
    name = "fake"

    def __init__(self, known=("AAA", "BBB"), fail=()):
        self.calls = []
        self.known, self.fail = set(known), set(fail)

    def daily(self, ticker, start, end):
        self.calls.append((ticker, pd.Timestamp(start), pd.Timestamp(end)))
        if ticker in self.fail:
            raise ProviderError("boom")
        if ticker not in self.known:
            return empty_prices()
        d = pd.bdate_range(start, end)
        return pd.DataFrame(
            {"date": d, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "adj_close": 1.0, "volume": 1}
        )


def _cache(tmp_path, prov, **kw):
    return PriceCache(prov, tmp_path, today=date(2024, 2, 1), clock=lambda: 0.0, sleep=lambda s: None, **kw)


def test_cache_fetches_only_missing_ranges(tmp_path):
    prov = FakeProvider()
    c = _cache(tmp_path, prov)
    df = c.get("AAA", "2024-01-02", "2024-01-15")
    assert len(prov.calls) == 1 and df["date"].min() == pd.Timestamp("2024-01-02")
    c.get("AAA", "2024-01-02", "2024-01-15")
    assert len(prov.calls) == 1  # fully cached
    df = c.get("AAA", "2024-01-02", "2024-01-31")
    assert len(prov.calls) == 2 and prov.calls[1][1] == pd.Timestamp("2024-01-16")
    df = c.get("AAA", "2023-12-01", "2024-01-31")
    assert len(prov.calls) == 3 and prov.calls[2][2] == pd.Timestamp("2024-01-01")
    assert (
        df["date"].is_unique and df["date"].is_monotonic_increasing and df["date"].min() == pd.Timestamp("2023-12-01")
    )
    meta = json.loads((tmp_path / "meta.json").read_text())
    assert meta["AAA"]["start"] == "2023-12-01" and meta["AAA"]["end"] == "2024-01-31"
    # end beyond yesterday is clamped (no future / partial bar)
    c.get("AAA", "2023-12-01", "2030-01-01")
    assert len(prov.calls) == 3


def test_cache_coverage_errors_and_blocked(tmp_path):
    prov = FakeProvider(fail=("CCC",))
    c = _cache(tmp_path, prov)
    out = c.get_many(["aaa", "ZZZ", "CCC", "BBB"], "2024-01-02", "2024-01-15")
    cov = c.coverage.to_dict()
    assert set(out) == {"AAA", "BBB"}
    assert cov["requested"] == 4 and cov["found"] == 2 and cov["missing"] == ["ZZZ"] and "CCC" in cov["errors"]
    assert cov["found_rate"] == 0.5
    # tickers with no data are remembered: second pass does not re-request ZZZ
    n = len(prov.calls)
    c.get_many(["ZZZ"], "2024-01-02", "2024-01-15")
    assert len(prov.calls) == n

    class Blocked(FakeProvider):
        def daily(self, *a):
            raise ProviderBlocked("captcha")

    c2 = _cache(tmp_path / "b", Blocked())
    c2.get_many(["AAA", "BBB", "CCC"], "2024-01-02", "2024-01-15")
    assert len(c2.coverage.errors) == 1  # stops after first block instead of hammering


def test_cache_rate_limit_enforced(tmp_path):
    t = {"now": 0.0}
    sleeps = []

    def sleep(s):
        sleeps.append(s)
        t["now"] += s

    c = PriceCache(
        FakeProvider(), tmp_path, min_interval=0.5, clock=lambda: t["now"], sleep=sleep, today=date(2024, 2, 1)
    )
    c.get_many(["AAA", "BBB", "CCC"], "2024-01-02", "2024-01-15")
    assert len(sleeps) == 2 and all(s == pytest.approx(0.5) for s in sleeps)


def test_yahoo_400_data_doesnt_exist_is_empty_not_error():
    import httpx

    from insider_intel.market.prices import YahooChartProvider

    body = {
        "chart": {
            "result": None,
            "error": {"code": "Bad Request", "description": "Data doesn't exist for startDate = 1, endDate = 2"},
        }
    }
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(400, json=body)))
    assert YahooChartProvider(client=client).daily("CSRA", "2016-01-01", "2016-12-31").empty


def test_yahoo_other_400_is_still_an_error():
    import httpx

    from insider_intel.market.prices import YahooChartProvider

    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(400, text="bad symbol")))
    with pytest.raises(ProviderError):
        YahooChartProvider(client=client).daily("X", "2016-01-01", "2016-12-31")
