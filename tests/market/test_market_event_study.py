import numpy as np
import pandas as pd
import pytest
from synth import make_calendar, make_market, make_stock, market_df, prices_df

from insider_intel.market.event_study import (
    bmp_test,
    bootstrap_ci,
    calendar_time_alpha,
    cross_sectional_t,
    holm_correction,
    run_event_study,
    score_decile_analysis,
)

CAL = make_calendar(1500)
RM = make_market(CAL, seed=123)
MKT = market_df(CAL, RM)
INJ = {k: 0.02 / 6 for k in range(6)}  # +2% total over days 0..5


def _world(n_events, seed, inject=None, windows=((0, 5), (0, 20))):
    rng = np.random.default_rng(seed)
    prices, evs = {}, []
    for i in range(n_events):
        p0 = int(rng.integers(300, 1300))
        beta = float(rng.uniform(0.6, 1.4))
        r = make_stock(CAL, RM, rng, beta=beta, inject_at=p0, inject=inject)
        tk = f"S{i}"
        prices[tk] = prices_df(CAL, r)
        evs.append({"ticker": tk, "event_day": CAL[p0]})
    return pd.DataFrame(evs), prices


def test_injected_car_is_recovered():
    ev, prices = _world(120, seed=1, inject=INJ)
    out = run_event_study(ev, prices, MKT, windows=[(0, 5), (0, 20), (-20, -1)], n_boot=300)
    r = out["results"]["market_model"]
    assert r["0_5"]["mean_car"] == pytest.approx(0.02, abs=0.007)
    assert r["0_5"]["t_cs"] > 3.5 and r["0_5"]["t_bmp"] > 3.5
    assert r["0_5"]["ci_lo"] > 0 and r["0_5"]["ci_lo"] < 0.02 < r["0_5"]["ci_hi"]
    # market-adjusted variant recovers it too (beta ~1 on average)
    assert out["results"]["market_adjusted"]["0_5"]["mean_car"] == pytest.approx(0.02, abs=0.008)
    # pre-event window should be null
    assert abs(r["m20_m1"]["t_cs"]) < 3.0
    assert out["coverage"]["n_events_ok"] == 120


def test_no_injection_gives_small_t():
    ev, prices = _world(120, seed=2)
    out = run_event_study(ev, prices, MKT, windows=[(0, 5)], n_boot=100)
    r = out["results"]["market_model"]["0_5"]
    assert abs(r["t_cs"]) < 3 and abs(r["t_bmp"]) < 3


def test_calibration_rejection_rate_under_null():
    rng = np.random.default_rng(7)
    rej_cs = rej_bmp = 0
    n_seeds, n_ev = 200, 25
    for _s in range(n_seeds):
        evs, prices = [], {}
        for i in range(n_ev):
            p0 = int(rng.integers(300, 1300))
            r = make_stock(CAL, RM, rng, beta=float(rng.uniform(0.6, 1.4)))
            prices[f"S{i}"] = prices_df(CAL, r)
            evs.append({"ticker": f"S{i}", "event_day": CAL[p0]})
        out = run_event_study(pd.DataFrame(evs), prices, MKT, windows=[(0, 5)], n_boot=10)
        res = out["results"]["market_model"]["0_5"]
        rej_cs += res["p_cs"] < 0.05
        rej_bmp += res["p_bmp"] < 0.05
    assert 0.02 <= rej_cs / n_seeds <= 0.09, rej_cs / n_seeds
    assert 0.02 <= rej_bmp / n_seeds <= 0.09, rej_bmp / n_seeds


def test_drop_reasons_and_estimation_window():
    rng = np.random.default_rng(3)
    p0 = 600
    prices = {}
    # normal
    prices["OK"] = prices_df(CAL, make_stock(CAL, RM, rng))
    # IPO'd 100 days before the event: < 120 estimation obs
    r = make_stock(CAL, RM, rng)
    young = prices_df(CAL, r).iloc[p0 - 100 :]
    prices["YOUNG"] = young
    # data ends 10 days after event: windows >10d are NaN, flagged delisted
    prices["DELIST"] = prices_df(CAL, make_stock(CAL, RM, rng)).iloc[: p0 + 10]
    # price below min
    prices["PENNY"] = prices_df(CAL, make_stock(CAL, RM, rng), p0=0.2)
    ev = pd.DataFrame(
        [{"ticker": t, "event_day": CAL[p0]} for t in ["OK", "YOUNG", "DELIST", "PENNY", "NOPRICE"]]
        + [{"ticker": "OK", "event_day": None}, {"ticker": "OK", "event_day": CAL[-3]}]
    )
    out = run_event_study(ev, prices, MKT, windows=[(0, 5), (0, 20)], min_price=1.0, n_boot=10)
    status = out["events"].drop_duplicates("ticker").set_index("ticker")["status"]
    assert status["YOUNG"] == "short_estimation_window"
    assert status["NOPRICE"] == "no_price_data"
    assert status["PENNY"] == "price_below_min"
    assert status["OK"] == "ok"
    reasons = out["coverage"]["drop_reasons"]
    assert reasons["short_estimation_window"] == 1
    assert reasons["no_price_data"] == 1
    assert reasons["price_below_min"] == 1
    assert reasons["no_event_day"] == 1
    d = out["events"].query("ticker == 'DELIST'").iloc[0]
    assert d["status"] == "ok"
    assert not np.isnan(d["car_mm_0_5"]) and np.isnan(d["car_mm_0_20"])
    assert "0_20:delisted_mid_window" in d["window_notes"]
    late = out["events"][out["events"]["event_day"] == CAL[-3]].iloc[0]
    assert "window_beyond_calendar" in late["window_notes"]
    assert late["status"] == "ok" or late["status"] == "no_valid_window"
    cov = out["coverage"]
    assert cov["n_events_input"] == 7 and "NOPRICE" in cov["tickers_missing_prices"]


def test_accepted_at_input_uses_event_day_rule():
    rng = np.random.default_rng(4)
    prices = {"A": prices_df(CAL, make_stock(CAL, RM, rng))}
    # Friday 2015-... pick a Friday in calendar, accepted after close -> Monday is day 0
    fri = next(d for d in CAL[400:] if d.dayofweek == 4)
    ev = pd.DataFrame({"ticker": ["A"], "accepted_at": [fri + pd.Timedelta(hours=22)]})  # 22:00 UTC = after close
    out = run_event_study(ev, prices, MKT, windows=[(0, 5)], n_boot=10)
    assert out["events"].loc[0, "event_day"] == fri + pd.Timedelta(days=3)


def test_calendar_time_alpha_recovers_injection():
    rng = np.random.default_rng(5)
    n = len(CAL)
    ff = pd.DataFrame(
        {
            "date": CAL,
            "mkt_rf": rng.normal(0.0004, 0.01, n),
            "smb": rng.normal(0, 0.004, n),
            "hml": rng.normal(0, 0.004, n),
            "rf": 0.0001,
        }
    )
    horizon, alpha_inj = 20, 0.001
    prices, evs = {}, []
    for i in range(400):
        p0 = int(rng.integers(100, n - 40))
        bm, bs, bh = rng.uniform(0.8, 1.2), rng.normal(0, 0.3), rng.normal(0, 0.3)
        r = (
            ff["rf"].to_numpy()
            + bm * ff["mkt_rf"].to_numpy()
            + bs * ff["smb"].to_numpy()
            + bh * ff["hml"].to_numpy()
            + rng.normal(0, 0.015, n)
        )
        r[0] = 0.0
        r[p0 + 1 : p0 + horizon + 1] += alpha_inj
        prices[f"S{i}"] = prices_df(CAL, r)
        evs.append({"ticker": f"S{i}", "event_day": CAL[p0]})
    res = calendar_time_alpha(pd.DataFrame(evs), prices, CAL, ff, horizon=horizon, min_stocks=5)
    assert res["ok"]
    assert res["alpha_daily"] == pytest.approx(alpha_inj, abs=0.0004)
    assert res["alpha_t"] > 3
    assert res["alpha_annual"] == pytest.approx(res["alpha_daily"] * 252)
    assert res["betas"]["mkt_rf"] == pytest.approx(1.0, abs=0.15)
    # a too-strict stocks/day threshold leaves too few days
    strict = calendar_time_alpha(pd.DataFrame(evs[:3]), prices, CAL, ff, horizon=horizon, min_stocks=50)
    assert not strict["ok"]


def test_calendar_time_null_alpha():
    rng = np.random.default_rng(6)
    n = len(CAL)
    ff = pd.DataFrame(
        {
            "date": CAL,
            "mkt_rf": rng.normal(0.0004, 0.01, n),
            "smb": rng.normal(0, 0.004, n),
            "hml": rng.normal(0, 0.004, n),
            "rf": 0.0001,
        }
    )
    prices, evs = {}, []
    for i in range(300):
        p0 = int(rng.integers(100, n - 40))
        r = ff["rf"].to_numpy() + ff["mkt_rf"].to_numpy() + rng.normal(0, 0.015, n)
        r[0] = 0.0
        prices[f"S{i}"] = prices_df(CAL, r)
        evs.append({"ticker": f"S{i}", "event_day": CAL[p0]})
    res = calendar_time_alpha(pd.DataFrame(evs), prices, CAL, ff, horizon=20, min_stocks=5)
    assert abs(res["alpha_t"]) < 3


def test_holm_correction():
    adj, rej = holm_correction([0.01, 0.04, 0.03, 0.005])
    assert adj == pytest.approx([0.03, 0.06, 0.06, 0.02])
    assert list(rej) == [True, False, False, True]
    adj, _ = holm_correction({"a": 0.5, "b": 0.9})
    assert adj == pytest.approx([1.0, 1.0])
    adj, rej = holm_correction([0.001, np.nan])
    assert adj[0] == pytest.approx(0.001) and np.isnan(adj[1]) and not rej[1]


def test_tests_basic_properties():
    x = np.r_[np.full(50, 0.02), np.full(50, 0.0)] + np.random.default_rng(0).normal(0, 0.01, 100)
    assert cross_sectional_t(x)["t"] > 5
    assert bmp_test(x / 0.01)["t"] > 5
    lo, hi = bootstrap_ci(x, n_boot=500, seed=1)
    assert lo < x.mean() < hi
    assert bootstrap_ci([1.0]) == (pytest.approx(np.nan, nan_ok=True), pytest.approx(np.nan, nan_ok=True))


def test_score_decile_analysis():
    rng = np.random.default_rng(0)
    score = rng.normal(size=1000)
    car = 0.01 * score + rng.normal(0, 0.02, 1000)
    res = score_decile_analysis(pd.DataFrame({"s": score, "car": car}), "s", "car")
    assert res["ok"] and len(res["table"]) == 10
    assert res["spread"] > 0.01 and res["t"] > 3 and res["spearman"] > 0.2
    noise = score_decile_analysis(pd.DataFrame({"s": score, "car": rng.normal(0, 0.02, 1000)}), "s", "car")
    assert abs(noise["t"]) < 3.5
    assert not score_decile_analysis(pd.DataFrame({"s": [1, 2], "car": [1, 2]}), "s", "car")["ok"]
