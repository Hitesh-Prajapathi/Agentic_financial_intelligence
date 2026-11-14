"""Event-study engine for insider-trade signals (plan step E4).

Pure functions on DataFrames; no network, no file IO.  Conventions:

* ``calendar``: ``pd.DatetimeIndex`` of trading days (from ``prices.trading_calendar``).
* ``prices``: ``dict[ticker -> DataFrame[date, ..., adj_close]]``.
* ``market``: ``DataFrame[date, adj_close]`` for the market proxy (SPY).
* Day 0 = the event day from :func:`event_day`.  The return of trading day t is
  ``adj_close_t / adj_close_{t-1} - 1`` (close-to-close), so day-0 return contains the
  overnight gap into the first session at which the news is tradable.
* Windows ``(a, b)`` are inclusive trading-day offsets from day 0.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
from scipy import stats

try:  # zoneinfo ships with 3.9+
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

DEFAULT_WINDOWS: list[tuple[int, int]] = [(0, 5), (0, 20), (0, 60), (-20, -1)]
EST_WINDOW = (-250, -30)
MIN_EST_OBS = 120
NY = ZoneInfo("America/New_York")
OPEN_T = pd.Timedelta(hours=9, minutes=30)
CLOSE_T = pd.Timedelta(hours=16)

CEO_CFO_RE = re.compile(
    r"\b(CEO|CFO|Chief\s+Executive|Chief\s+Financial|Principal\s+Executive|Principal\s+Financial)\b",
    re.I,
)


# --------------------------------------------------------------------------- #
# Event day
# --------------------------------------------------------------------------- #
def event_day(accepted_at_utc, calendar: pd.DatetimeIndex):
    """First trading session that opens AFTER the information is public.

    ``accepted_at_utc`` is the EDGAR acceptance time (naive values are treated as UTC).
    Converted to US/Eastern (DST-aware), then, with ``d`` the Eastern calendar date:

    * ``d`` is a trading day and time < 09:30 ET   -> ``d`` (can trade the open)
    * ``d`` is a trading day and 09:30 <= time < 16:00 ET -> next trading day
      (conservative: cannot trade at the open that already happened)
    * time >= 16:00 ET, or ``d`` is not a trading day (weekend/holiday, any time)
      -> first trading day strictly after ``d``

    Returns a normalised ``pd.Timestamp`` or ``None`` if the next session is beyond the
    end of ``calendar`` (event too recent) or the input is missing.  Half-day early
    closes (13:00 ET) are not modelled; treating a post-13:00 filing on those few days as
    intraday is slightly conservative-to-neutral.
    """
    if accepted_at_utc is None or pd.isna(accepted_at_utc):
        return None
    ts = pd.Timestamp(accepted_at_utc)
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    local = ts.tz_convert(NY)
    d = pd.Timestamp(local.year, local.month, local.day)
    tod = pd.Timedelta(hours=local.hour, minutes=local.minute, seconds=local.second)
    cal = pd.DatetimeIndex(calendar)
    is_trading = d in cal
    if is_trading and tod < OPEN_T:
        return d
    pos = cal.searchsorted(d, side="right")  # first trading day strictly after d
    if pos >= len(cal):
        return None
    return cal[pos]


# --------------------------------------------------------------------------- #
# Event construction
# --------------------------------------------------------------------------- #
def _owner_set(v) -> set[str]:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return {"unknown"}
    if isinstance(v, (list, tuple, set, np.ndarray, pd.Series)):
        items = [str(x).strip() for x in v if str(x).strip()]
    else:
        items = [x.strip() for x in re.split(r"[,;|\s]+", str(v)) if x.strip()]
    return set(items) or {"unknown"}


def _dedup(df: pd.DataFrame, days: int | None) -> pd.DataFrame:
    """Per issuer, keep an event only if >= ``days`` calendar days after the last KEPT one."""
    if not days or df.empty:
        return df
    keep = []
    for _, g in df.sort_values("accepted_at").groupby("ticker", sort=False):
        last = None
        for idx, t in zip(g.index, g["accepted_at"]):
            if last is None or (t - last) >= pd.Timedelta(days=days):
                keep.append(idx)
                last = t
    return df.loc[keep].sort_values(["accepted_at", "ticker"]).reset_index(drop=True)


def _filing_events(rows: pd.DataFrame) -> pd.DataFrame:
    """Collapse trade rows to one candidate event per (ticker, accepted_at)."""
    r = rows.copy()
    r["_owners"] = r["owner_ciks"].map(_owner_set) if "owner_ciks" in r else [{"unknown"}] * len(r)
    r["_value"] = r["shares"].astype(float) * r["price_per_share"].astype(float) if {"shares", "price_per_share"} <= set(r) else np.nan
    out = []
    for (tk, at), g in r.groupby(["issuer_ticker", "accepted_at"], sort=False):
        owners = set().union(*g["_owners"])
        out.append(
            {
                "ticker": str(tk).upper(),
                "accepted_at": pd.Timestamp(at),
                "owner_ciks": sorted(owners),
                "n_owners": len(owners),
                "total_shares": float(g["shares"].sum()) if "shares" in g else np.nan,
                "total_value": float(np.nansum(g["_value"])) if g["_value"].notna().any() else np.nan,
                "price": float(g["price_per_share"].median()) if "price_per_share" in g else np.nan,
            }
        )
    return pd.DataFrame(out)


def _clean_rows(rows_df: pd.DataFrame, code: str, min_price) -> pd.DataFrame:
    need = {"issuer_ticker", "code", "accepted_at"}
    miss = need - set(rows_df.columns)
    if miss:
        raise ValueError(f"rows_df missing columns: {sorted(miss)}")
    r = rows_df[rows_df["code"].astype(str).str.upper() == code].copy()
    r = r[r["issuer_ticker"].notna() & (r["issuer_ticker"].astype(str).str.strip() != "")]
    r["accepted_at"] = pd.to_datetime(r["accepted_at"])
    r = r[r["accepted_at"].notna()]
    if min_price is not None and "price_per_share" in r:
        r = r[r["price_per_share"].astype(float) >= min_price]
    return r


def build_events(rows_df: pd.DataFrame, kind: str, *, min_price: float | None = None, dedup_days: int | None = 30, **params) -> pd.DataFrame:
    """Build an events table (one row per event) from parsed Form 4 rows.

    Output columns: ``event_id, kind, ticker, accepted_at, owner_ciks, n_owners,
    total_shares, total_value, price`` (``accepted_at`` naive UTC).

    kinds / params
      * ``single_buy``          any open-market purchase (code P)
      * ``large_buy``           P with filing value >= ``min_value`` (default 100_000)
      * ``officer_buy``         P by an owner whose ``officer_title`` matches ``title_regex``
                                (default CEO/CFO); needs an ``officer_title`` column
      * ``cluster_buy``         >= ``k`` (default 2) DISTINCT owners with P in the same issuer
                                within ``window_days`` (default 30) calendar days; the event
                                is stamped at the acceptance of the filing that completes the
                                k-th distinct owner (point-in-time: no look-ahead)
      * ``discretionary_sell``  S with ``is_10b5_1`` False
      * ``planned_sell``        S with ``is_10b5_1`` True
        (rows with unknown ``is_10b5_1`` are excluded from both sell kinds)

    ``dedup_days``: per issuer, no new event within this many calendar days of the previous
    *kept* event (non-chained); ``None``/0 disables.  ``min_price`` drops rows whose reported
    ``price_per_share`` is below it (e.g. 1.0).
    """
    if kind in ("single_buy", "large_buy", "officer_buy", "cluster_buy"):
        rows = _clean_rows(rows_df, "P", min_price)
    elif kind in ("discretionary_sell", "planned_sell"):
        rows = _clean_rows(rows_df, "S", min_price)
        if "is_10b5_1" not in rows:
            raise ValueError("is_10b5_1 column required for sell kinds")
        flag = rows["is_10b5_1"].map(lambda v: np.nan if pd.isna(v) else bool(v)).astype(float)
        rows = rows[flag == (1.0 if kind == "planned_sell" else 0.0)]
    else:
        raise ValueError(f"unknown event kind: {kind}")

    if kind == "officer_buy":
        if "officer_title" not in rows:
            raise ValueError("officer_title column required for officer_buy")
        rx = re.compile(params.get("title_regex", CEO_CFO_RE.pattern), re.I)
        rows = rows[rows["officer_title"].fillna("").astype(str).map(lambda s: bool(rx.search(s)))]

    cols = ["event_id", "kind", "ticker", "accepted_at", "owner_ciks", "n_owners", "total_shares", "total_value", "price"]
    if rows.empty:
        return pd.DataFrame(columns=cols)

    if kind == "cluster_buy":
        ev = _cluster_events(rows, k=int(params.get("k", 2)), window_days=int(params.get("window_days", 30)))
        ev = _dedup(ev, dedup_days)
    else:
        ev = _filing_events(rows)
        if kind == "large_buy":
            ev = ev[ev["total_value"] >= float(params.get("min_value", 100_000))]
        ev = _dedup(ev, dedup_days)
    if ev.empty:
        return pd.DataFrame(columns=cols)
    ev = ev.sort_values(["accepted_at", "ticker"]).reset_index(drop=True)
    ev["kind"] = kind
    ev["event_id"] = [f"{kind}:{t}:{a:%Y%m%dT%H%M%S}" for t, a in zip(ev["ticker"], ev["accepted_at"])]
    return ev[cols]


def _cluster_events(rows: pd.DataFrame, k: int, window_days: int) -> pd.DataFrame:
    """Candidate cluster events: at each filing, count distinct known owners with P in the
    trailing ``window_days`` (inclusive of this filing); emit if >= k."""
    fe = _filing_events(rows)
    out = []
    win = pd.Timedelta(days=window_days)
    for tk, g in fe.sort_values("accepted_at").groupby("ticker", sort=False):
        g = g.reset_index(drop=True)
        for i in range(len(g)):
            t = g.loc[i, "accepted_at"]
            w = g[(g["accepted_at"] <= t) & (g["accepted_at"] >= t - win)]
            owners = set().union(*[set(o) for o in w["owner_ciks"]]) - {"unknown"}
            if len(owners) >= k:
                out.append(
                    {
                        "ticker": tk,
                        "accepted_at": t,
                        "owner_ciks": sorted(owners),
                        "n_owners": len(owners),
                        "total_shares": float(w["total_shares"].sum()),
                        "total_value": float(np.nansum(w["total_value"])) if w["total_value"].notna().any() else np.nan,
                        "price": float(w["price"].median()),
                    }
                )
    return pd.DataFrame(out, columns=["ticker", "accepted_at", "owner_ciks", "n_owners", "total_shares", "total_value", "price"])


# --------------------------------------------------------------------------- #
# Return alignment and market model
# --------------------------------------------------------------------------- #
def _aligned(df: pd.DataFrame, cal: pd.DatetimeIndex, col: str = "adj_close") -> np.ndarray:
    dcol = df["date"]
    # fast path: skip pd.to_datetime (very slow per call) when the column is already datetime64
    idx = pd.DatetimeIndex(dcol if pd.api.types.is_datetime64_any_dtype(dcol) else pd.to_datetime(dcol)).normalize()
    vals = pd.to_numeric(df[col], errors="coerce").to_numpy(dtype=float).copy()
    keep = ~idx.duplicated(keep="last")
    idx, vals = idx[keep], vals[keep]
    vals[~(vals > 0)] = np.nan
    out = np.full(len(cal), np.nan)
    pos = cal.get_indexer(idx)
    ok = pos >= 0
    out[pos[ok]] = vals[ok]
    return out


def _returns(p: np.ndarray) -> np.ndarray:
    r = np.full(len(p), np.nan)
    r[1:] = p[1:] / p[:-1] - 1.0
    return r


@dataclass
class MarketModel:
    alpha: float
    beta: float
    sigma2: float  # residual variance (n-2)
    n: int
    mean_rm: float
    sxx: float  # sum (rm - mean)^2 over estimation window
    sigma2_ma: float  # variance of (r - rm) over the window (market-adjusted model)


def fit_market_model(r: np.ndarray, rm: np.ndarray, min_obs: int = MIN_EST_OBS) -> MarketModel | None:
    ok = ~(np.isnan(r) | np.isnan(rm))
    n = int(ok.sum())
    if n < min_obs:
        return None
    y, x = r[ok], rm[ok]
    xm = x.mean()
    sxx = float(((x - xm) ** 2).sum())
    if sxx <= 0:
        return None
    beta = float(((x - xm) * (y - y.mean())).sum() / sxx)
    alpha = float(y.mean() - beta * xm)
    resid = y - alpha - beta * x
    s2 = float((resid**2).sum() / (n - 2))
    d = y - x
    return MarketModel(alpha, beta, s2, n, float(xm), sxx, float(d.var(ddof=1)))


def wname(a: int, b: int) -> str:
    f = lambda v: f"m{-v}" if v < 0 else str(v)  # noqa: E731
    return f"{f(a)}_{f(b)}"


def abnormal_returns(r: np.ndarray, rm: np.ndarray, mm: MarketModel | None, model: str = "market_model") -> np.ndarray:
    if model == "market_adjusted":
        return r - rm
    return r - (mm.alpha + mm.beta * rm)


def car_variance(mm: MarketModel, rm_win: np.ndarray, model: str = "market_model") -> float:
    """Forecast-error variance of a CAR over a window (Boehmer et al. correction)."""
    L = len(rm_win)
    if model == "market_adjusted":
        return mm.sigma2_ma * L
    return mm.sigma2 * (L + L**2 / mm.n + float(((rm_win.sum() - L * mm.mean_rm) ** 2) / mm.sxx))


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def cross_sectional_t(x: Sequence[float]) -> dict:
    x = np.asarray(x, float)
    x = x[~np.isnan(x)]
    n = len(x)
    if n < 2:
        return {"n": n, "mean": float(x.mean()) if n else np.nan, "t": np.nan, "p": np.nan}
    se = x.std(ddof=1) / np.sqrt(n)
    t = x.mean() / se if se > 0 else np.nan
    return {"n": n, "mean": float(x.mean()), "t": float(t), "p": float(2 * stats.t.sf(abs(t), n - 1))}


def bmp_test(scar: Sequence[float]) -> dict:
    """Boehmer-Musumeci-Poulsen (1991) standardized cross-sectional test.

    ``scar_i = CAR_i / sqrt(forecast-error variance_i)``; statistic is
    ``mean(scar) / (std(scar)/sqrt(N))`` using the *cross-sectional* std, which is robust to
    event-induced variance increases.  p-value from t(N-1).
    """
    s = np.asarray(scar, float)
    s = s[~np.isnan(s)]
    n = len(s)
    if n < 2:
        return {"n": n, "t": np.nan, "p": np.nan}
    sd = s.std(ddof=1)
    t = s.mean() / (sd / np.sqrt(n)) if sd > 0 else np.nan
    return {"n": n, "t": float(t), "p": float(2 * stats.t.sf(abs(t), n - 1))}


def bootstrap_ci(x: Sequence[float], n_boot: int = 2000, alpha: float = 0.05, seed: int = 0) -> tuple[float, float]:
    x = np.asarray(x, float)
    x = x[~np.isnan(x)]
    if len(x) < 2:
        return (np.nan, np.nan)
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(x), size=(n_boot, len(x)))
    means = x[idx].mean(axis=1)
    return (float(np.quantile(means, alpha / 2)), float(np.quantile(means, 1 - alpha / 2)))


def holm_correction(pvals, alpha: float = 0.05):
    """Holm-Bonferroni step-down.  Returns ``(adjusted_p, reject)`` as numpy arrays in input order.

    NaN p-values pass through as NaN / False and do not count toward m.
    """
    p = np.asarray(list(pvals.values()) if isinstance(pvals, dict) else pvals, float)
    adj = np.full(len(p), np.nan)
    valid = np.where(~np.isnan(p))[0]
    m = len(valid)
    order = valid[np.argsort(p[valid])]
    running = 0.0
    for rank, i in enumerate(order):
        running = max(running, min(1.0, (m - rank) * p[i]))
        adj[i] = running
    reject = np.where(np.isnan(adj), False, adj <= alpha)
    return adj, reject


# --------------------------------------------------------------------------- #
# Calendar-time portfolio
# --------------------------------------------------------------------------- #
def calendar_time_alpha(
    events: pd.DataFrame,
    prices: dict[str, pd.DataFrame],
    calendar: pd.DatetimeIndex,
    ff: pd.DataFrame,
    horizon: int = 60,
    start_offset: int = 1,
    min_stocks: int = 5,
    maxlags: int | None = None,
) -> dict:
    """Equal-weight portfolio of stocks within [+start_offset, +horizon] of an event.

    Each day the portfolio holds every stock with at least one active event window (a stock is
    counted once even with overlapping events).  Daily excess return (minus RF) is regressed
    on Mkt-RF, SMB, HML with Newey-West (HAC) standard errors.  Days with fewer than
    ``min_stocks`` stocks are dropped.  Entry at the close of day 0 (day-0 return excluded),
    hence conservative.  ``events`` needs ``ticker`` and ``event_day``.
    """
    import statsmodels.api as sm

    cal = pd.DatetimeIndex(calendar)
    n = len(cal)
    tickers = sorted(set(events["ticker"]) & set(prices))
    if not tickers:
        return {"ok": False, "reason": "no_tickers_with_prices"}
    col = {t: i for i, t in enumerate(tickers)}
    R = np.full((n, len(tickers)), np.nan)
    for t in tickers:
        R[:, col[t]] = _returns(_aligned(prices[t], cal))
    active = np.zeros((n, len(tickers)), bool)
    for t, d in zip(events["ticker"], events["event_day"]):
        if t not in col or pd.isna(d) or d not in cal:
            continue
        p0 = cal.get_loc(d)
        lo, hi = p0 + start_offset, min(p0 + horizon, n - 1)
        if lo <= hi:
            active[lo : hi + 1, col[t]] = True
    use = active & ~np.isnan(R)
    cnt = use.sum(axis=1)
    port = np.where(cnt > 0, np.nansum(np.where(use, R, 0.0), axis=1) / np.maximum(cnt, 1), np.nan)
    ser = pd.DataFrame({"port": port, "n_stocks": cnt}, index=cal)
    f = ff.set_index(pd.to_datetime(ff["date"]).dt.normalize())[["mkt_rf", "smb", "hml", "rf"]]
    d = ser.join(f, how="inner")
    d = d[(d["n_stocks"] >= min_stocks) & d["port"].notna()]
    T = len(d)
    if T < 30:
        return {"ok": False, "reason": f"too_few_days ({T})", "n_days": T}
    y = d["port"] - d["rf"]
    X = sm.add_constant(d[["mkt_rf", "smb", "hml"]])
    L = maxlags if maxlags is not None else int(np.floor(4 * (T / 100) ** (2 / 9)))
    res = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": L})
    a = float(res.params["const"])
    return {
        "ok": True,
        "alpha_daily": a,
        "alpha_annual": a * 252,
        "alpha_t": float(res.tvalues["const"]),
        "alpha_p": float(res.pvalues["const"]),
        "betas": {k: float(res.params[k]) for k in ["mkt_rf", "smb", "hml"]},
        "r2": float(res.rsquared),
        "n_days": T,
        "mean_n_stocks": float(d["n_stocks"].mean()),
        "min_stocks": min_stocks,
        "horizon": horizon,
        "nw_lags": L,
    }


# --------------------------------------------------------------------------- #
# Main driver
# --------------------------------------------------------------------------- #
def _window_status(p0: int, a: int, b: int, n: int, last_valid: int) -> str | None:
    if p0 + a < 1:
        return "window_before_data"
    if p0 + b > n - 1:
        return "window_beyond_calendar"
    return None


def run_event_study(
    events: pd.DataFrame,
    prices: dict[str, pd.DataFrame],
    market: pd.DataFrame,
    windows: Sequence[tuple[int, int]] = DEFAULT_WINDOWS,
    est_window: tuple[int, int] = EST_WINDOW,
    min_est_obs: int = MIN_EST_OBS,
    min_price: float | None = None,
    n_boot: int = 2000,
    seed: int = 0,
    ff: pd.DataFrame | None = None,
    horizon: int = 60,
    min_stocks: int = 5,
) -> dict:
    """Run the full event study.

    ``events``: needs ``ticker`` and either ``event_day`` or ``accepted_at`` (naive UTC).
    Returns ``{"events": tidy per-event DataFrame, "summary": DataFrame (one row per
    model x window), "results": nested dict, "coverage": dict, "calendar_time": dict|None,
    "params": dict}``.

    Per-event columns: ``status`` (``ok`` or drop reason: ``no_event_day``, ``no_price_data``,
    ``price_below_min``, ``short_estimation_window``, ``no_valid_window``), ``beta``, ``alpha``,
    ``n_est``, ``window_notes`` and, for model in {mm (market model), ma (market-adjusted)},
    ``car_<model>_<win>`` and ``scar_<model>_<win>``.  A window is NaN (with a note) when
    prices are missing inside it (``missing_prices``), the series ends before it
    (``delisted_mid_window``) or the calendar does (``window_beyond_calendar``).
    """
    cal = pd.DatetimeIndex(sorted(pd.to_datetime(market["date"]).dt.normalize().unique()))
    n = len(cal)
    rm_all = _returns(_aligned(market, cal))
    ev = events.copy().reset_index(drop=True)
    if "event_day" not in ev.columns:
        ev["event_day"] = [event_day(t, cal) for t in ev["accepted_at"]]
    if "event_id" not in ev.columns:
        ev["event_id"] = [f"ev{i}" for i in range(len(ev))]
    ret_cache: dict[str, tuple[np.ndarray, int]] = {}
    close_cache: dict[str, np.ndarray] = {}
    est_a, est_b = est_window
    rows, drop_reasons = [], {}
    for e in ev.to_dict("records"):
        tk = str(e["ticker"]).upper()
        base = {"event_id": e["event_id"], "ticker": tk, "event_day": e["event_day"], "status": "ok", "window_notes": ""}
        for extra in ("kind", "accepted_at", "n_owners", "total_value"):
            if extra in ev.columns:
                base[extra] = e[extra]

        def drop(reason):
            base["status"] = reason
            drop_reasons[reason] = drop_reasons.get(reason, 0) + 1
            rows.append(base)

        d0 = e["event_day"]
        if d0 is None or pd.isna(d0) or d0 not in cal:
            drop("no_event_day")
            continue
        if tk not in prices or len(prices[tk]) == 0:
            drop("no_price_data")
            continue
        if tk not in ret_cache:
            p = _aligned(prices[tk], cal)
            valid = np.where(~np.isnan(p))[0]
            ret_cache[tk] = (_returns(p), int(valid[-1]) if len(valid) else -1)
            close_cache[tk] = _aligned(prices[tk], cal, "close") if "close" in prices[tk] else p
        r, last_valid = ret_cache[tk]
        p0 = cal.get_loc(d0)
        if min_price is not None:
            px = e["price"] if "price" in ev.columns and pd.notna(e.get("price")) else None
            if px is None:
                c = close_cache[tk][max(0, p0 - 5) : p0]
                c = c[~np.isnan(c)]
                px = c[-1] if len(c) else np.nan
            if not (px >= min_price):
                drop("price_below_min")
                continue
        lo = max(1, p0 + est_a)
        hi = p0 + est_b
        if hi < lo:
            drop("short_estimation_window")
            continue
        mm = fit_market_model(r[lo : hi + 1], rm_all[lo : hi + 1], min_est_obs)
        if mm is None:
            drop("short_estimation_window")
            continue
        base.update(beta=mm.beta, alpha=mm.alpha, n_est=mm.n, resid_sd=np.sqrt(mm.sigma2))
        notes, any_ok = [], False
        for a, b in windows:
            w = wname(a, b)
            reason = _window_status(p0, a, b, n, last_valid)
            sl = slice(p0 + a, p0 + b + 1)
            if reason is None:
                rw, mw = r[sl], rm_all[sl]
                if np.isnan(rw).any() or np.isnan(mw).any():
                    reason = "delisted_mid_window" if p0 + b > last_valid else "missing_prices"
            if reason:
                notes.append(f"{w}:{reason}")
                for m in ("mm", "ma"):
                    base[f"car_{m}_{w}"] = np.nan
                    base[f"scar_{m}_{w}"] = np.nan
                continue
            any_ok = True
            for m, model in (("mm", "market_model"), ("ma", "market_adjusted")):
                car = float(abnormal_returns(rw, mw, mm, model).sum())
                base[f"car_{m}_{w}"] = car
                base[f"scar_{m}_{w}"] = car / np.sqrt(car_variance(mm, mw, model))
        base["window_notes"] = ";".join(notes)
        if not any_ok:
            drop("no_valid_window")
            continue
        rows.append(base)

    tidy = pd.DataFrame(rows)
    results: dict = {"market_model": {}, "market_adjusted": {}}
    summ = []
    for m, mname in (("mm", "market_model"), ("ma", "market_adjusted")):
        for a, b in windows:
            w = wname(a, b)
            if f"car_{m}_{w}" not in tidy:
                continue
            car = tidy[f"car_{m}_{w}"].to_numpy(float)
            scar = tidy[f"scar_{m}_{w}"].to_numpy(float)
            cs = cross_sectional_t(car)
            bm = bmp_test(scar)
            ci = bootstrap_ci(car, n_boot=n_boot, seed=seed)
            c = car[~np.isnan(car)]
            res = {
                "n": cs["n"],
                "mean_car": cs["mean"],
                "median_car": float(np.median(c)) if len(c) else np.nan,
                "share_positive": float((c > 0).mean()) if len(c) else np.nan,
                "t_cs": cs["t"],
                "p_cs": cs["p"],
                "t_bmp": bm["t"],
                "p_bmp": bm["p"],
                "ci_lo": ci[0],
                "ci_hi": ci[1],
            }
            results[mname][w] = res
            summ.append({"model": mname, "window": w, **res})
    n_ok = int((tidy["status"] == "ok").sum()) if len(tidy) else 0
    tickers_req = sorted({str(t).upper() for t in ev["ticker"]})
    coverage = {
        "n_events_input": len(ev),
        "n_events_ok": n_ok,
        "n_dropped": len(ev) - n_ok,
        "drop_reasons": drop_reasons,
        "tickers_requested": len(tickers_req),
        "tickers_with_prices": sum(1 for t in tickers_req if t in prices and len(prices[t])),
        "tickers_missing_prices": [t for t in tickers_req if t not in prices or len(prices[t]) == 0],
        "ok_rate": n_ok / len(ev) if len(ev) else np.nan,
        "window_n": {wname(a, b): int(tidy[f"car_mm_{wname(a, b)}"].notna().sum()) if len(tidy) else 0 for a, b in windows},
    }
    cts = None
    if ff is not None and n_ok:
        okev = tidy[tidy["status"] == "ok"][["ticker", "event_day"]]
        cts = calendar_time_alpha(okev, prices, cal, ff, horizon=horizon, min_stocks=min_stocks)
    return {
        "events": tidy,
        "summary": pd.DataFrame(summ),
        "results": results,
        "coverage": coverage,
        "calendar_time": cts,
        "params": {"windows": list(windows), "est_window": est_window, "min_est_obs": min_est_obs, "min_price": min_price, "n_boot": n_boot, "seed": seed},
    }


# --------------------------------------------------------------------------- #
# Ranking-variant comparison
# --------------------------------------------------------------------------- #
def score_decile_analysis(df: pd.DataFrame, score_col: str, car_col: str, n_bins: int = 10) -> dict:
    """Mean CAR by score bin and top-minus-bottom spread (Welch t-stat).

    Bins are rank-based (ties broken by order) so they are equal-sized.  ``n_bins=10`` gives
    deciles.  Also returns the Spearman rank correlation between score and CAR.
    """
    d = df[[score_col, car_col]].dropna().copy()
    if len(d) < 2 * n_bins:
        return {"ok": False, "reason": f"need >= {2 * n_bins} rows, have {len(d)}", "n": len(d)}
    d["bin"] = pd.qcut(d[score_col].rank(method="first"), n_bins, labels=False)
    tab = d.groupby("bin")[car_col].agg(["count", "mean", "median", "std"]).reset_index()
    top, bot = d[d["bin"] == n_bins - 1][car_col].to_numpy(), d[d["bin"] == 0][car_col].to_numpy()
    spread = float(top.mean() - bot.mean())
    t, p = stats.ttest_ind(top, bot, equal_var=False)
    rho, rho_p = stats.spearmanr(d[score_col], d[car_col])
    return {
        "ok": True,
        "n": len(d),
        "table": tab,
        "top_mean": float(top.mean()),
        "bottom_mean": float(bot.mean()),
        "spread": spread,
        "t": float(t),
        "p": float(p),
        "spearman": float(rho),
        "spearman_p": float(rho_p),
    }
