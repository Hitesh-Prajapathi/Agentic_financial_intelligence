"""Synthetic market/stock generators for the offline event-study tests."""

import numpy as np
import pandas as pd


def make_calendar(n=1500, start="2015-01-01"):
    return pd.bdate_range(start, periods=n)


def make_market(cal, seed=0, mu=0.0004, sd=0.01):
    rng = np.random.default_rng(seed)
    rm = rng.normal(mu, sd, len(cal))
    rm[0] = 0.0
    return rm


def prices_df(cal, r, p0=50.0):
    p = p0 * np.cumprod(1 + r)
    return pd.DataFrame({"date": cal, "open": p, "high": p, "low": p, "close": p, "adj_close": p, "volume": 1e6})


def market_df(cal, rm):
    return prices_df(cal, rm, 100.0)


def make_stock(cal, rm, rng, beta=1.0, sd=0.015, inject_at=None, inject=None, alpha=0.0):
    """inject: dict {offset: extra daily return}, applied relative to position inject_at."""
    r = alpha + beta * rm + rng.normal(0, sd, len(cal))
    r[0] = 0.0
    if inject_at is not None and inject:
        for k, v in inject.items():
            r[inject_at + k] += v
    return r
