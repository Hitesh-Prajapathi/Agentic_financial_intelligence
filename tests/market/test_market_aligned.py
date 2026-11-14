"""_aligned was rewritten for speed (numpy reindex, no per-call pd.to_datetime on datetime64 columns);
it must match the original Series-based implementation on messy frames."""
import numpy as np
import pandas as pd
import pytest

from insider_intel.market.event_study import _aligned


def _reference(df, cal, col="adj_close"):
    d = pd.to_datetime(df["date"]).dt.normalize()
    s = pd.Series(pd.to_numeric(df[col], errors="coerce").to_numpy(), index=d.to_numpy())
    s = s[~s.index.duplicated(keep="last")]
    s = s.where(s > 0)
    return s.reindex(cal).to_numpy(dtype=float)


@pytest.mark.parametrize("variant", ["ts", "str", "ts_with_time"])
@pytest.mark.parametrize("col", ["adj_close", "close"])
def test_aligned_matches_reference_on_messy_frame(variant, col):
    cal = pd.bdate_range("2020-01-01", periods=50)
    dates = list(cal[:10]) + [cal[3], cal[3]] + [pd.Timestamp("2019-12-01"), pd.Timestamp("2030-01-01")] + list(cal[10:30])
    px = np.arange(len(dates), dtype=float) + 1
    px[2], px[5], px[7], px[11] = 0, -3, np.nan, np.nan
    if variant == "ts":
        dd = pd.Series(pd.to_datetime(dates))
    elif variant == "str":
        dd = pd.Series([d.strftime("%Y-%m-%d") for d in dates])
    else:
        dd = pd.Series(pd.to_datetime(dates)) + pd.Timedelta(hours=5)
    df = pd.DataFrame({"date": dd, "adj_close": px, "close": px * 2})
    assert np.array_equal(_aligned(df, cal, col), _reference(df, cal, col), equal_nan=True)
