"""Daily price data layer for the event study (plan step E4).

Endpoints verified 2026-11-13 (see also docs/event_study_prereg.md):

* **Stooq** (``https://stooq.com/q/d/l/?s=spy.us&i=d``): now answers plain HTTP
  clients with an HTML page containing a JavaScript proof-of-work challenge
  ("This site requires JavaScript to verify your browser").  No CSV is returned.
  ``StooqProvider`` therefore raises ``ProviderBlocked`` when it sees HTML; it is
  kept only because it works from a browser-fetched CSV / if the limit is lifted.
  When it works, Stooq ``Close`` is **split-adjusted only** (no dividend
  adjustment), so ``adj_close == close`` for this provider.
* **Yahoo chart JSON** (``query1.finance.yahoo.com/v8/finance/chart``): works
  without an API key.  ``close`` is split-adjusted, ``adj_close`` is split- *and*
  dividend-adjusted (total-return basis).  It is an unofficial endpoint and may be
  rate limited (HTTP 429) or change without notice.
* **Ken French daily factors**: official zip CSV, works without a key.
* ``CSVDirProvider`` reads pre-downloaded ``<TICKER>.csv`` files (offline use).

Returns in the event study are computed from ``adj_close``.
"""
from __future__ import annotations

import io
import json
import re
import time
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

import duckdb
import pandas as pd

USER_AGENT = "insider-intel-research-dev"
PRICE_COLUMNS = ["date", "open", "high", "low", "close", "adj_close", "volume"]
FF_URL = (
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
    "F-F_Research_Data_Factors_daily_CSV.zip"
)


class ProviderError(RuntimeError):
    """Transient / per-ticker failure (HTTP error, parse error)."""


class ProviderBlocked(ProviderError):
    """The provider is refusing automated clients (captcha / JS challenge / rate limit)."""


def _to_ts(d) -> pd.Timestamp:
    return pd.Timestamp(d).normalize()


def empty_prices() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.Series(dtype="datetime64[ns]"),
            **{c: pd.Series(dtype="float64") for c in PRICE_COLUMNS[1:]},
        }
    )


def normalise_prices(df: pd.DataFrame, start=None, end=None) -> pd.DataFrame:
    """Coerce to the canonical schema, sort, drop duplicate dates / NaN closes."""
    if df is None or len(df) == 0:
        return empty_prices()
    out = df.copy()
    out.columns = [str(c).strip().lower().replace(" ", "_") for c in out.columns]
    if "adj_close" not in out.columns:
        out["adj_close"] = out.get("close")
    for c in PRICE_COLUMNS:
        if c not in out.columns:
            out[c] = float("nan")
    out = out[PRICE_COLUMNS]
    out["date"] = pd.to_datetime(out["date"]).dt.normalize()
    for c in PRICE_COLUMNS[1:]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["adj_close"])
    out = out.drop_duplicates("date", keep="last").sort_values("date")
    if start is not None:
        out = out[out["date"] >= _to_ts(start)]
    if end is not None:
        out = out[out["date"] <= _to_ts(end)]
    return out.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Providers
# --------------------------------------------------------------------------- #
class PriceProvider(ABC):
    name = "base"

    @abstractmethod
    def daily(self, ticker: str, start, end) -> pd.DataFrame:
        """Daily bars for ``ticker`` in [start, end] (inclusive).

        Returns DataFrame[date, open, high, low, close, adj_close, volume].
        An unknown ticker returns an *empty* frame; transient problems raise
        ``ProviderError`` (``ProviderBlocked`` when the source refuses clients).
        """


def parse_stooq_csv(text: str) -> pd.DataFrame:
    """Parse Stooq's ``Date,Open,High,Low,Close,Volume`` CSV.

    Stooq prices are split-adjusted but not dividend-adjusted -> adj_close = close.
    Raises ``ProviderBlocked`` for the HTML challenge/captcha page.
    """
    t = (text or "").strip()
    if not t or t.lower().startswith("no data"):
        return empty_prices()
    if t.lstrip().startswith("<") or "<html" in t[:200].lower():
        raise ProviderBlocked("Stooq returned an HTML page (JS challenge/captcha) instead of CSV")
    if "limit" in t[:200].lower() and "date" not in t[:50].lower():
        raise ProviderBlocked(f"Stooq limit message: {t[:100]}")
    df = pd.read_csv(io.StringIO(t))
    df.columns = [c.strip().lower() for c in df.columns]
    if "date" not in df.columns or "close" not in df.columns:
        raise ProviderError(f"unexpected Stooq columns: {list(df.columns)}")
    df["adj_close"] = df["close"]
    return normalise_prices(df)


class StooqProvider(PriceProvider):
    name = "stooq"
    URL = "https://stooq.com/q/d/l/"

    def __init__(self, client=None, apikey: str | None = None, timeout: float = 20.0):
        self._client = client
        self.apikey = apikey
        self.timeout = timeout

    def daily(self, ticker: str, start, end) -> pd.DataFrame:
        import httpx

        params = {
            "s": f"{ticker.lower().replace('-', '.')}.us",
            "i": "d",
            "d1": _to_ts(start).strftime("%Y%m%d"),
            "d2": _to_ts(end).strftime("%Y%m%d"),
        }
        if self.apikey:
            params["apikey"] = self.apikey
        client = self._client or httpx.Client(timeout=self.timeout, headers={"User-Agent": USER_AGENT})
        try:
            r = client.get(self.URL, params=params)
        except httpx.HTTPError as e:
            raise ProviderError(f"stooq request failed: {e}") from e
        if r.status_code != 200:
            raise ProviderError(f"stooq HTTP {r.status_code}")
        return parse_stooq_csv(r.text)


def parse_yahoo_chart(payload: dict) -> pd.DataFrame:
    """Parse a Yahoo v8 chart JSON payload.  Unknown symbol -> empty frame."""
    chart = (payload or {}).get("chart") or {}
    result = chart.get("result")
    err = chart.get("error")
    if not result:
        if err and str(err.get("code", "")).lower() == "not found":
            return empty_prices()
        if err:
            raise ProviderError(f"yahoo error: {err}")
        return empty_prices()
    r = result[0]
    ts = r.get("timestamp") or []
    if not ts:
        return empty_prices()
    quote = r["indicators"]["quote"][0]
    adj = (r["indicators"].get("adjclose") or [{}])[0].get("adjclose") or quote.get("close")
    off = int((r.get("meta") or {}).get("gmtoffset", 0))
    dates = [
        datetime.fromtimestamp(int(t) + off, tz=timezone.utc).date() for t in ts
    ]  # exchange-local calendar date
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(dates),
            "open": quote.get("open"),
            "high": quote.get("high"),
            "low": quote.get("low"),
            "close": quote.get("close"),
            "adj_close": adj,
            "volume": quote.get("volume"),
        }
    )
    return normalise_prices(df)


class YahooChartProvider(PriceProvider):
    """Keyless Yahoo chart endpoint.  Unofficial; may rate-limit."""

    name = "yahoo_chart"
    URL = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}"

    def __init__(self, client=None, timeout: float = 20.0):
        self._client = client
        self.timeout = timeout

    def daily(self, ticker: str, start, end) -> pd.DataFrame:
        import httpx

        sym = ticker.upper().replace(".", "-")
        p1 = int(_to_ts(start).tz_localize("UTC").timestamp())
        p2 = int((_to_ts(end) + pd.Timedelta(days=1)).tz_localize("UTC").timestamp())
        params = {
            "period1": p1,
            "period2": p2,
            "interval": "1d",
            "events": "div,split",
            "includeAdjustedClose": "true",
        }
        client = self._client or httpx.Client(timeout=self.timeout, headers={"User-Agent": USER_AGENT})
        try:
            r = client.get(self.URL.format(sym=sym), params=params)
        except httpx.HTTPError as e:
            raise ProviderError(f"yahoo request failed: {e}") from e
        if r.status_code == 429:
            raise ProviderBlocked("yahoo HTTP 429 (rate limited)")
        if r.status_code == 404:
            return empty_prices()
        if r.status_code != 200:
            raise ProviderError(f"yahoo HTTP {r.status_code}")
        try:
            payload = r.json()
        except ValueError as e:
            raise ProviderError("yahoo returned non-JSON") from e
        return normalise_prices(parse_yahoo_chart(payload), start, end)


class CSVDirProvider(PriceProvider):
    """Reads ``<dir>/<TICKER>.csv`` (Stooq or canonical column layout)."""

    name = "csv_dir"

    def __init__(self, directory):
        self.directory = Path(directory)

    def daily(self, ticker: str, start, end) -> pd.DataFrame:
        p = self.directory / f"{ticker.upper()}.csv"
        if not p.exists():
            p = self.directory / f"{ticker.lower()}.csv"
        if not p.exists():
            return empty_prices()
        df = pd.read_csv(p)
        return normalise_prices(df, start, end)


# --------------------------------------------------------------------------- #
# Parquet helpers (duckdb; pyarrow not required)
# --------------------------------------------------------------------------- #
def _q(path) -> str:
    return str(path).replace("'", "''")


def write_parquet(df: pd.DataFrame, path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    con = duckdb.connect(":memory:")
    try:
        con.register("_df", df)
        con.execute(f"COPY _df TO '{_q(tmp)}' (FORMAT PARQUET)")
    finally:
        con.close()
    tmp.replace(path)


def read_parquet(path) -> pd.DataFrame:
    con = duckdb.connect(":memory:")
    try:
        return con.execute(f"SELECT * FROM read_parquet('{_q(path)}')").df()
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# Cache
# --------------------------------------------------------------------------- #
@dataclass
class PriceCoverage:
    requested: list[str] = field(default_factory=list)
    found: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)  # provider returned no rows
    errors: dict[str, str] = field(default_factory=dict)  # transient failures, not cached
    fetched_requests: int = 0

    def to_dict(self) -> dict:
        n = len(self.requested)
        return {
            "requested": n,
            "found": len(self.found),
            "missing": sorted(self.missing),
            "errors": dict(self.errors),
            "found_rate": (len(self.found) / n) if n else None,
            "provider_requests": self.fetched_requests,
        }


def _safe_name(ticker: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "_", ticker.upper())


class PriceCache:
    """Parquet-per-ticker cache with gap-only refresh and polite rate limiting.

    ``meta.json`` records, per ticker, the date range already requested from the
    provider (so IPO/delisting boundaries are not re-requested on every call).
    Only the parts of [start, end] outside that range are fetched.
    End dates are clamped to yesterday (today's bar is incomplete).
    """

    def __init__(
        self,
        provider: PriceProvider,
        cache_dir="data/prices",
        min_interval: float = 0.5,  # <= 2 requests/second
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        today: date | None = None,
    ):
        self.provider = provider
        self.dir = Path(cache_dir)
        self.min_interval = min_interval
        self._clock, self._sleep = clock, sleep
        self._last = None
        self.today = today
        self.coverage = PriceCoverage()

    # -- meta
    @property
    def _meta_path(self) -> Path:
        return self.dir / "meta.json"

    def _load_meta(self) -> dict:
        try:
            return json.loads(self._meta_path.read_text())
        except (OSError, ValueError):
            return {}

    def _save_meta(self, meta: dict) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        self._meta_path.write_text(json.dumps(meta, indent=1, sort_keys=True))

    def _path(self, ticker: str) -> Path:
        return self.dir / f"{_safe_name(ticker)}.parquet"

    def _throttle(self) -> None:
        now = self._clock()
        if self._last is not None:
            wait = self.min_interval - (now - self._last)
            if wait > 0:
                self._sleep(wait)
                now = self._clock()
        self._last = now

    def _yesterday(self) -> pd.Timestamp:
        t = pd.Timestamp(self.today) if self.today else pd.Timestamp.now().normalize()
        return t.normalize() - pd.Timedelta(days=1)

    # -- core
    @staticmethod
    def missing_ranges(meta_entry: dict | None, start: pd.Timestamp, end: pd.Timestamp):
        if end < start:
            return []
        if not meta_entry:
            return [(start, end)]
        ms, me = _to_ts(meta_entry["start"]), _to_ts(meta_entry["end"])
        out = []
        if start < ms:
            out.append((start, ms - pd.Timedelta(days=1)))
        if end > me:
            out.append((me + pd.Timedelta(days=1), end))
        return out

    def get(self, ticker: str, start, end) -> pd.DataFrame:
        """Prices for one ticker, fetching only missing date ranges."""
        start, end = _to_ts(start), min(_to_ts(end), self._yesterday())
        meta = self._load_meta()
        entry = meta.get(ticker.upper())
        path = self._path(ticker)
        cached = read_parquet(path) if path.exists() else empty_prices()
        frames = [cached] if len(cached) else []
        for s, e in self.missing_ranges(entry, start, end):
            self._throttle()
            self.coverage.fetched_requests += 1
            df = self.provider.daily(ticker, s, e)
            if len(df):
                frames.append(df)
        if frames:
            merged = normalise_prices(pd.concat(frames, ignore_index=True))
        else:
            merged = empty_prices()
        new_entry = {
            "start": str(min(start, _to_ts(entry["start"])).date()) if entry else str(start.date()),
            "end": str(max(end, _to_ts(entry["end"])).date()) if entry else str(end.date()),
            "provider": self.provider.name,
            "rows": int(len(merged)),
            "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        meta[ticker.upper()] = new_entry
        if len(merged):
            write_parquet(merged, path)
        self._save_meta(meta)
        return normalise_prices(merged, start, end)

    def get_many(self, tickers: Iterable[str], start, end) -> dict[str, pd.DataFrame]:
        """Fetch many tickers; records ``self.coverage``. Tickers with no data are omitted."""
        cov = PriceCoverage()
        self.coverage = cov
        out: dict[str, pd.DataFrame] = {}
        for t in sorted({str(x).upper() for x in tickers if x}):
            cov.requested.append(t)
            try:
                df = self.get(t, start, end)
            except ProviderBlocked as e:
                cov.errors[t] = f"blocked: {e}"
                break  # do not hammer a provider that is refusing us
            except ProviderError as e:
                cov.errors[t] = str(e)
                continue
            if len(df):
                out[t] = df
                cov.found.append(t)
            else:
                cov.missing.append(t)
        return out


# --------------------------------------------------------------------------- #
# Fama-French factors
# --------------------------------------------------------------------------- #
def parse_ff_daily_csv(text: str) -> pd.DataFrame:
    """Parse Ken French's ``F-F_Research_Data_Factors_daily.csv``.

    Quirks handled: free-text preamble, header line ``,Mkt-RF,SMB,HML,RF`` with an
    empty first field, values in *percent*, blank line + copyright footer.
    Returns DataFrame[date, mkt_rf, smb, hml, rf] in **decimal** returns.
    """
    rows = []
    started = False
    for line in text.splitlines():
        s = line.strip()
        parts = [p.strip() for p in s.split(",")]
        is_data = len(parts) >= 5 and re.fullmatch(r"\d{8}", parts[0]) is not None
        if is_data:
            started = True
            rows.append(parts[:5])
        elif started:
            break  # footer reached
    if not rows:
        raise ProviderError("no data rows found in Fama-French CSV")
    df = pd.DataFrame(rows, columns=["date", "mkt_rf", "smb", "hml", "rf"])
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    for c in ["mkt_rf", "smb", "hml", "rf"]:
        df[c] = pd.to_numeric(df[c], errors="coerce") / 100.0
    return df.dropna().reset_index(drop=True)


def fama_french_daily(cache_dir="data/prices", max_age_days: float = 30, force: bool = False, client=None) -> pd.DataFrame:
    """Daily FF3 factors (decimal), cached as parquet; refreshed when older than ``max_age_days``."""
    import httpx

    path = Path(cache_dir) / "ff3_daily.parquet"
    if path.exists() and not force:
        age_days = (time.time() - path.stat().st_mtime) / 86400
        if age_days <= max_age_days:
            return read_parquet(path).assign(date=lambda d: pd.to_datetime(d["date"]))
    client = client or httpx.Client(timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True)
    try:
        r = client.get(FF_URL)
    except httpx.HTTPError as e:
        if path.exists():  # stale cache beats nothing
            return read_parquet(path).assign(date=lambda d: pd.to_datetime(d["date"]))
        raise ProviderError(f"Fama-French download failed: {e}") from e
    if r.status_code != 200:
        raise ProviderError(f"Fama-French HTTP {r.status_code}")
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        text = z.read(name).decode("latin-1")
    df = parse_ff_daily_csv(text)
    write_parquet(df, path)
    return df


# --------------------------------------------------------------------------- #
# Calendar
# --------------------------------------------------------------------------- #
def trading_calendar(index_prices: pd.DataFrame) -> pd.DatetimeIndex:
    """Trading days = dates on which the reference index (SPY) has a price."""
    raw = index_prices["date"] if "date" in index_prices.columns else index_prices.index
    idx = pd.DatetimeIndex(pd.to_datetime(raw)).normalize()
    return pd.DatetimeIndex(sorted(idx.unique()))
