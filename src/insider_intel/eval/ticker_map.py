"""Issuer CIK -> price ticker mapping for the historical event study.

Source: SEC ``company_tickers.json`` (CURRENT ticker list; delisted / acquired issuers are absent).
The shape is parsed tolerantly (dict-of-dicts, list-of-dicts, or ``{"fields": [...], "data": [...]}``).
Rule per CIK:
  1. filing ticker is among the CIK's current tickers -> keep it (preserves share class, e.g. GOOG vs GOOGL)
  2. else the first current ticker listed for the CIK (SEC orders by market cap)
  3. else the filing ticker (``filing_fallback``; often a delisted name that the price source lacks)
  4. else unmapped.
``ticker_source`` is recorded per CIK so coverage can be reported by source.
"""
from __future__ import annotations

import json
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from .sec_datasets import norm_cik, norm_ticker

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def parse_company_tickers(payload: Any) -> dict[str, list[str]]:
    """-> {unpadded cik: [tickers in file order]}."""
    rows: list[tuple[Any, Any]] = []
    if isinstance(payload, dict) and "data" in payload and "fields" in payload:
        f = [str(x).lower() for x in payload["fields"]]
        ci = next((i for i, n in enumerate(f) if n.startswith("cik")), None)
        ti = f.index("ticker") if "ticker" in f else None
        if ci is None or ti is None:
            raise ValueError(f"unrecognised company_tickers fields: {payload['fields']}")
        rows = [(r[ci], r[ti]) for r in payload["data"]]
    else:
        items = payload.values() if isinstance(payload, dict) else payload
        for it in items:
            if not isinstance(it, dict):
                continue
            c = it.get("cik_str", it.get("cik"))
            rows.append((c, it.get("ticker")))
    out: dict[str, list[str]] = {}
    for c, t in rows:
        ck, tk = norm_cik(c), norm_ticker(t)
        if ck and tk and tk not in out.setdefault(ck, []):
            out[ck].append(tk)
    return out


def load_company_tickers(cache_path: str | Path = "data/cache/company_tickers.json", client=None,
                         max_age_days: float = 30.0, force: bool = False) -> dict[str, list[str]]:
    """Cached load. ``client`` is an edgar.client.EdgarClient (needs SEC_USER_AGENT); only used on a cache miss."""
    p = Path(cache_path)
    if p.exists() and not force and (time.time() - p.stat().st_mtime) / 86400 <= max_age_days:
        return parse_company_tickers(json.loads(p.read_text()))
    if client is None:
        if p.exists():  # stale beats nothing when we cannot fetch
            return parse_company_tickers(json.loads(p.read_text()))
        raise RuntimeError("company_tickers.json not cached and no EDGAR client supplied")
    payload = client.get_json(COMPANY_TICKERS_URL)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload))
    return parse_company_tickers(payload)


def map_ciks(filing_tickers: dict[str, str | None], sec: dict[str, list[str]]) -> tuple[dict[str, dict], dict]:
    """filing_tickers: {cik: ticker as filed (or None)}. Returns ({cik: {ticker, source, changed}}, coverage)."""
    out: dict[str, dict] = {}
    for cik, ft in filing_tickers.items():
        ft = norm_ticker(ft)
        cur = sec.get(cik) or []
        if ft and ft in cur:
            tk, src = ft, "cik_current"
        elif cur:
            tk, src = cur[0], "cik_current"
        elif ft:
            tk, src = ft, "filing_fallback"
        else:
            tk, src = None, "unmapped"
        out[cik] = {"ticker": tk, "source": src, "changed": bool(ft and tk and ft != tk), "filing_ticker": ft}
    n = len(out)
    src_counts = Counter(v["source"] for v in out.values())
    cov = {
        "n_ciks": n,
        "by_source": dict(src_counts),
        "share_current": (src_counts.get("cik_current", 0) / n) if n else None,
        "n_ticker_changed_vs_filing": sum(v["changed"] for v in out.values()),
        "note": ("'filing_fallback' CIKs are absent from SEC's current ticker list (typically delisted/acquired "
                 "issuers); their filing ticker may be missing from the price source or reassigned to another company."),
    }
    return out, cov


def most_common_filing_ticker(rows: Iterable[tuple[str, str | None]]) -> dict[str, str | None]:
    """(cik, ticker) pairs -> {cik: most frequent non-null ticker}."""
    c: dict[str, Counter] = {}
    for cik, tk in rows:
        c.setdefault(cik, Counter())
        if tk:
            c[cik][tk] += 1
    return {k: (v.most_common(1)[0][0] if v else None) for k, v in c.items()}
