"""Historical event-study inputs from the SEC Insider Transactions Data Sets (plan E4, "E1 loader" route).

What differs from the live pipeline (all deliberate, all documented in docs/event_study_prereg.md):

* **Information date.**  The official datasets carry no EDGAR acceptance time, only FILING_DATE.  Each
  filing is stamped at 23:00 UTC on its FILING_DATE (= 18:00/19:00 US/Eastern, same Eastern date, so always
  after the 16:00 close).  ``market.event_study.event_day`` then yields the first trading session strictly
  AFTER the filing date, i.e. up to one session later than the live definition for filings accepted before
  the close.  Conservative: the first day's reaction is excluded.
* **10b5-1 flag.**  ``SUBMISSION.AFF10B5ONE`` ("1"/"0") when the column exists and the value is non-blank;
  otherwise ``None`` (unknown, NEVER False).  ``build_events`` excludes unknown-status sells from both sell
  kinds, so the discretionary-vs-planned hypothesis (H3) only has data from quarters where the Form 4
  checkbox exists.  No footnote-text fallback is used.
* **Form 4/A** amendments are excluded from the primary build (the original already carries the trade);
  their count is reported.
* Rows are collapsed by ``market.event_study`` per (ticker, accepted_at): same-day filings of one issuer
  form ONE candidate event (this also merges several insiders filing the same day).
* **Titles** of all reporting owners of an accession are joined with " | ", so the CEO/CFO regex matches if
  ANY owner of a (possibly joint) filing is such an officer.

Per-quarter reduced rows (P/S only) are cached as parquet so the multi-GB quarterly extractions are only
needed once.  Nothing here deletes anything.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

import pandas as pd

from .sec_datasets import _d, download_quarter, load_quarter, norm_cik, norm_owner_ciks, norm_ticker
from .ticker_map import map_ciks, most_common_filing_ticker

INFO_HOUR_UTC = 23  # 23:00 UTC is 18:00 (EST) / 19:00 (EDT) on the same Eastern calendar date
ROW_COLUMNS = [
    "accession_number",
    "form_type",
    "issuer_cik",
    "issuer_ticker",
    "owner_ciks",
    "officer_title",
    "filing_date",
    "aff_10b5_1",
    "transaction_date",
    "code",
    "shares",
    "price_per_share",
]


def parse_aff(v) -> bool | None:
    if v is None or (isinstance(v, float) and v != v):
        return None
    s = str(v).strip().lower()
    if s in ("1", "true", "y", "yes", "x"):
        return True
    if s in ("0", "false", "n", "no"):
        return False
    return None


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _cols(con, table: str) -> set[str]:
    try:
        return {str(r[0]).upper() for r in con.execute(f"DESCRIBE {table}").fetchall()}
    except Exception:
        return set()


def extract_quarter_rows(con) -> tuple[pd.DataFrame, dict]:
    """Reduced P/S non-derivative rows of Form 4 and 4/A with filing date, 10b5-1 flag, owners and titles."""
    sub_cols, own_cols = _cols(con, "raw_submission"), _cols(con, "raw_reportingowner")
    have_trans = bool(_cols(con, "nonderiv_trans"))
    info = {"aff10b5one_column_present": "AFF10B5ONE" in sub_cols, "n_load_rejects": 0}
    try:
        info["n_load_rejects"] = int(
            con.execute("SELECT coalesce(sum(rejected_rows),0) FROM load_rejects").fetchone()[0]
        )
    except Exception:
        pass
    if not have_trans or "ACCESSION_NUMBER" not in sub_cols:
        return pd.DataFrame(columns=ROW_COLUMNS), info
    aff = "NULLIF(trim(s.AFF10B5ONE),'')" if "AFF10B5ONE" in sub_cols else "NULL::VARCHAR"
    title = "NULLIF(trim(RPTOWNER_TITLE),'')" if "RPTOWNER_TITLE" in own_cols else "NULL::VARCHAR"
    owners = (
        f"SELECT ACCESSION_NUMBER AS acc, string_agg(trim(RPTOWNERCIK), '|') AS owners, "
        f"string_agg({title}, ' | ') AS titles FROM raw_reportingowner GROUP BY 1"
        if "RPTOWNERCIK" in own_cols
        else "SELECT NULL::VARCHAR AS acc, NULL::VARCHAR AS owners, NULL::VARCHAR AS titles WHERE false"
    )
    df = con.execute(f"""
        WITH o AS ({owners})
        SELECT t.ACCESSION_NUMBER AS accession_number, upper(trim(s.DOCUMENT_TYPE)) AS form_type,
               s.ISSUERCIK AS issuer_cik, s.ISSUERTRADINGSYMBOL AS issuer_ticker,
               o.owners AS owner_ciks, o.titles AS officer_title,
               {_d("s.FILING_DATE")} AS filing_date, {aff} AS aff_raw,
               t.trans_date AS transaction_date, upper(trim(t.trans_code)) AS code,
               t.trans_shares AS shares, t.trans_price AS price_per_share
        FROM nonderiv_trans t
        JOIN raw_submission s ON s.ACCESSION_NUMBER = t.ACCESSION_NUMBER
        LEFT JOIN o ON o.acc = t.ACCESSION_NUMBER
        WHERE upper(trim(t.trans_code)) IN ('P','S') AND upper(trim(s.DOCUMENT_TYPE)) IN ('4','4/A')
    """).df()
    if df.empty:
        return pd.DataFrame(columns=ROW_COLUMNS), info
    uo = {v: norm_owner_ciks(v) for v in df["owner_ciks"].dropna().unique()}
    df["owner_ciks"] = df["owner_ciks"].map(uo)
    df["issuer_cik"] = df["issuer_cik"].map(norm_cik)
    df["issuer_ticker"] = df["issuer_ticker"].map(norm_ticker)
    df["aff_10b5_1"] = pd.Series([parse_aff(v) for v in df.pop("aff_raw")], index=df.index, dtype=object)
    df["filing_date"] = pd.to_datetime(df["filing_date"])
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["accession_number"] = df["accession_number"].astype(str).str.strip()
    return df[ROW_COLUMNS].reset_index(drop=True), info


# --------------------------------------------------------------------------- per-quarter cache
def _paths(year: int, q: int, data_dir: Path, cache_dir: Path) -> tuple[Path, Path, Path]:
    return (data_dir / f"{year}q{q}_form345.zip", cache_dir / f"{year}q{q}.parquet", cache_dir / f"{year}q{q}.json")


def quarter_status(year: int, q: int, data_dir="data/sec_datasets", cache_dir="data/cache/event_rows") -> str:
    """'cached' (reduced rows available, no network), 'zip' (zip on disk) or 'missing' (needs a download)."""
    import zipfile

    zp, pq, js = _paths(year, q, Path(data_dir), Path(cache_dir))
    if pq.exists() and js.exists():
        return "cached"
    return "zip" if zp.exists() and zipfile.is_zipfile(zp) else "missing"


def load_quarter_rows(
    year: int,
    q: int,
    data_dir="data/sec_datasets",
    cache_dir="data/cache/event_rows",
    downloader: Callable | None = None,
) -> tuple[pd.DataFrame, dict]:
    from ..market.prices import read_parquet, write_parquet

    data_dir, cache_dir = Path(data_dir), Path(cache_dir)
    zp, pq, js = _paths(year, q, data_dir, cache_dir)
    if pq.exists() and js.exists():
        df = read_parquet(pq)
        df["filing_date"] = pd.to_datetime(df["filing_date"])
        df["transaction_date"] = pd.to_datetime(df["transaction_date"])
        df["aff_10b5_1"] = pd.Series(
            [None if pd.isna(v) else bool(v) for v in df["aff_10b5_1"]], index=df.index, dtype=object
        )
        return df, json.loads(js.read_text())
    zpath = (downloader or download_quarter)(year, q, data_dir)
    con = load_quarter(zpath)
    try:
        df, info = extract_quarter_rows(con)
    finally:
        con.close()
    info = {**info, "quarter": f"{year}q{q}", "zip_sha256": _sha256_file(Path(zpath)), "n_rows": int(len(df))}
    cache_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(df, pq)
    js.write_text(json.dumps(info))
    return df, info


# --------------------------------------------------------------------------- event rows
def info_timestamp(filing_date: pd.Series) -> pd.Series:
    """FILING_DATE -> naive-UTC 'accepted_at' stamp at 23:00 UTC (after the close on the same Eastern date)."""
    return pd.to_datetime(filing_date).dt.normalize() + pd.Timedelta(hours=INFO_HOUR_UTC)


def build_event_rows(
    quarter_frames: list[pd.DataFrame],
    sec_tickers: dict[str, list[str]] | None = None,
    quarter_infos: list[dict] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Concatenate quarters -> rows for ``market.event_study.build_events`` + a funnel/coverage dict.

    Output columns add ``accepted_at`` (stamp above), ``is_10b5_1`` (True/False/None), ``filing_ticker`` and
    replace ``issuer_ticker`` by the price ticker chosen via the CIK map (``ticker_source`` recorded).
    """
    frames = [f for f in quarter_frames if len(f)]
    rows = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=ROW_COLUMNS)
    funnel: dict = {"rows_ps_form4_and_4a": int(len(rows))}
    amend = rows["form_type"] == "4/A"
    funnel["rows_4A_excluded"] = int(amend.sum())
    rows = rows[~amend]
    nofd = rows["filing_date"].isna()
    funnel["rows_no_filing_date_dropped"] = int(nofd.sum())
    rows = rows[~nofd].copy()
    rows["accepted_at"] = info_timestamp(rows["filing_date"])
    rows["is_10b5_1"] = rows["aff_10b5_1"]

    # ticker mapping via issuer CIK
    ft = most_common_filing_ticker(zip(rows["issuer_cik"], rows["issuer_ticker"]))
    cmap, cov = map_ciks(ft, sec_tickers or {})
    rows["filing_ticker"] = rows["issuer_ticker"]
    rows["issuer_ticker"] = rows["issuer_cik"].map(lambda c: (cmap.get(c) or {}).get("ticker"))
    rows["ticker_source"] = rows["issuer_cik"].map(lambda c: (cmap.get(c) or {}).get("source", "unmapped"))
    funnel["rows_no_ticker_dropped"] = int(rows["issuer_ticker"].isna().sum())
    rows = rows[rows["issuer_ticker"].notna()].reset_index(drop=True)
    funnel["rows_for_event_building"] = int(len(rows))
    funnel["n_filings"] = int(rows["accession_number"].nunique())

    sells = rows[rows["code"] == "S"]
    aff_known = sells["is_10b5_1"].notna()
    funnel["sell_rows"] = int(len(sells))
    funnel["sell_rows_with_10b5_1_flag"] = int(aff_known.sum())
    funnel["sell_rows_10b5_1_unknown_excluded_from_sell_kinds"] = int((~aff_known).sum())
    by_q = {}
    if len(sells):
        q = sells["filing_date"].dt.to_period("Q").astype(str)
        for k, g in sells.groupby(q):
            by_q[k] = {"sell_rows": int(len(g)), "with_flag": int(g["is_10b5_1"].notna().sum())}
    return rows, {
        "funnel": funnel,
        "ticker_mapping": cov,
        "aff10b5one_by_filing_quarter": by_q,
        "quarters": quarter_infos or [],
        "info_timestamp_rule": f"FILING_DATE {INFO_HOUR_UTC}:00 UTC (after the close, same Eastern date)",
    }
