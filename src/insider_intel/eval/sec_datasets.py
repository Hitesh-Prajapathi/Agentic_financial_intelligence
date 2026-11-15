"""Loader for the SEC "Insider Transactions Data Sets" (quarterly Form 3/4/5 structured data).

URL pattern (as documented on the sec.gov landing page; NOT verified live from the build
environment, which got HTTP 403 for the automated request):
    https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{year}q{q}_form345.zip

Each zip holds tab-separated files (header row, ``.tsv``): SUBMISSION, REPORTINGOWNER,
NONDERIV_TRANS, NONDERIV_HOLDING, DERIV_TRANS, DERIV_HOLDING, FOOTNOTES, OWNER_SIGNATURE,
plus a readme. Dates are ``DD-MON-YYYY`` (e.g. ``03-JAN-2025``).

Row order within an accession: NONDERIV_TRANS_SK is the documented surrogate key of a
non-derivative transaction row. We treat it as the ordering key (numeric ascending, file
order as tie-break) and set ``row_idx`` = 0-based rank within the accession. The
assumption (SK ascending == order in the filing XML) is taken from the readme wording and
should be re-checked against a real quarter; the scorer does not rely on it (it matches
rows by content, with row_idx only as tie-break).
"""

from __future__ import annotations

import os
import time
import zipfile
from pathlib import Path

import duckdb
import pandas as pd

URL_TEMPLATE = "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{year}q{q}_form345.zip"
DEFAULT_UA = "insider-intel-research-dev (open-source research tool; contact via repository)"
TABLES = [
    "SUBMISSION",
    "REPORTINGOWNER",
    "NONDERIV_TRANS",
    "NONDERIV_HOLDING",
    "DERIV_TRANS",
    "DERIV_HOLDING",
    "FOOTNOTES",
    "OWNER_SIGNATURE",
]

OFFICIAL_COLUMNS = [
    "accession_number",
    "row_idx",
    "form_type",
    "issuer_cik",
    "issuer_ticker",
    "owner_ciks",
    "transaction_date",
    "code",
    "shares",
    "price_per_share",
    "acquired_disposed",
    "post_transaction_shares",
    "direct_indirect",
    "security_title",
]


# ------------------------------------------------------------------ normalisers (shared with scorer)
def norm_cik(v) -> str | None:
    """CIK as str without leading zeros; blank/NaN -> None."""
    if v is None or (isinstance(v, float) and v != v):
        return None
    s = str(v).strip()
    if s.lower() in ("", "nan", "none", "null"):
        return None
    s = s.split(".")[0] if s.replace(".", "", 1).isdigit() else s
    s = s.lstrip("0")
    return s or "0"


NO_TICKER_PLACEHOLDERS = {"", "NONE", "N/A", "NA", "-"}


def norm_ticker(v) -> str | None:
    if v is None or (isinstance(v, float) and v != v):
        return None
    s = str(v).strip().upper()
    # filers write placeholders instead of leaving the symbol blank (2025Q3: NONE 699, N/A 182, '-' 17, NA 9)
    return None if s in NO_TICKER_PLACEHOLDERS else s


def norm_owner_ciks(v) -> str | None:
    """Sorted, de-duplicated '|'-joined normalised CIKs. Accepts '|'-str or list."""
    if v is None or (isinstance(v, float) and v != v):
        return None
    parts = v if isinstance(v, (list, tuple, set)) else str(v).split("|")
    ciks = sorted({c for c in (norm_cik(p) for p in parts) if c})
    return "|".join(ciks) if ciks else None


# ------------------------------------------------------------------ download
def quarter_url(year: int, q: int) -> str:
    return URL_TEMPLATE.format(year=year, q=q)


def download_quarter(
    year: int, q: int, dest_dir: str | Path = "data/sec_datasets", force: bool = False, timeout: float = 120.0
) -> Path:
    """Download one quarterly zip (cached). One request, polite UA. Raises RuntimeError on 403 etc."""
    import httpx

    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / f"{year}q{q}_form345.zip"
    if path.exists() and not force and zipfile.is_zipfile(path):
        return path
    ua = os.environ.get("SEC_USER_AGENT") or DEFAULT_UA
    tmp = path.with_suffix(".part")
    with httpx.stream(
        "GET",
        quarter_url(year, q),
        headers={"User-Agent": ua, "Accept-Encoding": "gzip, deflate"},
        timeout=timeout,
        follow_redirects=True,
    ) as r:
        if r.status_code != 200:
            raise RuntimeError(f"SEC download failed: HTTP {r.status_code} for {quarter_url(year, q)}")
        with open(tmp, "wb") as f:
            for chunk in r.iter_bytes():
                f.write(chunk)
    if not zipfile.is_zipfile(tmp):
        tmp.unlink(missing_ok=True)
        raise RuntimeError("SEC download did not return a zip (blocked or HTML error page)")
    tmp.replace(path)
    time.sleep(0.2)
    return path


# ------------------------------------------------------------------ load
def _extract(path: Path) -> Path:
    if path.is_dir():
        return path
    out = path.parent / path.stem
    marker = out / ".extracted"
    if not marker.exists():
        out.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(path) as z:
            for info in z.infolist():
                name = Path(info.filename).name
                if info.is_dir() or not name:
                    continue
                # flatten: keep only the basename, never trust member paths
                (out / name).write_bytes(z.read(info))
        marker.write_text("ok")
    return out


def load_quarter(path: str | Path) -> duckdb.DuckDBPyConnection:
    """Load a quarter zip (or extracted dir) into an in-memory DuckDB.

    Raw tables are all-varchar: ``raw_<table>`` with an ``_rn`` file-order column.
    Typed views: ``submission``, ``reportingowner``, ``nonderiv_trans`` (+ the rest typed as varchar).
    """
    d = _extract(Path(path))
    files = {
        p.stem.upper(): p for p in d.iterdir() if p.suffix.lower() in (".tsv", ".txt") and p.stem.upper() in TABLES
    }
    con = duckdb.connect(":memory:")
    # Ground truth must never lose rows silently: malformed lines go to a rejects table and are counted in
    # `load_rejects`, which callers (and the parse-accuracy report) must surface.
    con.execute("CREATE TABLE load_rejects (table_name VARCHAR, rejected_rows BIGINT)")
    for name, p in files.items():
        t = name.lower()
        con.execute(
            f"CREATE TABLE raw_{t} AS SELECT row_number() OVER () AS _rn, * FROM "
            f"read_csv(?, delim='\t', header=true, all_varchar=true, quote='', escape='', null_padding=false, "
            f"store_rejects=true, rejects_table='rejects_{t}', rejects_scan='rejects_scan_{t}')",
            [str(p)],
        )
        # DuckDB can log one bad line more than once (one entry per error it finds), so count distinct lines.
        n_rej = con.execute(f"SELECT COUNT(DISTINCT line) FROM rejects_{t}").fetchone()[0]
        con.execute("INSERT INTO load_rejects VALUES (?, ?)", [name, n_rej])
    have = {n.lower() for n in files}
    for name in TABLES:
        if name.lower() not in have:
            continue
        if name == "NONDERIV_TRANS":
            continue
        con.execute(f"CREATE VIEW {name.lower()} AS SELECT * FROM raw_{name.lower()}")
    if "nonderiv_trans" in have:
        con.execute(_NONDERIV_VIEW)
    return con


def _d(col: str) -> str:
    return f"coalesce(try_strptime(NULLIF(trim({col}),''), '%d-%b-%Y'), try_strptime(NULLIF(trim({col}),''), '%Y-%m-%d'))::DATE"


def _n(col: str) -> str:
    return f"TRY_CAST(NULLIF(trim({col}),'') AS DOUBLE)"


_NONDERIV_VIEW = f"""
CREATE VIEW nonderiv_trans AS
SELECT _rn,
       ACCESSION_NUMBER,
       TRY_CAST(NULLIF(trim(NONDERIV_TRANS_SK),'') AS BIGINT) AS sk,
       NULLIF(trim(SECURITY_TITLE),'') AS security_title,
       {_d("TRANS_DATE")} AS trans_date,
       NULLIF(trim(TRANS_CODE),'') AS trans_code,
       {_n("TRANS_SHARES")} AS trans_shares,
       {_n("TRANS_PRICEPERSHARE")} AS trans_price,
       NULLIF(trim(TRANS_ACQUIRED_DISP_CD),'') AS acquired_disposed,
       {_n("SHRS_OWND_FOLWNG_TRANS")} AS post_shares,
       NULLIF(trim(DIRECT_INDIRECT_OWNERSHIP),'') AS direct_indirect
FROM raw_nonderiv_trans
"""


def official_rows(con: duckdb.DuckDBPyConnection, accessions: list[str] | None = None) -> pd.DataFrame:
    """One row per non-derivative transaction in the official data, with OFFICIAL_COLUMNS schema."""
    df = con.execute(
        """
        SELECT t.ACCESSION_NUMBER AS accession_number, t.sk, t._rn,
               s.DOCUMENT_TYPE AS form_type, s.ISSUERCIK AS issuer_cik, s.ISSUERTRADINGSYMBOL AS issuer_ticker,
               t.trans_date AS transaction_date, t.trans_code AS code, t.trans_shares AS shares,
               t.trans_price AS price_per_share, t.acquired_disposed,
               t.post_shares AS post_transaction_shares, t.direct_indirect, t.security_title
        FROM nonderiv_trans t
        LEFT JOIN raw_submission s ON s.ACCESSION_NUMBER = t.ACCESSION_NUMBER
        """
    ).df()
    if df.empty:
        return pd.DataFrame(columns=OFFICIAL_COLUMNS)
    owners = con.execute("SELECT ACCESSION_NUMBER AS a, RPTOWNERCIK AS c FROM raw_reportingowner").df()
    omap: dict[str, str | None] = {}
    for a, grp in owners.groupby("a"):
        omap[a] = norm_owner_ciks(list(grp["c"]))
    df["owner_ciks"] = df["accession_number"].map(omap)
    df["accession_number"] = df["accession_number"].astype(str).str.strip()
    df["form_type"] = df["form_type"].map(lambda v: None if v is None or str(v).strip() == "" else str(v).strip())
    df["issuer_cik"] = df["issuer_cik"].map(norm_cik)
    df["issuer_ticker"] = df["issuer_ticker"].map(norm_ticker)
    df["transaction_date"] = pd.Series(
        [None if pd.isna(x) else pd.Timestamp(x).date() for x in df["transaction_date"]], index=df.index, dtype=object
    )
    df["sk"] = df["sk"].fillna(float("inf"))
    df = df.sort_values(["accession_number", "sk", "_rn"], kind="mergesort")
    df["row_idx"] = df.groupby("accession_number").cumcount()
    if accessions is not None:
        df = df[df["accession_number"].isin(set(accessions))]
    df = df[OFFICIAL_COLUMNS].reset_index(drop=True)
    return df
