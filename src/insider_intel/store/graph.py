"""Deterministic insider network (B6): owners <-> issuers, built straight from Form 4 CIKs. No LLM.

`insider_edges` has one row per (owner CIK, issuer CIK) with role flags, first/last acceptance time,
filing count and open-market (P/S) totals. It is rebuilt from `reporting_owners` + `filings` +
`transactions` after every ingest, so it can never drift from the raw tables.

Value semantics (identical to the ranking): P/S rows only, positive shares and price, rows superseded by a
later 4/A excluded, and the same trade reported twice for the same owner (same issuer, owner-CIK set, date,
code, shares, price) counted once. A joint filing's trade counts toward each of its reporting owners.
`n_filings` counts distinct accessions, amendments included.

Query helpers:
  insider_network(db, owner)            other issuers an owner is an insider at, with activity
  co_insiders(db, issuer, since)        other owners filing at an issuer (optionally only since a date)
Owner lookup tries the CIK first (digits) and otherwise a normalised token-set name match, so
"Kurtz George" == "George Kurtz" == "George R. Kurtz".
"""
from __future__ import annotations

import re
from datetime import date, datetime

import pandas as pd

_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v", "md", "phd", "esq"}
EDGE_COLUMNS = ["owner_cik", "owner_name", "issuer_cik", "issuer_ticker", "issuer_name", "is_director", "is_officer",
                "is_ten_pct", "officer_title", "first_accepted_at", "last_accepted_at", "n_filings", "buy_value",
                "sell_value", "n_buys", "n_sells"]


def normalize_name(name: str | None) -> frozenset[str]:
    """Order-insensitive token set: lower-cased, punctuation removed, suffixes and single-letter initials dropped."""
    toks = re.findall(r"[a-z0-9]+", (name or "").lower())
    return frozenset(t for t in toks if len(t) > 1 and t not in _SUFFIXES)


def _edge_sql(base_where: str = "", txn_where: str = "") -> str:
    """The one SQL definition of an edge. `*_where` fragments (starting with ' AND') narrow the filing / txn sets."""
    return f"""
WITH base AS (
    SELECT ro.owner_cik, ro.owner_name, ro.is_director, ro.is_officer, ro.is_ten_pct, ro.officer_title,
           f.issuer_cik, f.issuer_ticker, f.issuer_name, f.accession_number,
           COALESCE(f.accepted_at, f.filing_date) AS ts
    FROM reporting_owners ro JOIN filings f ON f.accession_number = ro.accession_number
    WHERE f.issuer_cik IS NOT NULL {base_where}
),
latest AS (
    SELECT owner_cik, issuer_cik, owner_name, issuer_ticker, issuer_name
    FROM (SELECT *, row_number() OVER (PARTITION BY owner_cik, issuer_cik ORDER BY ts DESC, accession_number DESC) AS rn
          FROM base) WHERE rn = 1
),
title AS (
    SELECT owner_cik, issuer_cik, officer_title
    FROM (SELECT *, row_number() OVER (PARTITION BY owner_cik, issuer_cik ORDER BY ts DESC, accession_number DESC) AS rn
          FROM base WHERE officer_title IS NOT NULL) WHERE rn = 1
),
agg AS (
    SELECT owner_cik, issuer_cik, MIN(ts) AS first_accepted_at, MAX(ts) AS last_accepted_at,
           COUNT(DISTINCT accession_number) AS n_filings,
           BOOL_OR(COALESCE(is_director, FALSE)) AS is_director, BOOL_OR(COALESCE(is_officer, FALSE)) AS is_officer,
           BOOL_OR(COALESCE(is_ten_pct, FALSE)) AS is_ten_pct
    FROM base GROUP BY owner_cik, issuer_cik
),
txn AS (
    SELECT owner_cik, issuer_cik, code, val FROM (
        SELECT ro.owner_cik, f.issuer_cik, t.code, t.shares * t.price_per_share AS val,
               row_number() OVER (PARTITION BY ro.owner_cik, f.issuer_cik, COALESCE(f.owner_ciks, ''), t.transaction_date,
                                  t.code, ROUND(t.shares, 6), ROUND(t.price_per_share, 6)
                                  ORDER BY t.accession_number, t.row_idx) AS rn
        FROM transactions t
        JOIN filings f ON f.accession_number = t.accession_number
        JOIN reporting_owners ro ON ro.accession_number = t.accession_number
        WHERE t.code IN ('P', 'S') AND t.shares > 0 AND t.price_per_share > 0
          AND COALESCE(t.superseded, FALSE) = FALSE AND f.issuer_cik IS NOT NULL {txn_where}
    ) WHERE rn = 1
),
tagg AS (
    SELECT owner_cik, issuer_cik,
           ROUND(COALESCE(SUM(val) FILTER (WHERE code = 'P'), 0), 2) AS buy_value,
           ROUND(COALESCE(SUM(val) FILTER (WHERE code = 'S'), 0), 2) AS sell_value,
           COUNT(*) FILTER (WHERE code = 'P') AS n_buys, COUNT(*) FILTER (WHERE code = 'S') AS n_sells
    FROM txn GROUP BY owner_cik, issuer_cik
)
SELECT a.owner_cik, l.owner_name, a.issuer_cik, l.issuer_ticker, l.issuer_name,
       a.is_director, a.is_officer, a.is_ten_pct, t.officer_title,
       a.first_accepted_at, a.last_accepted_at, a.n_filings,
       COALESCE(g.buy_value, 0) AS buy_value, COALESCE(g.sell_value, 0) AS sell_value,
       COALESCE(g.n_buys, 0) AS n_buys, COALESCE(g.n_sells, 0) AS n_sells
FROM agg a
JOIN latest l ON l.owner_cik = a.owner_cik AND l.issuer_cik = a.issuer_cik
LEFT JOIN title t ON t.owner_cik = a.owner_cik AND t.issuer_cik = a.issuer_cik
LEFT JOIN tagg g ON g.owner_cik = a.owner_cik AND g.issuer_cik = a.issuer_cik
ORDER BY a.owner_cik, a.issuer_cik
"""


def rebuild_insider_edges(db) -> int:
    """Delete and recompute `insider_edges`; deterministic for a given set of raw rows. Returns the edge count."""
    cols = ", ".join(EDGE_COLUMNS)
    with db.transaction():
        db.execute("DELETE FROM insider_edges")
        db.execute(f"INSERT INTO insider_edges ({cols}) {_edge_sql()}")
    return db.query("SELECT COUNT(*) FROM insider_edges")[0][0]


# ── lookups ─────────────────────────────────────────────────────────────────
def resolve_owner_ciks(db, owner: str) -> list[str]:
    """CIK(s) for an owner given as a CIK (digits) or a name in either word order."""
    owner = (owner or "").strip()
    if not owner:
        return []
    if owner.isdigit():
        cik = owner.lstrip("0") or "0"
        return [cik] if db.query("SELECT 1 FROM insider_edges WHERE owner_cik = ? LIMIT 1", [cik]) else []
    want = normalize_name(owner)
    if not want:
        return []
    rows = db.query("SELECT DISTINCT owner_cik, owner_name FROM insider_edges ORDER BY owner_cik")
    return sorted({cik for cik, name in rows if normalize_name(name) == want})


def _since_ts(since) -> datetime | None:
    if since is None:
        return None
    if isinstance(since, datetime):
        return since
    if isinstance(since, date):
        return datetime(since.year, since.month, since.day)
    return datetime.fromisoformat(str(since))


def insider_network(db, owner: str, exclude_issuer: str | None = None) -> pd.DataFrame:
    """Every issuer where `owner` (CIK or name) is an insider, with role and open-market activity.

    One row per (owner CIK, issuer), newest activity first. Empty frame if the owner is unknown.
    `exclude_issuer` (ticker or CIK) drops the issuer the caller is already looking at ("other companies").
    """
    ciks = resolve_owner_ciks(db, owner)
    if not ciks:
        return pd.DataFrame(columns=EDGE_COLUMNS)
    ph = ",".join("?" * len(ciks))
    sql = f"SELECT {', '.join(EDGE_COLUMNS)} FROM insider_edges WHERE owner_cik IN ({ph})"
    params: list = list(ciks)
    if exclude_issuer:
        sql += " AND NOT (issuer_ticker = ? OR issuer_cik = ?)"
        params += [exclude_issuer.strip().upper(), exclude_issuer.strip().lstrip("0")]
    sql += " ORDER BY last_accepted_at DESC, issuer_cik"
    return db.query_df(sql, params)


def resolve_issuer_ciks(db, issuer: str) -> list[str]:
    """Issuer CIK(s) from a ticker, a CIK, or (case-insensitive substring of) a name."""
    issuer = (issuer or "").strip()
    if not issuer:
        return []
    if issuer.isdigit():
        return [issuer.lstrip("0") or "0"]
    rows = db.query("SELECT DISTINCT issuer_cik FROM insider_edges WHERE issuer_ticker = ?", [issuer.upper()])
    if not rows:
        rows = db.query("SELECT DISTINCT issuer_cik FROM insider_edges WHERE lower(issuer_name) LIKE ?",
                        [f"%{issuer.lower()}%"])
    return sorted(r[0] for r in rows)


def co_insiders(db, issuer: str, since=None, exclude_owner: str | None = None) -> pd.DataFrame:
    """Owners filing at `issuer` (ticker / CIK / name), with their activity there since `since`.

    Computed from the raw tables with the same edge definition as `insider_edges`, restricted to filings
    accepted at or after `since` (None = all time). Sorted by gross open-market dollars, largest first.
    `exclude_owner` (CIK or name) drops the person the caller is already looking at.
    """
    ciks = resolve_issuer_ciks(db, issuer)
    if not ciks:
        return pd.DataFrame(columns=EDGE_COLUMNS)
    ph = ",".join("?" * len(ciks))
    base_where, txn_where = f" AND f.issuer_cik IN ({ph})", f" AND f.issuer_cik IN ({ph})"
    params: list = list(ciks)
    ts = _since_ts(since)
    if ts is not None:
        base_where += " AND COALESCE(f.accepted_at, f.filing_date) >= ?"
        txn_where += " AND COALESCE(f.accepted_at, f.filing_date) >= ?"
        params = params + [ts] + params + [ts]
    else:
        params = params + params
    df = db.query_df(_edge_sql(base_where, txn_where), params)
    if exclude_owner:
        skip = set(resolve_owner_ciks(db, exclude_owner))
        df = df[~df["owner_cik"].isin(skip)]
    if df.empty:
        return df.reset_index(drop=True)
    df = df.assign(_gross=df["buy_value"] + df["sell_value"])
    return df.sort_values(["_gross", "owner_cik"], ascending=[False, True]).drop(columns="_gross").reset_index(drop=True)
