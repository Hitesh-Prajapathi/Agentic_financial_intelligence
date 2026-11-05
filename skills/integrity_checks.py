"""Governance gate: deterministic integrity checks run BEFORE ranked data is published.

Each check recomputes something independently of the stage that produced it (a different SQL
path, not the same pandas code), so a bug or corrupted input is caught instead of rubber-stamped.
A run failing any check is quarantined: its ranking is never written and the Learning Loop never
sees it. Zero LLM calls.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from agents.symbolic_validation import compute_checksum

logger = logging.getLogger(__name__)
TOL = 1.0  # dollars; covers float rounding over many rows


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


def check_ingest_reconciliation(db, accessions: list[str], expected_txns: int) -> CheckResult:
    """Everything parsed in this run must be present in the database - nothing silently dropped."""
    if not accessions:
        return CheckResult("ingest_reconciliation", True, "no new filings this run")
    ph = ",".join("?" * len(accessions))
    n_f = db.query(f"SELECT COUNT(*) FROM filings WHERE accession_number IN ({ph})", accessions)[0][0]
    n_t = db.query(f"SELECT COUNT(*) FROM transactions WHERE accession_number IN ({ph})", accessions)[0][0]
    ok = n_f == len(accessions) and n_t == expected_txns
    return CheckResult("ingest_reconciliation", ok,
                       f"filings {n_f}/{len(accessions)}, transactions {n_t}/{expected_txns}")


def check_notional_reconciliation(db, window_start: datetime, stats: dict) -> CheckResult:
    """Total open-market $ computed by SQL must equal what ranking accounted for (kept + dropped)."""
    row = db.query(
        """SELECT COUNT(*), COALESCE(SUM(t.shares * t.price_per_share), 0)
           FROM transactions t JOIN filings f USING (accession_number)
           WHERE f.filing_date >= ? AND t.code IN ('P','S')
             AND t.shares > 0 AND t.price_per_share > 0
             AND f.issuer_ticker IS NOT NULL AND f.issuer_ticker <> ''""", [window_start])[0]
    sql_valid_rows, sql_total = int(row[0]), float(row[1])
    accounted = stats["notional_valid"] + stats["notional_inconsistent"]
    split_ok = abs(stats["notional_kept"] + stats["notional_dups"] - stats["notional_valid"]) <= TOL
    total_ok = abs(sql_total - accounted) <= TOL
    rows_ok = sql_valid_rows == (stats["rows_in"] - stats["dropped_invalid"])
    return CheckResult("notional_reconciliation", split_ok and total_ok and rows_ok,
                       f"sql=${sql_total:,.2f} vs ranked=${accounted:,.2f}; rows sql={sql_valid_rows} "
                       f"vs pandas={stats['rows_in'] - stats['dropped_invalid']}")


def check_ticker_bounds(db, window_start: datetime, ranked: pd.DataFrame) -> CheckResult:
    """No ticker's buy/sell total may exceed what the raw table holds for that ticker."""
    if ranked is None or ranked.empty:
        return CheckResult("ticker_bounds", True, "nothing ranked")
    bad = []
    for r in ranked.itertuples():
        row = db.query(
            """SELECT COALESCE(SUM(CASE WHEN t.code='P' THEN t.shares*t.price_per_share END),0),
                      COALESCE(SUM(CASE WHEN t.code='S' THEN t.shares*t.price_per_share END),0)
               FROM transactions t JOIN filings f USING (accession_number)
               WHERE f.filing_date >= ? AND f.issuer_ticker = ?""", [window_start, r.issuer_ticker])[0]
        if r.buy_value > float(row[0]) + TOL or r.sell_value > float(row[1]) + TOL:
            bad.append(r.issuer_ticker)
    return CheckResult("ticker_bounds", not bad, f"violations: {bad}" if bad else f"{len(ranked)} tickers within bounds")


def check_ranking_integrity(ranked: pd.DataFrame, top_n: int) -> CheckResult:
    if ranked is None or ranked.empty:
        return CheckResult("ranking_integrity", True, "empty ranking")
    problems = []
    if len(ranked) > top_n:
        problems.append(f"{len(ranked)} rows > top_n={top_n}")
    if list(ranked["rank"]) != list(range(1, len(ranked) + 1)):
        problems.append("ranks not consecutive from 1")
    vals = ranked["net_dollar_value"].abs().tolist()
    if any(vals[i] < vals[i + 1] for i in range(len(vals) - 1)):
        problems.append("not sorted by |net| descending")
    for r in ranked.itertuples():
        nums = (r.net_dollar_value, r.buy_value, r.sell_value)
        if any(isinstance(x, float) and (math.isnan(x) or math.isinf(x)) for x in nums):
            problems.append(f"{r.issuer_ticker}: NaN/inf")
        if r.buy_value < 0 or r.sell_value < 0:
            problems.append(f"{r.issuer_ticker}: negative gross value")
        if abs((r.buy_value - r.sell_value) - r.net_dollar_value) > 0.02:
            problems.append(f"{r.issuer_ticker}: net != buy - sell")
        if (r.net_dollar_value > 0) != (r.dominant_direction == "BUY"):
            problems.append(f"{r.issuer_ticker}: direction contradicts sign of net")
    return CheckResult("ranking_integrity", not problems, "; ".join(problems) or "ordering, arithmetic, direction OK")


def check_checksum(ranked: pd.DataFrame, checksum: str) -> CheckResult:
    if ranked is None or ranked.empty:
        return CheckResult("checksum", True, "empty ranking")
    recomputed = compute_checksum(ranked.to_dict("records"))
    return CheckResult("checksum", recomputed == checksum,
                       "verified" if recomputed == checksum else f"stored {checksum[:10]}… ≠ recomputed {recomputed[:10]}…")


def run_all_checks(db, window_start: datetime, accessions: list[str], expected_txns: int,
                   ranked: pd.DataFrame, checksum: str, stats: dict, top_n: int) -> tuple[bool, list[CheckResult]]:
    results = [
        check_ingest_reconciliation(db, accessions, expected_txns),
        check_notional_reconciliation(db, window_start, stats),
        check_ticker_bounds(db, window_start, ranked),
        check_ranking_integrity(ranked, top_n),
        check_checksum(ranked, checksum),
    ]
    for r in results:
        (logger.info if r.passed else logger.error)("[INTEGRITY] %s: %s (%s)", r.name, "PASS" if r.passed else "FAIL", r.detail)
    return all(r.passed for r in results), results
