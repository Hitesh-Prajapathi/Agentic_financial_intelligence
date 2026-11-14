"""Pure data-quality helpers over parsed Form 4 rows (dicts as produced by filing_to_rows)."""
from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

_EASTERN = ZoneInfo("America/New_York")
LATE_AFTER_BUSINESS_DAYS = 2


def _to_date(v) -> date | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def business_days_between(start: date, end: date) -> int:
    """Weekdays in (start, end]. Weekends only: market/federal holidays are NOT handled."""
    if end <= start:
        return 0
    n, d = 0, start
    while d < end:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n += 1
    return n


def acceptance_date_eastern(accepted_at: datetime) -> date:
    """Calendar date in US/Eastern of an EDGAR acceptance time (naive datetimes are taken as UTC)."""
    if accepted_at.tzinfo is None:
        accepted_at = accepted_at.replace(tzinfo=timezone.utc)
    return accepted_at.astimezone(_EASTERN).date()


def is_late(transaction_date, accepted_at: datetime | None) -> bool:
    """True if the Form 4 was accepted more than 2 business days after the transaction date.

    Weekends are skipped; holidays are not handled (so a holiday can make a timely filing look
    one day late). The 10pm-ET same-day filing-date cutoff is not modelled; the Eastern calendar
    date of acceptance is used. Unknown inputs return False.
    """
    txn, acc = _to_date(transaction_date), accepted_at
    if txn is None or acc is None:
        return False
    return business_days_between(txn, acceptance_date_eastern(acc)) > LATE_AFTER_BUSINESS_DAYS


def _f(x) -> float | None:
    return None if x is None else round(float(x), 6)


def dedup_key(row: dict) -> tuple:
    """Joint-filer-safe duplicate key: (issuer CIK, owner-CIK set, date, code, shares, price)."""
    return (
        row.get("issuer_cik"),
        row.get("owner_ciks"),
        row.get("transaction_date"),
        row.get("code"),
        _f(row.get("shares")),
        _f(row.get("price_per_share")),
    )


def dedup_rows(rows: Iterable[dict]) -> list[dict]:
    """Keep the first row per dedup_key, preserving order."""
    seen: set = set()
    out = []
    for r in rows:
        k = dedup_key(r)
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


def _owner_list(row: dict) -> list[str]:
    return [c for c in (row.get("owner_ciks") or "").split("|") if c]


def exercise_and_sell_pairs(rows: Iterable[dict]) -> set[tuple]:
    """Keys (issuer_cik, owner_cik, transaction_date) where the same owner has both an option
    exercise (M) and an open-market sale (S) at the same issuer on the same day. Joint-filing rows
    count for each of their owners."""
    by_key: dict[tuple, set[str]] = {}
    for r in rows:
        code = r.get("code")
        if code not in ("M", "S"):
            continue
        for owner in _owner_list(r):
            by_key.setdefault((r.get("issuer_cik"), owner, r.get("transaction_date")), set()).add(code)
    return {k for k, codes in by_key.items() if {"M", "S"} <= codes}


def flag_exercise_and_sell(rows: list[dict]) -> list[bool]:
    """Per-row flag aligned with `rows`: True for the M and S rows that belong to an exercise-and-sell pair."""
    pairs = exercise_and_sell_pairs(rows)
    return [
        r.get("code") in ("M", "S")
        and any((r.get("issuer_cik"), o, r.get("transaction_date")) in pairs for o in _owner_list(r))
        for r in rows
    ]
