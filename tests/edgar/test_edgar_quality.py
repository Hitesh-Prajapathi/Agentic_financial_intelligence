from datetime import date, datetime

from insider_intel.edgar.quality import (
    business_days_between,
    dedup_key,
    dedup_rows,
    exercise_and_sell_pairs,
    flag_exercise_and_sell,
    is_late,
)


def utc(y, m, d, h=16, mi=0):  # naive UTC, as Filing.accepted_at
    return datetime(y, m, d, h, mi)


# 2026-03-09 is a Monday
def test_business_days_skip_weekends():
    assert business_days_between(date(2026, 3, 9), date(2026, 3, 9)) == 0
    assert business_days_between(date(2026, 3, 9), date(2026, 3, 11)) == 2
    assert business_days_between(date(2026, 3, 13), date(2026, 3, 16)) == 1  # Fri -> Mon
    assert business_days_between(date(2026, 3, 14), date(2026, 3, 16)) == 1  # Sat -> Mon


def test_is_late_boundaries():
    mon = date(2026, 3, 9)
    assert not is_late(mon, utc(2026, 3, 9))  # same day
    assert not is_late(mon, utc(2026, 3, 11))  # 2 business days: on time
    assert is_late(mon, utc(2026, 3, 12))  # 3 business days: late


def test_is_late_over_a_weekend():
    fri = date(2026, 3, 6)
    assert not is_late(fri, utc(2026, 3, 10))  # Fri -> Tue = 2 business days
    assert is_late(fri, utc(2026, 3, 11))  # Fri -> Wed = 3


def test_is_late_uses_eastern_calendar_date_of_acceptance():
    # 02:00 UTC Thursday 3/12 == 22:00 EDT Wednesday 3/11 -> 2 business days after Monday: on time
    assert not is_late(date(2026, 3, 9), utc(2026, 3, 12, 2, 0))
    assert is_late(date(2026, 3, 9), utc(2026, 3, 12, 5, 0))  # 01:00 EDT Thursday


def test_is_late_accepts_iso_strings_and_unknowns():
    assert is_late("2026-03-09", utc(2026, 3, 13))
    assert not is_late(None, utc(2026, 3, 13))
    assert not is_late(date(2026, 3, 9), None)


def row(**kw):
    r = {"issuer_cik": "1", "owner_ciks": "10", "transaction_date": date(2026, 3, 9), "code": "S",
         "shares": 100.0, "price_per_share": 5.0}
    r.update(kw)
    return r


def test_dedup_key_fields_and_float_rounding():
    k = dedup_key(row(shares=100.0000001))
    assert k == ("1", "10", date(2026, 3, 9), "S", 100.0, 5.0)
    assert dedup_key(row(price_per_share=None))[-1] is None
    assert dedup_key(row()) != dedup_key(row(owner_ciks="10|11"))  # owner-CIK set is part of the key
    assert dedup_key(row(accession_number="a")) == dedup_key(row(accession_number="b"))  # accession is not


def test_dedup_rows_keeps_first_and_distinguishes_joint_sets():
    rows = [row(accession_number="a"), row(accession_number="b"), row(owner_ciks="10|11", accession_number="c")]
    assert [r["accession_number"] for r in dedup_rows(rows)] == ["a", "c"]


def test_exercise_and_sell_pairs_same_owner_issuer_day():
    rows = [
        row(code="M"), row(code="S"), row(code="S", shares=5.0),  # exercise-and-sell
        row(code="S", owner_ciks="20"),  # other owner: sell only
        row(code="M", issuer_cik="2"),  # other issuer: exercise only
        row(code="P", owner_ciks="30"), row(code="S", owner_ciks="30", transaction_date=date(2026, 3, 10)),
    ]
    assert exercise_and_sell_pairs(rows) == {("1", "10", date(2026, 3, 9))}
    assert flag_exercise_and_sell(rows) == [True, True, True, False, False, False, False]


def test_exercise_and_sell_joint_filing_flags_each_owner():
    rows = [row(code="M", owner_ciks="10|11"), row(code="S", owner_ciks="11")]
    assert exercise_and_sell_pairs(rows) == {("1", "11", date(2026, 3, 9))}
    assert flag_exercise_and_sell(rows) == [True, True]
