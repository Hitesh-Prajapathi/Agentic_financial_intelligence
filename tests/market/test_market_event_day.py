import pandas as pd

from insider_intel.market.event_study import event_day

CAL = pd.bdate_range("2024-01-01", "2024-12-31").drop([pd.Timestamp("2024-07-04"), pd.Timestamp("2024-03-29")])


def ed(s):
    return event_day(pd.Timestamp(s), CAL)


def test_friday_after_close_goes_to_monday():
    assert ed("2024-06-14 20:30") == pd.Timestamp("2024-06-17")  # 16:30 EDT


def test_friday_intraday_goes_to_monday():
    assert ed("2024-06-14 18:00") == pd.Timestamp("2024-06-17")  # 14:00 EDT


def test_pre_market_same_day():
    assert ed("2024-06-17 12:00") == pd.Timestamp("2024-06-17")  # 08:00 EDT


def test_open_boundary_summer():
    assert ed("2024-06-17 13:29:59") == pd.Timestamp("2024-06-17")  # 09:29:59 EDT
    assert ed("2024-06-17 13:30:00") == pd.Timestamp("2024-06-18")  # 09:30:00 EDT -> next day


def test_close_boundary():
    assert ed("2024-06-17 19:59:59") == pd.Timestamp("2024-06-18")  # 15:59:59 EDT intraday -> next
    assert ed("2024-06-17 20:00:00") == pd.Timestamp("2024-06-18")  # 16:00 EDT after close -> next


def test_dst_changes_utc_mapping():
    # Winter (EST, UTC-5): 14:30 UTC == 09:30 ET
    assert ed("2024-01-16 14:29") == pd.Timestamp("2024-01-16")
    assert ed("2024-01-16 14:30") == pd.Timestamp("2024-01-17")
    # Summer (EDT, UTC-4): 14:30 UTC == 10:30 ET  -> already intraday
    assert ed("2024-06-18 14:30") == pd.Timestamp("2024-06-19")
    # Day after DST ends (Mon 2024-11-04, EST): 14:30 UTC == 09:30 ET
    assert ed("2024-11-04 14:29") == pd.Timestamp("2024-11-04")
    assert ed("2024-11-04 14:30") == pd.Timestamp("2024-11-05")
    # Day after DST starts (Mon 2024-03-11, EDT): 13:00 UTC == 09:00 ET
    assert ed("2024-03-11 13:00") == pd.Timestamp("2024-03-11")


def test_holiday_gap_uses_calendar():
    assert ed("2024-07-03 21:00") == pd.Timestamp("2024-07-05")  # after close Wed, Thu holiday
    assert ed("2024-07-04 12:00") == pd.Timestamp("2024-07-05")  # pre-market on a holiday
    assert ed("2024-03-28 22:00") == pd.Timestamp("2024-04-01")  # Good Friday closed


def test_weekend_any_time():
    assert ed("2024-06-15 12:00") == pd.Timestamp("2024-06-17")
    assert ed("2024-06-16 23:00") == pd.Timestamp("2024-06-17")


def test_utc_date_differs_from_eastern_date():
    # 02:00 UTC Thursday == 22:00 ET Wednesday -> after close -> Thursday
    assert ed("2024-06-13 02:00") == pd.Timestamp("2024-06-13")
    # 01:00 UTC Friday 06-14 is Thursday 21:00 ET -> next trading day Friday
    assert ed("2024-06-14 01:00") == pd.Timestamp("2024-06-14")


def test_tz_aware_equals_naive_and_none_cases():
    assert event_day(pd.Timestamp("2024-06-14 20:30", tz="UTC"), CAL) == ed("2024-06-14 20:30")
    assert event_day(None, CAL) is None
    assert event_day(pd.NaT, CAL) is None


def test_beyond_calendar_returns_none():
    assert ed("2024-12-31 22:00") is None
