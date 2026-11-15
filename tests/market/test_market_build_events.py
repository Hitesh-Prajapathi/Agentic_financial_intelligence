import pandas as pd
import pytest

from insider_intel.market.event_study import build_events


def rows(data):
    cols = [
        "issuer_ticker",
        "owner_ciks",
        "code",
        "shares",
        "price_per_share",
        "accepted_at",
        "officer_title",
        "is_10b5_1",
    ]
    return pd.DataFrame(data, columns=cols)


def P(tk, owner, at, title="Director", sh=1000, px=10.0):
    return [tk, owner, "P", sh, px, at, title, False]


def S(tk, owner, at, plan, sh=1000, px=10.0):
    return [tk, owner, "S", sh, px, at, "Director", plan]


def test_single_buy_ignores_sales_and_dedups_per_issuer():
    df = rows(
        [
            P("AAA", "1", "2024-01-02 15:00"),
            P("AAA", "2", "2024-01-10 15:00"),  # within 30d of kept -> dropped
            P("AAA", "3", "2024-02-15 15:00"),  # >30d -> kept
            P("BBB", "4", "2024-01-05 15:00"),
            S("AAA", "5", "2024-01-03 15:00", False),
        ]
    )
    ev = build_events(df, "single_buy")
    assert list(ev["ticker"]) == ["AAA", "BBB", "AAA"]
    assert (ev["kind"] == "single_buy").all()
    assert build_events(df, "single_buy", dedup_days=None).shape[0] == 4


def test_same_filing_rows_collapse():
    df = rows([P("AAA", "1", "2024-01-02 15:00", sh=100), P("AAA", "1", "2024-01-02 15:00", sh=300)])
    ev = build_events(df, "single_buy")
    assert len(ev) == 1 and ev.loc[0, "total_shares"] == 400


def test_cluster_event_at_kth_owner_acceptance():
    df = rows(
        [
            P("AAA", "1", "2024-01-02 15:00"),
            P("AAA", "1", "2024-01-05 15:00"),  # same owner again: not a cluster
            P("AAA", "2", "2024-01-20 15:00"),  # 2nd distinct owner within 30d -> event
            P("AAA", "3", "2024-01-25 15:00"),  # inside dedup window -> no new event
            P("BBB", "7", "2024-01-02 15:00"),
            P("BBB", "8", "2024-03-01 15:00"),  # 59 days apart -> no cluster
        ]
    )
    ev = build_events(df, "cluster_buy", k=2, window_days=30)
    assert list(ev["ticker"]) == ["AAA"]
    assert ev.loc[0, "accepted_at"] == pd.Timestamp("2024-01-20 15:00")
    assert ev.loc[0, "n_owners"] == 2
    ev3 = build_events(df, "cluster_buy", k=3, window_days=30)
    assert ev3.loc[0, "accepted_at"] == pd.Timestamp("2024-01-25 15:00")


def test_cluster_multi_owner_filing_and_unknown_owner():
    df = rows(
        [P("AAA", "1,2", "2024-01-02 15:00"), P("CCC", None, "2024-01-02 15:00"), P("CCC", None, "2024-01-03 15:00")]
    )
    ev = build_events(df, "cluster_buy", k=2)
    assert list(ev["ticker"]) == ["AAA"]  # unknown owners never form a cluster


def test_officer_buy_title_regex():
    df = rows(
        [
            P("AAA", "1", "2024-01-02 15:00", title="Chief Executive Officer"),
            P("BBB", "2", "2024-01-02 15:00", title="VP Sales"),
            P("CCC", "3", "2024-01-02 15:00", title="Principal Financial Officer"),
            P("DDD", "4", "2024-01-02 15:00", title="Director"),
        ]
    )
    assert set(build_events(df, "officer_buy")["ticker"]) == {"AAA", "CCC"}
    with pytest.raises(ValueError):
        build_events(df.drop(columns="officer_title"), "officer_buy")


def test_sell_kinds_split_on_10b5_1_and_exclude_unknown():
    df = rows(
        [
            S("AAA", "1", "2024-01-02 15:00", True),
            S("BBB", "2", "2024-01-02 15:00", False),
            S("CCC", "3", "2024-01-02 15:00", None),
            P("DDD", "4", "2024-01-02 15:00"),
        ]
    )
    assert list(build_events(df, "planned_sell")["ticker"]) == ["AAA"]
    assert list(build_events(df, "discretionary_sell")["ticker"]) == ["BBB"]


def test_min_price_and_large_buy_and_unknown_kind():
    df = rows(
        [
            P("AAA", "1", "2024-01-02 15:00", px=0.5),
            P("BBB", "2", "2024-01-02 15:00", sh=20000, px=10),
            P("CCC", "3", "2024-01-02 15:00"),
        ]
    )
    assert set(build_events(df, "single_buy", min_price=1.0)["ticker"]) == {"BBB", "CCC"}
    assert list(build_events(df, "large_buy", min_value=100_000)["ticker"]) == ["BBB"]
    with pytest.raises(ValueError):
        build_events(df, "nonsense")


def test_empty_result_has_columns():
    ev = build_events(rows([S("AAA", "1", "2024-01-02 15:00", True)]), "single_buy")
    assert ev.empty and "event_id" in ev.columns
