import pandas as pd

from agents.symbolic_validation import compute_checksum, validate_and_rank


def row(acc, tk="AAA", code="P", shares=1000, price=10.0, name="Ann", idx=0, ad=None, date="2026-11-04", **kw):
    r = dict(accession_number=acc, row_idx=idx, issuer_ticker=tk, issuer_name=tk + " Inc", code=code, shares=shares,
             price_per_share=price, insider_name=name, insider_title="CEO", transaction_date=date,
             acquired_disposed=ad or ("A" if code == "P" else "D"), is_officer=True, is_director=False, is_10b5_1=False,
             post_transaction_shares=10_000)
    r.update(kw)
    return r


def rank(rows, **kw):
    return validate_and_rank(rows, top_n=kw.pop("top_n", 5), min_notional=kw.pop("min_notional", 0))


def test_net_flow_signs_and_direction():
    df, _, _ = rank([row("1", "AAA", "P", 1000, 10), row("2", "AAA", "S", 400, 10, name="Bob")])
    r = df.iloc[0]
    assert (r.buy_value, r.sell_value, r.net_dollar_value, r.dominant_direction) == (10_000, 4_000, 6_000, "BUY")


def test_grants_and_other_codes_are_excluded():
    df, _, stats = rank([row("1", code="A"), row("2", code="M"), row("3", code="F")])
    assert df.empty and stats["dropped_invalid"] == 3


def test_joint_filer_duplicates_counted_once():
    # same trade reported under two accession numbers by joint filers
    rows = [row("1", name="Fund LP"), row("2", name="Fund GP")]
    df, _, stats = rank(rows)
    assert df.iloc[0].buy_value == 10_000 and stats["dropped_joint_dups"] == 1
    assert stats["notional_kept"] + stats["notional_dups"] == stats["notional_valid"]


def test_different_trades_are_not_collapsed():
    df, _, stats = rank([row("1", shares=1000), row("2", shares=1500, name="Bob")])
    assert df.iloc[0].buy_value == 25_000 and stats["dropped_joint_dups"] == 0


def test_code_and_flag_disagreement_dropped():
    df, _, stats = rank([row("1", code="S", ad="A"), row("2", code="P", shares=500, name="Bob")])
    assert stats["dropped_inconsistent"] == 1 and df.iloc[0].sell_value == 0


def test_ranked_by_absolute_net_with_deterministic_ties():
    rows = [row("1", "BBB", "S", 100, 10), row("2", "AAA", "P", 100, 10, name="Z"), row("3", "CCC", "P", 900, 10, name="Y")]
    df, _, _ = rank(rows)
    assert list(df.issuer_ticker) == ["CCC", "AAA", "BBB"] and list(df["rank"]) == [1, 2, 3]


def test_top_n_and_min_notional():
    rows = [row(str(i), f"T{i}", shares=100 * (i + 1)) for i in range(8)]
    assert len(rank(rows, top_n=3)[0]) == 3
    assert len(rank(rows, min_notional=5_000)[0]) == 4  # only tickers with gross >= $5k


def test_signal_tags():
    rows = [row("1", "AAA", "P", 1000, 10, name="A"), row("2", "AAA", "P", 1000, 10.5, name="B"),
            row("3", "BBB", "S", 100, 10, name="C", is_10b5_1=True, post_transaction_shares=100)]
    df, _, _ = rank(rows)
    tags = dict(zip(df.issuer_ticker, df.signal_tags))
    assert "CLUSTER_BUY" in tags["AAA"] and "OFFICER" in tags["AAA"]
    assert "PLANNED_10b5-1" in tags["BBB"] and "LARGE_STAKE" in tags["BBB"]  # sold half of prior position


def test_checksum_changes_when_values_change():
    df, cs, _ = rank([row("1")])
    assert cs == compute_checksum(df.to_dict("records"))
    tampered = df.copy()
    tampered.loc[0, "net_dollar_value"] += 1
    assert compute_checksum(tampered.to_dict("records")) != cs


def test_empty_input():
    df, cs, stats = validate_and_rank(pd.DataFrame(), 5)
    assert df.empty and len(cs) == 64 and stats["rows_in"] == 0
