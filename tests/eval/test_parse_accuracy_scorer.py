import datetime as dt

import pandas as pd
import pytest

from insider_intel.eval.harness import ReportWriter
from insider_intel.eval.parse_accuracy import sample_accessions, score, write_report
from insider_intel.eval.sec_datasets import OFFICIAL_COLUMNS


def row(
    acc,
    idx,
    ticker,
    form="4",
    owners="111",
    date="2025-01-02",
    code="P",
    shares=100.0,
    price=10.0,
    ad="A",
    post=1000.0,
    di="D",
    title="Common Stock",
    cik="999",
):
    return dict(
        accession_number=acc,
        row_idx=idx,
        form_type=form,
        issuer_cik=cik,
        issuer_ticker=ticker,
        owner_ciks=owners,
        transaction_date=dt.date.fromisoformat(date),
        code=code,
        shares=shares,
        price_per_share=price,
        acquired_disposed=ad,
        post_transaction_shares=post,
        direct_indirect=di,
        security_title=title,
    )


OFFICIAL = pd.DataFrame(
    [
        row("A1", 0, "AAPL", date="2025-01-02", code="P", shares=100, price=10),
        row("A1", 1, "AAPL", date="2025-01-03", code="S", shares=50, price=12, ad="D", post=950),
        row("A1", 2, "AAPL", date="2025-01-04", code="P", shares=10, price=5, post=960),
        row("A2", 0, "MSFT", owners="222", date="2025-02-01", code="S", shares=20, price=100, ad="D"),
        row("A3", 0, "TSLA", form="4/A", owners="5|6", date="2025-03-01", code="P", shares=10, price=50),
        row("A4", 0, "NVDA", owners="7", code="P", shares=10, price=10),
        row("A4", 1, "NVDA", owners="7", date="2025-01-05", code="P", shares=5, price=10),
        row("A5", 0, "AMD", owners="8", code="P", shares=10, price=10),
    ],
    columns=OFFICIAL_COLUMNS,
)

PARSED = pd.DataFrame(
    [
        # A1: swapped order (row_idx reversed), ticker lower-case, wrong price on the S row
        row("A1", 0, "aapl", date="2025-01-04", code="P", shares=10, price=5, post=960),
        row("A1", 1, "aapl", date="2025-01-03", code="S", shares=50, price=13, ad="D", post=950),
        row("A1", 2, "aapl", date="2025-01-02", code="P", shares=100, price=10),
        # A2: wrong code
        row("A2", 0, "MSFT", owners="222", date="2025-02-01", code="P", shares=20, price=100, ad="D"),
        # A3: missing entirely
        # A4: perfect
        row("A4", 0, "NVDA", owners="7", code="P", shares=10, price=10),
        row("A4", 1, "NVDA", owners="7", date="2025-01-05", code="P", shares=5, price=10),
        # A5: correct row + extra junk row
        row("A5", 0, "AMD", owners="8", code="P", shares=10, price=10),
        row("A5", 1, "AMD", owners="8", date="2024-06-06", code="M", shares=999, price=1, ad="D", post=7),
    ],
    columns=OFFICIAL_COLUMNS,
)


def test_score_injected_errors():
    r = score(PARSED, OFFICIAL)
    assert r["n_accessions"] == 5 and r["n_official_rows"] == 8 and r["n_parsed_rows"] == 8
    assert r["n_matched_rows"] == 7
    assert r["row_precision"] == pytest.approx(7 / 8) and r["row_recall"] == pytest.approx(7 / 8)
    assert r["row_f1"] == pytest.approx(7 / 8)
    assert r["row_f1_strict"] == pytest.approx(5 / 8)  # wrong price + wrong code rows are not strict TPs
    assert r["filing_all_correct_rate"] == pytest.approx(1 / 5)  # only A4
    fm = r["field_exact_match"]
    assert fm["price_per_share"] == pytest.approx(6 / 7)
    assert fm["code"] == pytest.approx(6 / 7)
    assert fm["issuer_ticker"] == 1.0  # case-normalised
    assert fm["shares"] == 1.0 and fm["transaction_date"] == 1.0 and fm["owner_ciks"] == 1.0

    kinds = sorted((m["kind"], m["accession_number"], m["field"]) for m in r["mismatches"])
    assert kinds == sorted(
        [
            ("field", "A1", "price_per_share"),
            ("field", "A2", "code"),
            ("missing_row", "A3", "__row__"),
            ("extra_row", "A5", "__row__"),
        ]
    )
    pm = [m for m in r["mismatches"] if m["field"] == "price_per_share"][0]
    assert (pm["ours"], pm["official"]) == (13.0, 12.0)

    # flow: AAPL 450 vs 400 (50), MSFT -2000 vs +2000 (4000), TSLA 500 vs 0, NVDA/AMD exact
    nf = r["net_flow"]
    assert nf["n_tickers"] == 5
    assert nf["mae_usd"] == pytest.approx((50 + 4000 + 500) / 5)
    assert nf["median_abs_pct_err"] == pytest.approx(50 / 450 * 100)
    assert nf["mean_abs_pct_err"] == pytest.approx((50 / 450 * 100 + 200 + 100) / 5)

    assert r["amendments_4A"] == {"n": 1, "filing_exact_rate": 0.0, "row_recall": 0.0}
    assert r["joint_filers"]["n"] == 1


def test_row_order_and_ticker_case_only_is_perfect():
    p = OFFICIAL.copy()
    p["issuer_ticker"] = p["issuer_ticker"].str.lower()
    p["row_idx"] = p.groupby("accession_number")["row_idx"].transform(lambda s: s.max() - s)  # reversed
    r = score(p, OFFICIAL)
    assert r["filing_all_correct_rate"] == 1.0 and r["row_f1_strict"] == 1.0 and r["mismatches"] == []
    assert r["net_flow"]["mae_usd"] == 0.0


def test_tolerance_and_missing_price():
    off = pd.DataFrame(
        [row("X", 0, "ZZ", price=float("nan"), code="A", ad="A", shares=1000.0)], columns=OFFICIAL_COLUMNS
    )
    par = off.copy()
    par["shares"] = 1000.05  # rel err 5e-5 < 1e-4
    assert score(par, off)["filing_all_correct_rate"] == 1.0
    par["shares"] = 1000.5  # 5e-4 > tol
    r = score(par, off)
    assert r["field_exact_match"]["shares"] == 0.0 and r["n_matched_rows"] == 1


def test_empty_parsed_all_missing():
    r = score(pd.DataFrame(columns=OFFICIAL_COLUMNS), OFFICIAL)
    assert r["row_recall"] == 0.0 and r["row_precision"] == 0.0 and len(r["mismatches"]) == 8


def test_write_report_and_sampling(tmp_path):
    r = score(PARSED, OFFICIAL)
    w = ReportWriter("parse_accuracy", root=tmp_path)
    write_report(r, w, metadata={"seed": 0})
    md = (tmp_path / "parse_accuracy" / "latest.md").read_text()
    assert "Per-field exact match" in md and "price_per_share" in md and "missing_row" in md

    accs, hard = sample_accessions(OFFICIAL, sample_n=3, hard_n=1, seed=0)
    assert len(accs) == 3 and len(hard) == 1 and hard <= set(accs)
    assert hard <= {"A1", "A3", "A2"} | {"A1", "A3"}  # hard = >2? A1 (3 rows ok? no) -> A3 (4/A, joint)


def test_price_compared_at_official_cent_precision():
    """SEC datasets round prices to cents; a 4-decimal filer price must match, and be counted as rounding-only."""
    import pandas as pd

    from insider_intel.eval.parse_accuracy import score

    row = dict(
        accession_number="0001-25-000001",
        row_idx=0,
        form_type="4",
        issuer_cik="1",
        issuer_ticker="ABC",
        owner_ciks="2",
        transaction_date=pd.Timestamp("2025-07-01").date(),
        code="P",
        shares=100.0,
        price_per_share=16.61,
        acquired_disposed="A",
        post_transaction_shares=100.0,
        direct_indirect="D",
        security_title="Common Stock",
    )
    official = pd.DataFrame([row])
    ours_ok = pd.DataFrame([{**row, "price_per_share": 16.6067}])
    ours_bad = pd.DataFrame([{**row, "price_per_share": 16.70}])
    r = score(ours_ok, official)
    assert r["field_exact_match"]["price_per_share"] == 1.0 and r["price_rounding_only"] == 1
    assert r["net_flow"]["mae_usd_cent_rounded"] == 0.0
    r2 = score(ours_bad, official)
    assert r2["field_exact_match"]["price_per_share"] == 0.0 and r2["price_rounding_only"] == 0


def test_dataset_formatting_quirks_are_normalised_on_both_sides():
    """Real 2025Q3 quirks: ticker placeholders, CSV-quoted titles, shares rounded to 2 decimals."""
    import pandas as pd

    from insider_intel.eval.parse_accuracy import score
    from insider_intel.eval.sec_datasets import norm_ticker

    assert [norm_ticker(v) for v in ("NONE", "n/a", "NA", "-", " ", "abc")] == [None] * 5 + ["ABC"]
    row = dict(
        accession_number="0001-25-000002",
        row_idx=0,
        form_type="4",
        issuer_cik="1",
        issuer_ticker="NONE",
        owner_ciks="2",
        transaction_date=pd.Timestamp("2025-07-01").date(),
        code="P",
        shares=3.80,
        price_per_share=10.0,
        acquired_disposed="A",
        post_transaction_shares=3.8,
        direct_indirect="D",
        security_title='"common stock, par value $0.0001 per share (""common stock"")"',
    )
    ours = {
        **row,
        "issuer_ticker": None,
        "shares": 3.7970,
        "security_title": 'common stock, par value $0.0001 per share ("common stock")',
    }
    r = score(pd.DataFrame([ours]), pd.DataFrame([row]))
    assert r["filing_all_correct_rate"] == 1.0
    assert r["shares_rounding_only"] == 1
