"""Golden tests for the Form 4 parser.

Fixtures in tests/edgar/fixtures are either RECONSTRUCTED (real accession numbers, values rebuilt from
real sec-api.io JSON) or SYNTHETIC (fictional, accession 0000000000-26-0000NN). See fixtures/README.md.
Expected values below were typed by hand from the XML, not computed by the parser.
"""
from datetime import date, datetime
from pathlib import Path

import pytest

from insider_intel.edgar.form4 import (
    Form4ParseError,
    filing_to_rows,
    mentions_trading_plan,
    parse_form4,
)

FIX = Path(__file__).parent / "fixtures"


def load(acc, accepted_at=None):
    return parse_form4((FIX / f"{acc}.xml").read_bytes(), acc, accepted_at)


# ---------------------------------------------------------------------------- filing-level goldens
# acc -> (form_type, schema, issuer_cik, ticker, owner_ciks, n_nonderiv, aff, is_10b5_1, price_in_footnote)
FILING_GOLDEN = {
    "0000000000-26-000001": ("4", "X0609", "9000001", "ACME", ["9100001"], 1, False, False, False),
    "0000000000-26-000002": ("4", "X0609", "9000001", "ACME", ["9100003", "9100004"], 2, False, False, False),
    "0000000000-26-000003": ("4/A", "X0609", "9000001", "ACME", ["9100002"], 1, False, False, False),
    "0000000000-26-000004": ("4", "X0508", "9000001", "ACME", ["9100001"], 0, False, False, False),
    "0000000000-26-000005": ("4", "X0609", "9000002", None, ["9100001"], 1, False, False, False),
    "0000000000-26-000006": ("4", "X0609", "9000003", None, ["9100001"], 1, False, False, False),
    "0000000000-26-000007": ("4", "X0609", "9000001", "ACME", ["9100002"], 2, False, False, False),
    "0000000000-26-000008": ("4", "X0609", "9000001", "ACME", ["9100001"], 1, False, False, False),
    "0000000000-26-000009": ("4", "X0609", "9000001", "ACME", ["9100001"], 2, False, False, True),
    "0000000000-26-000010": ("4", "X0306", "9000001", "ACME", ["9100001"], 1, None, True, False),
    "0000000000-26-000011": ("4", "X0306", "9000001", "ACME", ["9100001"], 1, None, False, False),
    "0000000000-26-000012": ("4", "X0609", "9000001", "ACME", ["9100001"], 1, False, False, False),
    "0000000000-26-000013": ("4", "X0609", "9000001", "ACME", ["9100001"], 1, True, True, False),
    "0000000000-26-000014": ("4", "X0609", "9000001", "ACME", ["9100005"], 2, False, False, False),
    "0000000000-26-000015": ("4", "X0609", "9000001", "ACME", ["9100001"], 3, False, False, False),
    "0000000000-26-000016": ("4", "X0609", "9000001", "ACME", ["9100002"], 1, False, False, False),
    "0001193125-26-449032": ("4", "X0609", "1068851", "PB", ["1136856"], 6, True, True, False),
    "0001193125-26-448980": ("4", "X0609", "1228454", "BCBP", ["1826510"], 1, False, False, False),
    "0001193125-26-448969": ("4", "X0609", "1228454", "BCBP", ["1628816"], 1, False, False, False),
    "0001193125-26-448845": ("4", "X0609", "1495153", "MMYT", ["1971627"], 4, False, False, False),
    "0001570585-26-000156": ("4", "X0609", "1570585", "LBTY", ["1565787"], 1, False, False, False),
    "0001058725-26-000011": ("4", "X0609", "1570585", "LBTY", ["1058725"], 1, False, False, False),
    "0002093121-26-000009": ("4", "X0609", "1866364", "BULL", ["2093121"], 1, True, True, False),
    "0000080424-26-000271": ("4", "X0609", "80424", "PG", ["1844763"], 2, False, False, False),
}


def test_every_fixture_has_a_golden_row():
    on_disk = {p.stem for p in FIX.glob("*.xml")}
    assert on_disk == set(FILING_GOLDEN)


@pytest.mark.parametrize("acc", sorted(FILING_GOLDEN))
def test_filing_level_fields(acc):
    form, schema, icik, ticker, owners, n, aff, plan, pif = FILING_GOLDEN[acc]
    f = load(acc)
    assert f.accession_number == acc
    assert (f.form_type, f.schema_version) == (form, schema)
    assert (f.issuer_cik, f.issuer_ticker) == (icik, ticker)
    assert [o.cik for o in f.owners] == owners
    assert len(f.non_derivative) == n
    assert [t.row_idx for t in f.non_derivative] == list(range(n))
    assert f.aff_10b5_1 is aff
    assert f.is_10b5_1 is plan
    assert f.price_in_footnote is pif
    assert len(filing_to_rows(f)) == n


# ---------------------------------------------------------------------------- row-level goldens
def row(f, i):
    t = f.non_derivative[i]
    return (t.transaction_date, t.code, t.shares, t.price_per_share, t.acquired_disposed,
            t.post_transaction_shares, t.direct_indirect)


def test_simple_purchase():
    f = load("0000000000-26-000001")
    assert f.period_of_report == date(2026, 3, 10)
    assert f.issuer_name == "Acme Widgets Inc"
    o = f.owners[0]
    assert (o.name, o.is_officer, o.is_director, o.is_ten_pct, o.is_other) == ("Doe Jane", True, False, False, False)
    assert o.officer_title == "Chief Executive Officer"
    assert row(f, 0) == (date(2026, 3, 10), "P", 1000.0, 12.5, "A", 51000.0, "D")
    t = f.non_derivative[0]
    assert (t.security_title, t.form_type_in_txn, t.equity_swap, t.nature_of_ownership) == ("Common Stock", "4", False, None)


def test_joint_filing_keeps_all_owners():
    f = load("0000000000-26-000002")
    fund, gp = f.owners
    assert (fund.name, fund.is_ten_pct, fund.is_director) == ("Example Capital Partners LP", True, False)
    assert (gp.name, gp.is_ten_pct, gp.is_director) == ("Poe Edgar", True, True)
    assert row(f, 0)[1:4] == ("P", 25000.0, 3.1)
    assert f.non_derivative[0].direct_indirect == "I"
    assert f.non_derivative[0].nature_of_ownership == "By Example Capital Partners LP"
    rows = filing_to_rows(f)
    assert {r["owner_ciks"] for r in rows} == {"9100003|9100004"}


def test_amendment():
    f = load("0000000000-26-000003")
    assert f.form_type == "4/A"
    assert f.date_of_original_submission == date(2026, 3, 12)
    assert f.remarks == "Amendment to correct the number of shares sold."
    assert row(f, 0) == (date(2026, 3, 10), "S", 500.0, 14.0, "D", 20000.0, "D")
    assert filing_to_rows(f)[0]["form_type"] == "4/A"


def test_derivative_only_is_valid_and_empty():
    f = load("0000000000-26-000004")
    assert f.non_derivative == []
    assert filing_to_rows(f) == []


def test_missing_and_placeholder_tickers_are_none():
    assert load("0000000000-26-000005").issuer_ticker is None  # empty element
    assert load("0000000000-26-000006").issuer_ticker is None  # ' none '


def test_gift_code_g_is_kept_and_zero_price_distinct_from_missing():
    f = load("0000000000-26-000007")
    assert [t.code for t in f.non_derivative] == ["G", "G"]
    assert f.non_derivative[0].price_per_share == 0.0
    assert f.non_derivative[0].price_footnote_ids == ["F1"]
    assert f.non_derivative[1].price_per_share is None
    assert f.non_derivative[1].acquired_disposed == "A"
    assert f.non_derivative[1].direct_indirect == "I"
    assert f.non_derivative[1].nature_of_ownership == "By Roe Family Foundation"
    assert f.price_in_footnote is False  # G rows never count


def test_tax_withholding_code_f():
    f = load("0000000000-26-000008")
    assert row(f, 0) == (date(2026, 3, 10), "F", 412.0, 48.2, "D", 90588.0, "D")
    assert "tax withholding" in f.footnotes["F1"]


def test_price_given_only_in_footnote_is_flagged_and_text_kept():
    f = load("0000000000-26-000009")
    t0, t1 = f.non_derivative
    assert t0.price_per_share is None and t0.price_footnote_ids == ["F1"]
    assert t1.price_per_share == 20.5 and t1.price_footnote_ids == []
    assert f.price_in_footnote is True
    assert "Weighted average" in f.footnotes["F1"]
    assert filing_to_rows(f)[0]["price_per_share"] is None


def test_weighted_average_footnote_with_price_present_is_not_flagged():
    f = load("0002093121-26-000009")
    t = f.non_derivative[0]
    assert t.price_per_share == 5.3893 and t.price_footnote_ids == ["F2"]
    assert f.price_in_footnote is False
    assert "weighted average" in f.footnotes["F2"]


def test_10b5_1_checkbox_set_and_unset():
    assert load("0000000000-26-000013").is_10b5_1 is True
    f = load("0000000000-26-000012")
    assert f.aff_10b5_1 is False and f.is_10b5_1 is False


def test_10b5_1_legacy_footnote_fallback_and_negation():
    pos, neg = load("0000000000-26-000010"), load("0000000000-26-000011")
    assert pos.aff_10b5_1 is None and pos.is_10b5_1 is True
    assert neg.aff_10b5_1 is None and neg.is_10b5_1 is False


def test_10b5_1_real_filing_checkbox_and_footnote_agree():
    f = load("0002093121-26-000009")
    assert f.aff_10b5_1 is True
    assert "Rule 10b5-1 trading plan" in f.footnotes["F1"]


def test_other_relationship_text_string_booleans_commas_holdings_and_missing_nodes():
    f = load("0000000000-26-000014")
    o = f.owners[0]
    assert (o.is_other, o.is_director, o.is_officer, o.is_ten_pct) == (True, False, False, False)
    assert o.other_text == "Member of 10% owner group"
    assert f.issuer_ticker == "ACME"  # ' acme ' stripped and upper-cased
    # nonDerivativeHolding is skipped; row_idx counts transactions only
    assert [t.row_idx for t in f.non_derivative] == [0, 1]
    a, b = f.non_derivative
    assert (a.shares, a.price_per_share, a.post_transaction_shares) == (1250.5, 10.25, 2500.75)  # '1,250.5'
    assert a.equity_swap is True
    assert a.transaction_date == date(2026, 3, 10)
    # row with missing postTransactionAmounts / ownershipNature / price, lower-case code
    assert (b.code, b.acquired_disposed, b.shares) == ("P", "A", 10.0)
    assert b.price_per_share is None and b.post_transaction_shares is None
    assert b.direct_indirect is None and b.nature_of_ownership is None


def test_same_day_exercise_and_sale_rows_keep_every_code():
    f = load("0000000000-26-000015")
    assert [t.code for t in f.non_derivative] == ["M", "S", "S"]
    assert {t.transaction_date for t in f.non_derivative} == {date(2026, 3, 9)}
    assert [r["code"] for r in filing_to_rows(f)] == ["M", "S", "S"]


def test_indirect_ownership_via_trust():
    f = load("0000000000-26-000016")
    t = f.non_derivative[0]
    assert (t.direct_indirect, t.nature_of_ownership) == ("I", "By Roe Family Trust")
    assert f.footnotes["F1"].startswith("Shares held of record by the Roe Family Trust")


# ---------------------------------------------------------------------------- real-data (reconstructed) goldens
def test_real_multi_row_with_holdings_and_indirect_rows():
    f = load("0001193125-26-449032")
    assert f.owners[0].name == "HOLMES NED S" and f.owners[0].is_director and not f.owners[0].is_officer
    assert row(f, 0) == (date(2026, 11, 4), "S", 455.0, 73.9631, "A", 66070.0, "D")
    assert row(f, 1) == (date(2026, 11, 4), "S", 45.0, 74.7, "D", 66025.0, "D")
    assert (f.non_derivative[2].direct_indirect, f.non_derivative[2].nature_of_ownership) == ("I", "By profit sharing plan")
    assert f.non_derivative[4].nature_of_ownership == "As trustee of SSH trust for adult daughter"
    assert {t.code for t in f.non_derivative} == {"S"}


def test_real_shares_footnote_ids_are_separate_from_price_footnote_ids():
    f = load("0001193125-26-448980")
    t = f.non_derivative[0]
    assert (t.code, t.shares, t.price_per_share, t.post_transaction_shares) == ("P", 55.0, 9.1299, 1514.0)
    assert t.shares_footnote_ids == ["F1"] and t.price_footnote_ids == []
    assert set(f.footnotes) == {"F1", "F2"}
    assert f.footnotes["F1"].startswith("Represents shares of common stock purchased in the open market")


def test_real_exercise_and_sell_weighted_average():
    f = load("0001193125-26-448845")
    assert [t.code for t in f.non_derivative] == ["M", "S", "M", "S"]
    assert f.owners[0].officer_title == "Group Chief Executive Officer"
    assert f.owners[0].is_director and f.owners[0].is_officer
    assert row(f, 1) == (date(2026, 11, 3), "S", 20000.0, 45.25, "D", 0.0, "D")
    assert row(f, 3)[2:4] == (10000.0, 50.11)
    assert f.non_derivative[1].price_footnote_ids == ["F1"] and f.non_derivative[3].price_footnote_ids == ["F2"]
    assert f.footnotes["F1"].startswith("The price reported in Column 4 is a weighted average price")


def test_real_trust_purchase_with_remarks():
    f = load("0001570585-26-000156")
    t = f.non_derivative[0]
    assert (t.security_title, t.shares, t.price_per_share, t.post_transaction_shares) == ("Class A Common Shares", 211313.0, 9.4456, 602378.0)
    assert (t.direct_indirect, t.nature_of_ownership, t.price_footnote_ids) == ("I", "By Trust", ["F1"])
    assert f.owners[0].officer_title == "EVP, Chief Technology Officer"
    assert f.remarks == "The trading symbols for the Issuer's classes of common shares are LBTYA, LBTYB, and LBTYK."


def test_real_same_day_m_and_s_big_issuer_short_cik():
    f = load("0000080424-26-000271")
    assert f.issuer_cik == "80424"  # '0000080424' in the XML
    assert row(f, 0) == (date(2026, 11, 4), "M", 33624.0, 91.07, "A", 99242.0097, "D")
    assert row(f, 1) == (date(2026, 11, 4), "S", 33624.0, 165.0, "D", 65618.0097, "D")


def test_real_fixture_meta_gives_acceptance_time_naive_utc():
    import json
    meta = json.loads((FIX / "0001193125-26-449032.meta.json").read_text())
    assert meta["accepted_at"] == "2026-11-05T16:15:01"  # 11:15:01 EST


# ---------------------------------------------------------------------------- rows + misc
def test_filing_to_rows_shape_and_sorted_owner_ciks():
    acc_at = datetime(2026, 3, 10, 21, 30, 12)
    f = load("0000000000-26-000002", accepted_at=acc_at)
    r = filing_to_rows(f)[1]
    assert r == {
        "accession_number": "0000000000-26-000002", "row_idx": 1, "form_type": "4", "accepted_at": acc_at,
        "issuer_cik": "9000001", "issuer_ticker": "ACME", "owner_ciks": "9100003|9100004",
        "transaction_date": date(2026, 3, 10), "code": "P", "shares": 10000.0, "price_per_share": 3.2,
        "acquired_disposed": "A", "post_transaction_shares": 1035000.0, "direct_indirect": "I",
        "security_title": "Common Stock",
    }


def test_accession_without_dashes_is_normalised():
    f = parse_form4((FIX / "0000000000-26-000001.xml").read_bytes(), "000000000026000001")
    assert f.accession_number == "0000000000-26-000001"


def test_str_input_and_bom_and_namespace_tolerated():
    raw = (FIX / "0000000000-26-000001.xml").read_text()
    ns = raw.replace("<ownershipDocument>", '<ownershipDocument xmlns="http://www.sec.gov/edgar/ownership">')
    f = parse_form4("﻿" + ns, "0000000000-26-000001")
    assert f.issuer_ticker == "ACME" and len(f.non_derivative) == 1


def test_sgml_wrapped_submission_text_is_unwrapped():
    xml = (FIX / "0000000000-26-000001.xml").read_text()
    body = xml[xml.index("<ownershipDocument>"):]
    wrapped = f"<SEC-DOCUMENT>\n<SEC-HEADER>\n<TYPE>4\n<XML>\n{body}</XML>\n</SEC-DOCUMENT>\n"
    assert parse_form4(wrapped, "0000000000-26-000001").owners[0].cik == "9100001"


@pytest.mark.parametrize("bad", [b"", b"<html><body>Not XML</body>", b"<html></html>", b"<a><b></a>"])
def test_unusable_documents_raise_form4_parse_error(bad):
    with pytest.raises(Form4ParseError):
        parse_form4(bad, "0000000000-26-000001")


def test_missing_issuer_or_owner_raises():
    with pytest.raises(Form4ParseError):
        parse_form4("<ownershipDocument><reportingOwner><reportingOwnerId><rptOwnerCik>1</rptOwnerCik></reportingOwnerId></reportingOwner></ownershipDocument>", "0000000000-26-000001")
    with pytest.raises(Form4ParseError):
        parse_form4("<ownershipDocument><issuer><issuerCik>1</issuerCik></issuer></ownershipDocument>", "0000000000-26-000001")


def test_mentions_trading_plan():
    assert mentions_trading_plan("Sold under a Rule 10b5-1 trading plan")
    assert mentions_trading_plan("pursuant to a 10b51 plan")
    assert not mentions_trading_plan("These sales were not made pursuant to a Rule 10b5-1 plan")
    assert not mentions_trading_plan("")
