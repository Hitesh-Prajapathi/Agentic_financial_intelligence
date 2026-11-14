"""Ingest-side parsing: EDGAR XML -> table rows. (Parser internals are covered in tests/edgar/.)"""
from datetime import datetime
from pathlib import Path

from insider_intel.edgar.form4 import parse_form4
from insider_intel.pipeline.ingest import filing_to_db_rows

FIX = Path(__file__).parent / "edgar" / "fixtures"


def rows(acc, accepted=datetime(2026, 11, 5, 15, 0)):
    return filing_to_db_rows(parse_form4((FIX / f"{acc}.xml").read_bytes(), acc, accepted))


def test_real_filings_map_to_rows():
    total = 0
    for p in sorted(FIX.glob("*.xml")):
        r = rows(p.stem)
        f = r["filing"]
        assert f["accession_number"] == p.stem and f["owner_ciks"] and f["issuer_cik"]
        assert f["filing_date"] == f["accepted_at"] and f["filing_date"].tzinfo is None   # naive UTC
        assert len(r["owners"]) == len(f["owner_ciks"].split("|"))
        for t in r["txns"]:
            total += 1
            if t["shares"] and t["price_per_share"]:
                assert t["dollar_value"] == round(t["shares"] * t["price_per_share"], 2)
            else:
                assert t["dollar_value"] is None
    assert total > 0


def test_all_codes_and_unusable_prices_are_kept():
    r = rows("0000000000-26-000007")                 # gifts (G), zero vs missing price
    assert {t["code"] for t in r["txns"]} == {"G"} and any(t["dollar_value"] is None for t in r["txns"])
    r = rows("0000000000-26-000008")                 # tax withholding (F)
    assert {t["code"] for t in r["txns"]} == {"F"}


def test_price_in_footnote_is_flagged_not_dropped():
    r = rows("0000000000-26-000009")
    assert r["filing"]["price_in_footnote"] and any(t["price_footnote_ids"] for t in r["txns"])
    assert r["footnotes"]


def test_joint_filing_and_amendment_and_plan_flags():
    j = rows("0000000000-26-000002")
    assert len(j["owners"]) == 2 and "|" in j["filing"]["owner_ciks"]
    a = rows("0000000000-26-000003")
    assert a["filing"]["form_type"] == "4/A" and a["filing"]["date_of_original_submission"] is not None
    assert rows("0000000000-26-000013")["filing"]["is_10b5_1"] and not rows("0000000000-26-000012")["filing"]["is_10b5_1"]
    assert not rows("0000000000-26-000011")["filing"]["is_10b5_1"]       # negated footnote
