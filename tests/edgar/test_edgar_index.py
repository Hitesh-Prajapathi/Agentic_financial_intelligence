"""Daily-index parsing, filing resolution and acceptance-time parsing. Offline."""

import json
from datetime import date, datetime
from pathlib import Path

import httpx
import pytest

from insider_intel.edgar import index as ix
from insider_intel.edgar.client import EdgarClient
from insider_intel.edgar.index import FilingResolutionError

HERE = Path(__file__).parent
SAMPLES = HERE / "samples"
FIX = HERE / "fixtures"
ACC = "0000000000-26-000001"
FOLDER = "https://www.sec.gov/Archives/edgar/data/9000001/000000000026000001"


def test_daily_index_url():
    assert (
        ix.daily_index_url(date(2026, 3, 10))
        == "https://www.sec.gov/Archives/edgar/daily-index/2026/QTR1/form.20260310.idx"
    )
    assert "QTR4/form.20261105.idx" in ix.daily_index_url(date(2026, 11, 5))
    assert "QTR3/" in ix.daily_index_url(date(2026, 9, 15))


def test_parse_form_idx_keeps_only_forms_4_and_4a_and_dedups_by_accession():
    entries = ix.parse_form_idx((SAMPLES / "form.20260310.idx").read_text())
    # accessions ...001 and ...002 are each listed twice (issuer + owner rows) -> one entry each
    assert [e.accession for e in entries] == [
        "0000000000-26-000001",
        "0000000000-26-000002",
        "0000000000-26-000003",
        "0000000000-26-000006",
    ]
    assert [e.form_type for e in entries] == ["4", "4", "4/A", "4"]
    first = entries[0]
    assert first.company_name == "ACME WIDGETS INC"  # first listing (the issuer) wins
    assert first.cik == "9000001"
    assert first.date_filed == date(2026, 3, 10)
    assert first.path == "edgar/data/9000001/0000000000-26-000001.txt"
    assert entries[3].company_name == "Placeholder Ticker  Corp  /DE/"  # embedded double spaces survive


def test_parse_form_idx_other_forms_and_quarterly_date_format():
    txt = (SAMPLES / "form.20260310.idx").read_text()
    assert {e.form_type for e in ix.parse_form_idx(txt, forms=("144", "SC 13D"))} == {"144", "SC 13D"}
    q = txt.replace("20260310", "2026-03-10")
    assert ix.parse_form_idx(q)[0].date_filed == date(2026, 3, 10)


def test_parse_form_idx_requires_header():
    with pytest.raises(ValueError):
        ix.parse_form_idx("nothing useful here\n")


def test_fetch_daily_index_404_is_empty_list(tmp_path):
    c = EdgarClient(
        user_agent="x",
        cache_dir=tmp_path,
        transport=httpx.MockTransport(lambda r: httpx.Response(404)),
        sleep=lambda s: None,
    )
    assert ix.fetch_daily_index(c, date(2026, 3, 14)) == []


def test_xml_candidates_skip_xsl_index_and_non_xml():
    listing = json.loads((SAMPLES / "index.json").read_text())
    assert ix.xml_candidates(listing) == ["form4.xml"]
    tricky = {
        "directory": {
            "item": [
                {"name": "xslF345X05/form4.xml"},
                {"name": "FilingSummary.xml"},
                {"name": "a-index.xml"},
                {"name": "primary_doc.xml"},
                {"name": "wf-form4_1.xml"},
            ]
        }
    }
    assert ix.xml_candidates(tricky) == ["wf-form4_1.xml", "primary_doc.xml"]


def test_is_ownership_document():
    assert ix.is_ownership_document((FIX / f"{ACC}.xml").read_bytes())
    assert not ix.is_ownership_document(b"<html><body>rendered</body></html>")


def test_acceptance_datetime_est_and_edt_to_naive_utc():
    html = (SAMPLES / f"{ACC}-index-headers.html").read_text()
    assert ix.parse_acceptance_datetime(html) == datetime(2026, 3, 10, 20, 30, 12)  # EDT (UTC-4)
    assert ix.parse_acceptance_datetime("<ACCEPTANCE-DATETIME>20261105111501") == datetime(
        2026, 11, 5, 16, 15, 1
    )  # EST
    assert ix.parse_acceptance_datetime("ACCEPTANCE-DATETIME:\t20260115093000") == datetime(2026, 1, 15, 14, 30)
    assert ix.parse_acceptance_datetime("no header") is None


def _handler(counter):
    xml = (FIX / f"{ACC}.xml").read_bytes()

    def handler(req):
        counter.append(req.url.path)
        p = req.url.path
        if p.endswith("/index.json"):
            return httpx.Response(200, content=(SAMPLES / "index.json").read_bytes())
        if p.endswith("/form4.xml"):
            return httpx.Response(200, content=xml)
        if p.endswith("-index-headers.html"):
            return httpx.Response(200, content=(SAMPLES / f"{ACC}-index-headers.html").read_bytes())
        return httpx.Response(404)

    return handler


def test_fetch_filing_then_load_filing_fully_offline_from_cache(tmp_path):
    calls = []
    c = EdgarClient(
        user_agent="x", cache_dir=tmp_path, transport=httpx.MockTransport(_handler(calls)), sleep=lambda s: None
    )
    doc = ix.fetch_filing(c, "9000001", ACC)
    assert doc.xml_name == "form4.xml"
    assert doc.accepted_at == datetime(2026, 3, 10, 20, 30, 12)
    assert len(calls) == 3
    f = ix.load_filing(c, 9000001, ACC)  # second call: cache only
    assert len(calls) == 3
    assert f.accepted_at == datetime(2026, 3, 10, 20, 30, 12)
    assert f.issuer_ticker == "ACME" and f.non_derivative[0].code == "P"
    assert (tmp_path / f"{ACC}.xml").exists() and (tmp_path / f"{ACC}.meta.json").exists()


def test_fetch_filing_skips_html_candidates_that_are_not_ownership_documents(tmp_path):
    listing = {"directory": {"item": [{"name": "a4.xml"}, {"name": "b4.xml"}]}}
    xml = (FIX / f"{ACC}.xml").read_bytes()

    def handler(req):
        p = req.url.path
        if p.endswith("index.json"):
            return httpx.Response(200, json=listing)
        if p.endswith("a4.xml"):
            return httpx.Response(200, content=b"<html>rendered</html>")
        if p.endswith("b4.xml"):
            return httpx.Response(200, content=xml)
        return httpx.Response(404)

    c = EdgarClient(user_agent="x", cache_dir=tmp_path, transport=httpx.MockTransport(handler), sleep=lambda s: None)
    doc = ix.fetch_filing(c, 9000001, ACC)
    assert doc.xml_name == "b4.xml" and doc.accepted_at is None  # headers 404 -> accepted_at unknown, no crash


def test_fetch_filing_unknown_folder(tmp_path):
    c = EdgarClient(
        user_agent="x",
        cache_dir=tmp_path,
        transport=httpx.MockTransport(lambda r: httpx.Response(404)),
        sleep=lambda s: None,
    )
    with pytest.raises(FilingResolutionError):
        ix.fetch_filing(c, 9000001, ACC)


def test_filing_folder_url_strips_cik_padding():
    assert ix.filing_folder_url("0009000001", ACC) == FOLDER


def test_fixture_meta_json_works_as_cache_for_load_filing():
    """The real-filing fixtures ship .xml + .meta.json, so they can be loaded through the cache path offline."""
    acc = "0001193125-26-449032"
    c = EdgarClient(user_agent="x", cache_dir=FIX, transport=httpx.MockTransport(lambda r: pytest.fail("network")))
    f = ix.load_filing(c, 1068851, acc)
    assert f.accepted_at == datetime(2026, 11, 5, 16, 15, 1) and f.issuer_ticker == "PB"
