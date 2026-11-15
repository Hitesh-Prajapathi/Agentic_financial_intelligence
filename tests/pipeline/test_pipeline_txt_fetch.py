"""One-request-per-filing fetch via the full-submission .txt. Offline (httpx MockTransport)."""

from datetime import datetime
from pathlib import Path

import httpx

from insider_intel.edgar.client import EdgarClient
from insider_intel.edgar.index import fetch_filing_txt, load_filing_txt

ACC = "0000000000-26-000001"
XML = (Path(__file__).resolve().parent.parent / "edgar" / "fixtures" / f"{ACC}.xml").read_bytes()
TXT = (
    b"<SEC-DOCUMENT>"
    + ACC.encode()
    + b".txt : 20260310163012\n<SEC-HEADER>"
    + ACC.encode()
    + b".hdr.sgml : 20260310163012\n<ACCEPTANCE-DATETIME>20260310163012\nACCESSION NUMBER:\t\t"
    + ACC.encode()
    + b"\n</SEC-HEADER>\n<DOCUMENT>\n<TYPE>4\n<SEQUENCE>1\n<FILENAME>form4.xml\n<TEXT>\n<XML>\n"
    + XML
    + b"\n</XML>\n</TEXT>\n</DOCUMENT>\n</SEC-DOCUMENT>\n"
)


def _client(tmp_path, handler):
    return EdgarClient(
        user_agent="unit-test-agent contact@test.invalid",
        cache_dir=tmp_path,
        transport=httpx.MockTransport(handler),
        sleep=lambda s: None,
    )


def test_one_request_per_cold_filing_then_zero(tmp_path):
    seen = []

    def handler(req):
        seen.append(str(req.url))
        return httpx.Response(200, content=TXT)

    c = _client(tmp_path, handler)
    f = load_filing_txt(c, "9000001", ACC)
    assert seen == [f"https://www.sec.gov/Archives/edgar/data/9000001/{ACC}.txt"]
    assert f.accession_number == ACC and f.issuer_cik and f.owners
    # 16:30:12 US/Eastern on 10 Mar 2026 (EDT) = 20:30:12 UTC
    assert f.accepted_at == datetime(2026, 3, 10, 20, 30, 12)
    assert (tmp_path / f"{ACC}.xml").exists() and (tmp_path / f"{ACC}.meta.json").exists()
    f2 = load_filing_txt(c, "9000001", ACC)  # warm cache: no network
    assert len(seen) == 1 and f2 == f


def test_falls_back_to_folder_route_when_txt_is_missing(tmp_path):
    def handler(req):
        url = str(req.url)
        if url.endswith(".txt"):
            return httpx.Response(404)
        if url.endswith("index.json"):
            return httpx.Response(200, json={"directory": {"item": [{"name": "form4.xml"}]}})
        if url.endswith("form4.xml"):
            return httpx.Response(200, content=XML)
        if url.endswith("-index-headers.html"):
            return httpx.Response(200, text="ACCEPTANCE-DATETIME>20260310163012")
        return httpx.Response(404)

    doc = fetch_filing_txt(_client(tmp_path, handler), "9000001", ACC)
    assert doc.xml == XML and doc.accepted_at == datetime(2026, 3, 10, 20, 30, 12)
