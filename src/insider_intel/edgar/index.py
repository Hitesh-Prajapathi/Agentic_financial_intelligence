"""Discovery: EDGAR daily form index, accession -> primary Form 4 XML, acceptance datetime.

Formats implemented (from the documented EDGAR layouts; see tests/edgar/fixtures/README.md for
what was and was not verified against live EDGAR):

* Daily form index ``/Archives/edgar/daily-index/{YYYY}/QTR{q}/form.{YYYYMMDD}.idx`` - fixed-width
  columns "Form Type | Company Name | CIK | Date Filed | File Name" under a dashed rule. EDGAR lists a
  filing once per associated filer (issuer and each reporting owner), so entries are de-duplicated
  by accession and the CIK may be an owner's rather than the issuer's.
* Filing folder ``/Archives/edgar/data/{cik}/{accession-no-dashes}/index.json`` - directory listing;
  the ownership document is the root-level ``.xml`` whose root element is ``ownershipDocument``
  (NOT the ``xslF345X0N/...`` rendered path).
* ``{accession}-index-headers.html`` carries ``<ACCEPTANCE-DATETIME>YYYYMMDDHHMMSS`` in US/Eastern.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from .client import EdgarClient, EdgarError, EdgarNotFound
from .form4 import parse_form4
from .models import Filing

BASE = "https://www.sec.gov/Archives/edgar"
FORM4_FORMS = ("4", "4/A")
_EASTERN = ZoneInfo("America/New_York")


class FilingResolutionError(EdgarError):
    """Could not locate the ownership XML for a filing."""


@dataclass(frozen=True)
class IndexEntry:
    form_type: str
    company_name: str
    cik: str  # unpadded; may be the issuer's OR a reporting owner's CIK
    date_filed: date
    path: str  # e.g. edgar/data/1234567/0001234567-26-000123.txt
    accession: str  # e.g. 0001234567-26-000123


def quarter_of(d: date) -> int:
    return (d.month - 1) // 3 + 1


def daily_index_url(d: date) -> str:
    return f"{BASE}/daily-index/{d.year}/QTR{quarter_of(d)}/form.{d:%Y%m%d}.idx"


def accession_from_path(path: str) -> str | None:
    m = re.search(r"(\d{10}-\d{2}-\d{6})", path)
    return m.group(1) if m else None


def _parse_filed(s: str) -> date:
    s = s.strip()
    return datetime.strptime(s, "%Y-%m-%d" if "-" in s else "%Y%m%d").date()


_ROW_RE = re.compile(
    r"^(?P<form>\S.*?)\s{2,}(?P<company>.+?)\s+(?P<cik>\d+)\s+(?P<date>\d{8}|\d{4}-\d{2}-\d{2})\s+(?P<path>\S+)\s*$"
)


def parse_form_idx(text: str, forms: tuple[str, ...] = FORM4_FORMS) -> list[IndexEntry]:
    """Parse a form.idx (daily or quarterly) into entries for the given form types, de-duplicated by accession."""
    lines = text.splitlines()
    cols = None
    start = 0
    for i, ln in enumerate(lines):
        if ln.startswith("Form Type") and "Company Name" in ln and "CIK" in ln:
            cols = (ln.index("Company Name"), ln.index("CIK"), ln.index("Date Filed"), ln.index("File Name"))
            start = i + 1
            break
    if cols is None:
        raise ValueError("form.idx header line ('Form Type  Company Name  CIK  Date Filed  File Name') not found")
    if start < len(lines) and set(lines[start].strip()) == {"-"}:
        start += 1

    out: list[IndexEntry] = []
    seen: set[str] = set()
    for ln in lines[start:]:
        if not ln.strip():
            continue
        c_company, c_cik, c_date, c_path = cols
        form = ln[:c_company].strip()
        company = ln[c_company:c_cik].strip()
        cik = ln[c_cik:c_date].strip()
        filed = ln[c_date:c_path].strip()
        path = ln[c_path:].strip()
        if not (cik.isdigit() and re.fullmatch(r"\d{8}|\d{4}-\d{2}-\d{2}", filed)):
            m = _ROW_RE.match(ln)  # column drift: fall back to a right-anchored regex
            if not m:
                continue
            form, company, cik, filed, path = (m["form"].strip(), m["company"].strip(), m["cik"], m["date"], m["path"])
        if form not in forms:
            continue
        acc = accession_from_path(path)
        if not acc or acc in seen:
            continue
        seen.add(acc)
        out.append(IndexEntry(form, company, cik.lstrip("0") or "0", _parse_filed(filed), path, acc))
    return out


def fetch_daily_index(client: EdgarClient, d: date, forms: tuple[str, ...] = FORM4_FORMS) -> list[IndexEntry]:
    """Form 4 / 4/A entries filed on `d`. Weekends/holidays (no index file, 404) yield []."""
    try:
        return parse_form_idx(client.get_text(daily_index_url(d)), forms)
    except EdgarNotFound:
        return []


# --- filing folder -> ownership XML --------------------------------------------------------
def filing_folder_url(cik: str | int, accession: str) -> str:
    return f"{BASE}/data/{int(cik)}/{accession.replace('-', '')}"


def xml_candidates(index_json: dict) -> list[str]:
    """Root-level .xml file names from a folder index.json, best candidate first (never xsl-rendered paths)."""
    items = (index_json.get("directory") or {}).get("item") or []
    names = [str(it.get("name", "")) for it in items]
    ok = [
        n
        for n in names
        if n.lower().endswith(".xml")
        and "/" not in n
        and not n.lower().startswith("xsl")
        and "-index" not in n.lower()
        and n.lower() != "filingsummary.xml"
    ]
    # Prefer names that look like Form 4 documents (form4.xml, doc4.xml, wf-form4_*.xml, ...).
    return sorted(ok, key=lambda n: (0 if "4" in n.lower() else 1, n))


_OWNERSHIP_ROOT = re.compile(rb"<ownershipDocument[\s>]")


def is_ownership_document(content: bytes) -> bool:
    return bool(_OWNERSHIP_ROOT.search(content[:20000]))


_ACCEPT_RE = re.compile(r"ACCEPTANCE-DATETIME(?:>|&gt;|:)\s*(\d{14})")


def parse_acceptance_datetime(text: str) -> datetime | None:
    """ACCEPTANCE-DATETIME (YYYYMMDDHHMMSS, US/Eastern) from an -index-headers.html or .txt header -> naive UTC."""
    m = _ACCEPT_RE.search(text or "")
    if not m:
        return None
    local = datetime.strptime(m.group(1), "%Y%m%d%H%M%S").replace(tzinfo=_EASTERN)
    return local.astimezone(timezone.utc).replace(tzinfo=None)


@dataclass
class FilingDocument:
    accession: str
    cik: str
    xml: bytes
    accepted_at: datetime | None
    xml_name: str | None


def fetch_filing(client: EdgarClient, cik: str | int, accession: str) -> FilingDocument:
    """Fetch (or load from the immutable cache) the ownership XML and acceptance time of a filing.

    `cik` may be any CIK associated with the filing (issuer or reporting owner). Network use on a
    cold cache: folder index.json, the XML, and -index-headers.html.
    """
    folder = filing_folder_url(cik, accession)
    xml = client.read_cached(accession, ".xml")
    meta = None
    raw_meta = client.read_cached(accession, ".meta.json")
    if raw_meta:
        meta = json.loads(raw_meta)

    xml_name = (meta or {}).get("xml_name")
    if xml is None:
        try:
            listing = client.get_json(f"{folder}/index.json")
        except EdgarNotFound as exc:
            raise FilingResolutionError(f"no filing folder for {accession} under CIK {cik}") from exc
        for name in xml_candidates(listing):
            content = client.get(f"{folder}/{name}").content
            if is_ownership_document(content):
                xml, xml_name = content, name
                client.write_cached(accession, xml, ".xml")
                break
        if xml is None:
            raise FilingResolutionError(f"no ownershipDocument XML found in {folder}")

    accepted_at = None
    if meta and meta.get("accepted_at"):
        accepted_at = datetime.fromisoformat(meta["accepted_at"])
    else:
        try:
            accepted_at = parse_acceptance_datetime(client.get_text(f"{folder}/{accession}-index-headers.html"))
        except EdgarNotFound:
            accepted_at = None
        if accepted_at is not None and client.cache_dir is not None:
            client.write_cached(
                accession,
                json.dumps({"cik": str(cik), "xml_name": xml_name, "accepted_at": accepted_at.isoformat()}).encode(),
                ".meta.json",
            )
    return FilingDocument(accession, str(cik), xml, accepted_at, xml_name)


def load_filing(client: EdgarClient, cik: str | int, accession: str) -> Filing:
    """fetch_filing + parse_form4 in one call."""
    doc = fetch_filing(client, cik, accession)
    return parse_form4(doc.xml, accession, doc.accepted_at)


# --- single-request path: the full submission .txt -----------------------------------------
_OWNERSHIP_DOC = re.compile(rb"<ownershipDocument\b.*?</ownershipDocument>", re.DOTALL)


def submission_txt_url(cik: str | int, accession: str) -> str:
    """Full-submission text file: SEC-HEADER (with ACCEPTANCE-DATETIME) + the ownership XML in one document."""
    return f"{BASE}/data/{int(cik)}/{accession}.txt"


def fetch_filing_txt(client: EdgarClient, cik: str | int, accession: str) -> FilingDocument:
    """Like `fetch_filing` but ONE request per filing on a cold cache (the submission .txt).

    The ownershipDocument is carved out of the SGML wrapper and cached exactly as `fetch_filing` caches it
    (`<accession>.xml` + `.meta.json`), so the two paths share one cache and a warm cache needs no network.
    Falls back to the folder-based `fetch_filing` if the .txt does not exist (404).
    """
    cached_xml = client.read_cached(accession, ".xml")
    raw_meta = client.read_cached(accession, ".meta.json")
    if cached_xml is not None and raw_meta:
        meta = json.loads(raw_meta)
        acc_at = datetime.fromisoformat(meta["accepted_at"]) if meta.get("accepted_at") else None
        return FilingDocument(accession, str(cik), cached_xml, acc_at, meta.get("xml_name"))
    try:
        data = client.get(submission_txt_url(cik, accession)).content
    except EdgarNotFound:
        return fetch_filing(client, cik, accession)
    m = _OWNERSHIP_DOC.search(data)
    if not m:
        raise FilingResolutionError(f"no ownershipDocument in submission text of {accession}")
    xml = m.group(0)
    accepted_at = parse_acceptance_datetime(data[:20000].decode("utf-8", "replace"))
    if client.cache_dir is not None:
        client.write_cached(accession, xml, ".xml")
        client.write_cached(
            accession,
            json.dumps(
                {"cik": str(cik), "xml_name": None, "accepted_at": accepted_at.isoformat() if accepted_at else None}
            ).encode(),
            ".meta.json",
        )
    return FilingDocument(accession, str(cik), xml, accepted_at, None)


def load_filing_txt(client: EdgarClient, cik: str | int, accession: str) -> Filing:
    """fetch_filing_txt + parse_form4."""
    doc = fetch_filing_txt(client, cik, accession)
    return parse_form4(doc.xml, accession, doc.accepted_at)
