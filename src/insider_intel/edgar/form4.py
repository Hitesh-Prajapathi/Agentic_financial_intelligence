"""Form 4 / 4/A ownership-document parser (stdlib ElementTree only).

Input is the raw ownershipDocument XML as filed on EDGAR. Handles values wrapped in
``<value>`` children, ``<footnoteId id=".."/>`` elements, missing optional nodes,
comma-formatted numbers, namespace-free (and namespaced) documents, joint filers,
amendments and derivative-only filings.

CIKs are normalised to unpadded strings (EDGAR XML pads them to 10 digits; the daily
index and company_tickers.json do not).
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import date, datetime

from .models import Filing, NonDerivTxn, ReportingOwner


class Form4ParseError(Exception):
    """The document is not a parseable Form 4 ownershipDocument."""


# --- Rule 10b5-1 text fallback (same logic as the legacy tools/sec_api_tool.py) ------------
_PLAN_RE = re.compile(r"10b5-?1", re.IGNORECASE)
_NEGATION_RE = re.compile(r"\b(not|no|never|outside)\b", re.IGNORECASE)
_NO_TICKER = {"", "NONE", "N/A", "NA", "-"}  # placeholders seen in real filings (SEC 2025Q3 dataset)
_ACC_RE = re.compile(r"^(\d{10})-?(\d{2})-?(\d{6})$")
_TRUE = {"1", "true", "y", "yes"}


def mentions_trading_plan(text: str) -> bool:
    """True if the text says a trade was made under a Rule 10b5-1 plan (ignores 'not pursuant to ...')."""
    for m in _PLAN_RE.finditer(text or ""):
        if not _NEGATION_RE.search(text[max(0, m.start() - 70):m.start()]):
            return True
    return False


# --- small helpers ------------------------------------------------------------------------
def _strip_ns(root: ET.Element) -> None:
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.rsplit("}", 1)[1]


def _text(el: ET.Element | None) -> str | None:
    if el is None:
        return None
    t = (el.text or "").strip()
    return t or None


def _val(parent: ET.Element | None, tag: str) -> str | None:
    """Text of parent/tag, unwrapping a <value> child when present."""
    if parent is None:
        return None
    el = parent.find(tag)
    if el is None:
        return None
    v = el.find("value")
    return _text(v) if v is not None else _text(el)


def _fn_ids(parent: ET.Element | None, tag: str) -> list[str]:
    """footnoteId ids attached directly to parent/tag (shares and price footnotes live on different tags)."""
    if parent is None:
        return []
    el = parent.find(tag)
    if el is None:
        return []
    return [f.get("id") for f in el.findall("footnoteId") if f.get("id")]


def _num(s: str | None) -> float | None:
    if s is None:
        return None
    s = s.replace(",", "").replace("$", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _bool(s: str | None) -> bool | None:
    if s is None:
        return None
    return s.strip().lower() in _TRUE


def _date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s.strip()[:10])  # tolerate '2026-11-04-05:00' tz suffixes
    except ValueError:
        return None


def _cik(s: str | None) -> str | None:
    if not s or not s.strip():
        return None
    return s.strip().lstrip("0") or "0"


def _norm_accession(acc: str) -> str:
    m = _ACC_RE.match(acc.strip())
    return "-".join(m.groups()) if m else acc.strip()


def _norm_ticker(s: str | None) -> str | None:
    if s is None:
        return None
    t = s.strip().upper()
    return None if t in _NO_TICKER else t


def _upper1(s: str | None) -> str | None:
    return s.strip().upper() or None if s else None


def _load_root(xml: str | bytes) -> ET.Element:
    data = xml.encode("utf-8") if isinstance(xml, str) else xml
    data = data.lstrip(b"\xef\xbb\xbf").lstrip()
    if not data:
        raise Form4ParseError("empty document")
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        # full-submission .txt files wrap the XML in SGML; try to carve the document out
        m = re.search(rb"<ownershipDocument\b.*?</ownershipDocument>", data, re.DOTALL)
        if not m:
            raise Form4ParseError(f"not well-formed XML: {exc}") from exc
        try:
            root = ET.fromstring(m.group(0))
        except ET.ParseError as exc2:
            raise Form4ParseError(f"not well-formed XML: {exc2}") from exc2
    _strip_ns(root)
    if root.tag != "ownershipDocument":
        raise Form4ParseError(f"root element is <{root.tag}>, expected <ownershipDocument>")
    return root


def _find_checkbox(root: ET.Element) -> bool | None:
    """Rule 10b5-1 checkbox. Current schemas (Form 4 since April 2023) call it <aff10b5One>."""
    el = root.find("aff10b5One")
    if el is None:  # tolerate variant naming / placement
        for child in root.iter():
            if child is not root and "10b5" in child.tag.lower() and len(child) == 0:
                el = child
                break
    txt = _text(el)
    return _bool(txt) if txt is not None else None


def _parse_owner(el: ET.Element) -> ReportingOwner:
    rid = el.find("reportingOwnerId")
    rel = el.find("reportingOwnerRelationship")
    cik = _cik(_val(rid, "rptOwnerCik"))
    if not cik:
        raise Form4ParseError("reportingOwner without rptOwnerCik")
    return ReportingOwner(
        cik=cik,
        name=_val(rid, "rptOwnerName"),
        is_director=bool(_bool(_val(rel, "isDirector"))),
        is_officer=bool(_bool(_val(rel, "isOfficer"))),
        is_ten_pct=bool(_bool(_val(rel, "isTenPercentOwner"))),
        is_other=bool(_bool(_val(rel, "isOther"))),
        officer_title=_val(rel, "officerTitle"),
        other_text=_val(rel, "otherText"),
    )


def _parse_txn(idx: int, t: ET.Element) -> NonDerivTxn:
    coding = t.find("transactionCoding")
    amounts = t.find("transactionAmounts")
    post = t.find("postTransactionAmounts")
    nature = t.find("ownershipNature")
    return NonDerivTxn(
        row_idx=idx,
        security_title=_val(t, "securityTitle"),
        transaction_date=_date(_val(t, "transactionDate")),
        form_type_in_txn=_val(coding, "transactionFormType"),
        code=_upper1(_val(coding, "transactionCode")),
        equity_swap=_bool(_val(coding, "equitySwapInvolved")),
        shares=_num(_val(amounts, "transactionShares")),
        price_per_share=_num(_val(amounts, "transactionPricePerShare")),
        shares_footnote_ids=_fn_ids(amounts, "transactionShares"),
        price_footnote_ids=_fn_ids(amounts, "transactionPricePerShare"),
        acquired_disposed=_upper1(_val(amounts, "transactionAcquiredDisposedCode")),
        post_transaction_shares=_num(_val(post, "sharesOwnedFollowingTransaction")),
        direct_indirect=_upper1(_val(nature, "directOrIndirectOwnership")),
        nature_of_ownership=_val(nature, "natureOfOwnership"),
    )


def parse_form4(xml: str | bytes, accession_number: str, accepted_at: datetime | None = None) -> Filing:
    """Parse an ownershipDocument into a Filing. Raises Form4ParseError on unusable input."""
    root = _load_root(xml)

    issuer = root.find("issuer")
    issuer_cik = _cik(_val(issuer, "issuerCik"))
    if not issuer_cik:
        raise Form4ParseError("missing issuerCik")

    owners = [_parse_owner(o) for o in root.findall("reportingOwner")]
    if not owners:
        raise Form4ParseError("no reportingOwner")

    txns = [
        _parse_txn(i, t)
        for i, t in enumerate(root.findall("nonDerivativeTable/nonDerivativeTransaction"))
    ]

    footnotes: dict[str, str] = {}
    for f in root.findall("footnotes/footnote"):
        fid = f.get("id")
        if fid:
            footnotes[fid] = " ".join("".join(f.itertext()).split())

    rem_el = root.find("remarks")
    remarks = " ".join("".join(rem_el.itertext()).split()) or None if rem_el is not None else None
    doc_type = (_val(root, "documentType") or "4").strip()
    checkbox = _find_checkbox(root)
    text_blob = " ".join(list(footnotes.values()) + ([remarks] if remarks else []))
    is_plan = checkbox if checkbox is not None else mentions_trading_plan(text_blob)

    price_in_fn = any(
        t.code in ("P", "S") and t.price_per_share is None and bool(t.price_footnote_ids) for t in txns
    )

    return Filing(
        accession_number=_norm_accession(accession_number),
        form_type=doc_type,
        schema_version=_val(root, "schemaVersion"),
        accepted_at=accepted_at,
        period_of_report=_date(_val(root, "periodOfReport")),
        date_of_original_submission=_date(_val(root, "dateOfOriginalSubmission")),
        issuer_cik=issuer_cik,
        issuer_name=_val(issuer, "issuerName"),
        issuer_ticker=_norm_ticker(_val(issuer, "issuerTradingSymbol")),
        owners=owners,
        non_derivative=txns,
        footnotes=footnotes,
        remarks=remarks,
        aff_10b5_1=checkbox,
        is_10b5_1=bool(is_plan),
        price_in_footnote=price_in_fn,
    )


def filing_to_rows(f: Filing) -> list[dict]:
    """One flat dict per non-derivative transaction (all transaction codes)."""
    owner_ciks = "|".join(sorted({o.cik for o in f.owners}))
    return [
        {
            "accession_number": f.accession_number,
            "row_idx": t.row_idx,
            "form_type": f.form_type,
            "accepted_at": f.accepted_at,
            "issuer_cik": f.issuer_cik,
            "issuer_ticker": f.issuer_ticker,
            "owner_ciks": owner_ciks,
            "transaction_date": t.transaction_date,
            "code": t.code,
            "shares": t.shares,
            "price_per_share": t.price_per_share,
            "acquired_disposed": t.acquired_disposed,
            "post_transaction_shares": t.post_transaction_shares,
            "direct_indirect": t.direct_indirect,
            "security_title": t.security_title,
        }
        for t in f.non_derivative
    ]
