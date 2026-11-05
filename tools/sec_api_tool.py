"""sec-api.io Insider Trading API client (Form 4).

Verified against the live API: query field is `documentType`, results live under `transactions`,
ticker is `issuer.tradingSymbol`, and numbers are nested under `amounts`. Page size is capped at 50.
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Callable

from sec_api import InsiderTradingApi

from config.settings import settings as default_settings
from telemetry.otel_setup import tracer

logger = logging.getLogger(__name__)

PAGE_SIZE = 50
SIGNAL_CODES = ("P", "S")  # open-market purchase / sale: the informative insider signals
_PLAN_RE = re.compile(r"10b5-?1", re.IGNORECASE)
_NEGATION_RE = re.compile(r"\b(not|no|never|outside)\b", re.IGNORECASE)


def mentions_trading_plan(text: str) -> bool:
    """True if the text says a trade was made under a Rule 10b5-1 plan (ignores 'not pursuant to ...')."""
    for m in _PLAN_RE.finditer(text or ""):
        if not _NEGATION_RE.search(text[max(0, m.start() - 70):m.start()]):
            return True
    return False


class UpstreamAPIError(Exception):
    pass


class ParsingError(Exception):
    pass


def _utc_naive(iso: str | None) -> datetime | None:
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _num(x) -> float | None:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


class SecApiTool:
    def __init__(self, settings=default_settings):
        self.settings = settings
        self.api = InsiderTradingApi(api_key=settings.sec_api_key)
        self._last_request = 0.0

    # ── fetching ────────────────────────────────────────────────────────────
    def _query(self, hours: int, offset: int) -> dict:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=hours)
        codes = " OR ".join(SIGNAL_CODES)
        q = (f'documentType:"4" AND filedAt:[{start:%Y-%m-%dT%H:%M:%S} TO {end:%Y-%m-%dT%H:%M:%S}] '
             f"AND nonDerivativeTable.transactions.coding.code:({codes})")
        return {"query": q, "from": str(offset), "size": str(PAGE_SIZE), "sort": [{"filedAt": {"order": "desc"}}]}

    def _get_page(self, hours: int, offset: int) -> list[dict]:
        delays = [1, 3, 6]
        for attempt in range(len(delays) + 1):
            try:
                wait = 0.12 - (time.time() - self._last_request)
                if wait > 0:
                    time.sleep(wait)
                self._last_request = time.time()
                resp = self.api.get_data(self._query(hours, offset))
                return resp.get("transactions", []) if isinstance(resp, dict) else []
            except Exception as e:  # noqa: BLE001
                if attempt == len(delays):
                    raise UpstreamAPIError(f"SEC API failed after retries: {e}") from e
                time.sleep(delays[attempt])
        return []

    def fetch_recent_filings(self, hours: int | None = None, max_pages: int | None = None,
                             page_is_known: Callable[[list[dict]], bool] | None = None) -> list[dict]:
        """Newest-first pagination. Stops early when `page_is_known(page)` says we've caught up."""
        hours = hours or self.settings.filing_lookback_hours
        max_pages = max_pages or self.settings.sec_max_pages
        out: list[dict] = []
        with tracer.start_as_current_span("sec_api.fetch_filings") as span:
            span.set_attribute("sec.hours", hours)
            for page in range(max_pages):
                rows = self._get_page(hours, page * PAGE_SIZE)
                out.extend(rows)
                if len(rows) < PAGE_SIZE:
                    break
                if page_is_known and page_is_known(rows):
                    break
            span.set_attribute("sec.filings_fetched", len(out))
        return out

    # ── parsing ─────────────────────────────────────────────────────────────
    def parse_filing(self, raw: dict) -> tuple[dict, list[dict]]:
        """Return (filing_row, [transaction_rows]) keeping only P/S rows that have a usable price."""
        try:
            acc = raw["accessionNo"]
            issuer = raw.get("issuer") or {}
            owner = raw.get("reportingOwner") or {}
            if isinstance(owner, list):  # joint filings
                owner = owner[0] if owner else {}
            rel = owner.get("relationship") or {}

            if rel.get("officerTitle"):
                title = rel["officerTitle"]
            elif rel.get("isDirector"):
                title = "Director"
            elif rel.get("isTenPercentOwner"):
                title = "10% Owner"
            else:
                title = rel.get("otherText") or "Insider"

            footnotes = " ".join(f.get("text", "") for f in raw.get("footnotes") or [])
            planned = mentions_trading_plan(footnotes) or mentions_trading_plan(raw.get("remarks") or "")

            filing = {
                "accession_number": acc,
                "filing_date": _utc_naive(raw.get("filedAt")),
                "issuer_name": issuer.get("name"),
                "issuer_ticker": (issuer.get("tradingSymbol") or "").strip().upper() or None,
                "issuer_cik": issuer.get("cik"),
                "insider_name": owner.get("name"),
                "insider_title": title,
                "is_officer": bool(rel.get("isOfficer")),
                "is_director": bool(rel.get("isDirector")),
                "is_ten_pct": bool(rel.get("isTenPercentOwner")),
                "is_10b5_1": planned,
                "form_type": raw.get("documentType", "4"),
            }

            txns = []
            for idx, t in enumerate((raw.get("nonDerivativeTable") or {}).get("transactions") or []):
                code = (t.get("coding") or {}).get("code")
                if code not in SIGNAL_CODES:
                    continue
                amt = t.get("amounts") or {}
                shares, price = _num(amt.get("shares")), _num(amt.get("pricePerShare"))
                if not shares or not price or shares <= 0 or price <= 0:
                    continue  # no usable price (e.g. price given in a footnote)
                txns.append({
                    "accession_number": acc,
                    "row_idx": idx,
                    "transaction_date": (t.get("transactionDate") or "")[:10] or None,
                    "security_title": t.get("securityTitle"),
                    "code": code,
                    "acquired_disposed": amt.get("acquiredDisposedCode"),
                    "shares": shares,
                    "price_per_share": price,
                    "dollar_value": round(shares * price, 2),
                    "ownership_nature": (t.get("ownershipNature") or {}).get("directOrIndirectOwnership"),
                    "post_transaction_shares": _num((t.get("postTransactionAmounts") or {}).get("sharesOwnedFollowingTransaction")),
                })
            return filing, txns
        except Exception as e:  # noqa: BLE001
            raise ParsingError(f"Error parsing filing {raw.get('accessionNo')}: {e}") from e
