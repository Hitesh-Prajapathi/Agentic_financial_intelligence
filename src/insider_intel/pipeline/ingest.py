"""Form 4 / 4/A ingestion from SEC EDGAR (replaces sec-api.io). No API key; needs SEC_USER_AGENT.

Flow
  1. Discovery: the EDGAR daily form index, one file per business day, from the watermark (last fully
     ingested index date) + 1 up to today. The very first run starts `edgar_initial_lookback_days` back.
  2. Fetch + parse: ONE request per new filing (the submission .txt carries the header and the XML), cached
     write-once on disk, parsed with `insider_intel.edgar.form4`. Already-stored accessions are skipped.
  3. Store ALL transaction codes. Ranking filters to P/S itself.
  4. Mark amended rows, rebuild the insider graph, advance the watermark.

Resumability. Filings are committed in batches, the watermark only advances over index days that were
FULLY ingested (no transient failure, not stopped by the cap / time budget) and strictly before today, and
re-ingesting a stored accession is a no-op. A run killed at any point resumes cleanly on the next run, and
re-running a window yields identical derived tables.

Mapping EDGAR -> tables
  filings          accession, filing_date := accepted_at (index date if EDGAR gave none), issuer_*, form_type,
                   insider_* / role flags of the PRIMARY (first) owner, is_10b5_1, accepted_at, owner_ciks
                   ('|'-joined CIKs of ALL owners), price_in_footnote, is_late, date_of_original_submission
  reporting_owners one row per reporting owner (CIK, name, role flags, officer title)
  transactions     one row per non-derivative row, every code; direct/indirect -> ownership_nature
  footnotes        footnote id -> text
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import date, datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo

from insider_intel.config import settings as default_settings
from insider_intel.edgar.client import EdgarBlockedError, EdgarClient, EdgarConfigError, EdgarError
from insider_intel.edgar.form4 import Form4ParseError
from insider_intel.edgar.index import FilingResolutionError, IndexEntry, fetch_daily_index, load_filing_txt
from insider_intel.edgar.models import Filing
from insider_intel.edgar.quality import is_late
from insider_intel.obs import timed
from insider_intel.store.graph import rebuild_insider_edges

logger = logging.getLogger(__name__)

SIGNAL_CODES = ("P", "S")  # open-market purchase / sale: the informative insider signals (used by ranking)
_EASTERN = ZoneInfo("America/New_York")
MAX_CONSECUTIVE_TRANSIENT = 25  # this many fetch failures in a row = SEC is down/throttling us; abort the run


class DBWriteError(Exception):
    pass


class IngestAborted(Exception):
    """SEC is unreachable or blocking us; nothing more can be fetched this run."""


# ── source ──────────────────────────────────────────────────────────────────
class EdgarSource:
    """What `ingest` needs from EDGAR: `entries(day)` and `load(entry)`. The HTTP client is built lazily so
    that demo mode / the UI work without SEC_USER_AGENT."""

    def __init__(self, settings=default_settings, client: EdgarClient | None = None):
        self.settings = settings
        self._client = client

    @property
    def client(self) -> EdgarClient:
        if self._client is None:
            if not self.settings.has_sec_user_agent:
                raise EdgarConfigError(
                    "SEC_USER_AGENT is not set (or is still the placeholder). SEC requires a descriptive "
                    "User-Agent with contact info, e.g. SEC_USER_AGENT='your-app-name you@example.com'")
            self._client = EdgarClient(user_agent=self.settings.sec_user_agent, cache_dir=self.settings.edgar_cache_dir)
        return self._client

    def entries(self, day: date) -> list[IndexEntry]:
        return fetch_daily_index(self.client, day)

    def load(self, entry: IndexEntry) -> Filing:
        return load_filing_txt(self.client, entry.cik, entry.accession)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()


# ── EDGAR -> rows ───────────────────────────────────────────────────────────
def _primary_title(o) -> str:
    if o.officer_title:
        return o.officer_title
    if o.is_director:
        return "Director"
    if o.is_ten_pct:
        return "10% Owner"
    return o.other_text or "Insider"


def filing_to_db_rows(f: Filing, entry: IndexEntry | None = None) -> dict:
    """Flatten a parsed Filing into rows for filings / reporting_owners / footnotes / transactions."""
    primary = f.owners[0]
    accepted = f.accepted_at
    filing_ts = accepted or (datetime.combine(entry.date_filed, dtime.min) if entry else None)
    late = any(is_late(t.transaction_date, accepted) for t in f.non_derivative)
    filing = {
        "accession_number": f.accession_number, "filing_date": filing_ts, "accepted_at": accepted,
        "issuer_name": f.issuer_name, "issuer_ticker": f.issuer_ticker, "issuer_cik": f.issuer_cik,
        "insider_name": primary.name, "insider_title": _primary_title(primary),
        "is_officer": primary.is_officer, "is_director": primary.is_director, "is_ten_pct": primary.is_ten_pct,
        "is_10b5_1": f.is_10b5_1, "form_type": f.form_type,
        "owner_ciks": "|".join(sorted({o.cik for o in f.owners})),
        "price_in_footnote": f.price_in_footnote, "is_late": late,
        "date_of_original_submission": f.date_of_original_submission,
    }
    owners = [{"accession_number": f.accession_number, "owner_cik": o.cik, "owner_name": o.name,
               "is_director": o.is_director, "is_officer": o.is_officer, "is_ten_pct": o.is_ten_pct,
               "is_other": o.is_other, "officer_title": o.officer_title} for o in f.owners]
    footnotes = [{"accession_number": f.accession_number, "footnote_id": k, "text": v} for k, v in f.footnotes.items()]
    txns = []
    for t in f.non_derivative:
        usable = t.shares is not None and t.price_per_share is not None and t.shares > 0 and t.price_per_share > 0
        txns.append({
            "accession_number": f.accession_number, "row_idx": t.row_idx, "transaction_date": t.transaction_date,
            "security_title": t.security_title, "code": t.code, "acquired_disposed": t.acquired_disposed,
            "shares": t.shares, "price_per_share": t.price_per_share,
            "dollar_value": round(t.shares * t.price_per_share, 2) if usable else None,
            "ownership_nature": t.direct_indirect, "post_transaction_shares": t.post_transaction_shares,
            "price_footnote_ids": ",".join(t.price_footnote_ids) or None,
        })
    return {"filing": filing, "owners": owners, "footnotes": footnotes, "txns": txns}


# ── amendments ──────────────────────────────────────────────────────────────
def mark_superseded(db) -> int:
    """Recompute `transactions.superseded` from scratch (deterministic, idempotent). Returns rows marked.

    Amendment heuristic: group transactions by (issuer CIK, owner-CIK set, transaction date, code). When a
    4/A exists in a group, every version accepted EARLIER than the latest 4/A in that group (the original
    and any older 4/A) is superseded as a whole; the latest 4/A's rows, and any plain Form 4 accepted after
    it, stay active. Matching is deliberately NOT on shares/price, because amendments usually correct those.
    Known limits: a 4/A that restates only some rows of a group retires the original's other rows of that
    group too; filings without owner CIKs (pre-EDGAR-ingest legacy rows) are never superseded. Version order
    is (acceptance time, accession number).
    """
    with db.transaction():
        _mark_superseded(db)
    return db.query("SELECT COUNT(*) FROM transactions WHERE superseded")[0][0]


def _mark_superseded(db) -> None:
    db.execute("UPDATE transactions SET superseded = FALSE WHERE superseded IS NULL OR superseded")
    db.execute("""
        UPDATE transactions SET superseded = TRUE WHERE id IN (
            WITH tx AS (
                SELECT t.id, t.accession_number, f.form_type, f.issuer_cik, f.owner_ciks, t.transaction_date, t.code,
                       COALESCE(f.accepted_at, f.filing_date) AS ts
                FROM transactions t JOIN filings f ON f.accession_number = t.accession_number
                WHERE f.owner_ciks IS NOT NULL AND f.issuer_cik IS NOT NULL
            ),
            latest_amend AS (
                SELECT issuer_cik, owner_ciks, transaction_date, code, ts, accession_number FROM (
                    SELECT *, row_number() OVER (PARTITION BY issuer_cik, owner_ciks, transaction_date, code
                                                 ORDER BY ts DESC, accession_number DESC) AS rn
                    FROM tx WHERE form_type = '4/A') WHERE rn = 1
            )
            SELECT tx.id FROM tx JOIN latest_amend a
              ON tx.issuer_cik = a.issuer_cik AND tx.owner_ciks = a.owner_ciks
             AND tx.transaction_date = a.transaction_date AND tx.code = a.code
            WHERE tx.ts < a.ts OR (tx.ts = a.ts AND tx.accession_number < a.accession_number)
        )""")


# ── ingest ──────────────────────────────────────────────────────────────────
def _business_days(start: date, end: date) -> list[date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _existing(db, accessions: list[str]) -> set[str]:
    found: set[str] = set()
    for i in range(0, len(accessions), 1000):
        found |= db.existing_accessions(accessions[i:i + 1000])
    return found


def ingest(source, db, settings=default_settings, *, today: date | None = None, since: date | None = None,
           max_filings: int | None = None, batch_size: int = 200,
           clock: Callable[[], float] = time.monotonic) -> dict:
    """Stage 1: pull new Form 4 / 4/A filings from EDGAR and store them. See the module docstring.

    `since` overrides the watermark start (backfill; the watermark never moves backwards). The filing cap is
    `max_filings`, else `edgar_first_run_max_filings` when there is no watermark yet, else
    `edgar_max_filings_per_run` (0 = unlimited). `edgar_max_runtime_minutes` is a soft time budget.
    Returns counts plus the accessions / expected row count the gate reconciles.
    """
    with timed("ingest") as span:
        today = today or datetime.now(_EASTERN).date()
        watermark = db.get_watermark()
        if since is not None:
            start = since
        elif watermark is None:
            start = today - timedelta(days=settings.edgar_initial_lookback_days)
        else:
            start = watermark + timedelta(days=1)
        if max_filings is not None:
            cap = max_filings
        elif watermark is None and since is None:
            cap = settings.edgar_first_run_max_filings
        else:
            cap = settings.edgar_max_filings_per_run
        deadline = clock() + settings.edgar_max_runtime_minutes * 60 if settings.edgar_max_runtime_minutes else None

        res = {"filings_fetched": 0, "filings_ingested": 0, "transactions_found": 0, "duplicates_skipped": 0,
               "parse_errors": 0, "fetch_errors": 0, "index_errors": 0, "days_complete": 0,
               "stopped_early": None, "start_date": start.isoformat(), "accessions": [], "expected_txns": 0}
        batch: list[dict] = []

        def flush() -> None:
            if not batch:
                return
            try:
                with db.transaction():
                    db.insert_filings([b["filing"] for b in batch])
                    db.insert_reporting_owners([o for b in batch for o in b["owners"]])
                    db.insert_footnotes([n for b in batch for n in b["footnotes"]])
                    db.insert_transactions([t for b in batch for t in b["txns"]])
            except Exception as e:  # noqa: BLE001
                raise DBWriteError(f"Database insert failed: {e}") from e
            res["filings_ingested"] += len(batch)
            res["accessions"] += [b["filing"]["accession_number"] for b in batch]
            res["expected_txns"] += sum(len(b["txns"]) for b in batch)
            res["transactions_found"] = res["expected_txns"]
            batch.clear()

        contiguous, consecutive_fail = True, 0
        try:
            for day in _business_days(start, today):
                if res["stopped_early"]:
                    break
                try:
                    entries = source.entries(day)
                except (EdgarBlockedError, EdgarConfigError):
                    raise
                except EdgarError as e:
                    logger.warning("daily index %s failed: %s", day, e)
                    res["index_errors"] += 1
                    contiguous = False
                    continue
                res["filings_fetched"] += len(entries)
                have = _existing(db, [e.accession for e in entries])
                res["duplicates_skipped"] += len(have)
                day_ok = True
                # An empty index for yesterday/today is "not published yet", not a holiday: never advance past it.
                if not entries and day >= today - timedelta(days=1):
                    day_ok = False
                for entry in (e for e in entries if e.accession not in have):
                    if cap and res["filings_ingested"] + len(batch) >= cap:
                        res["stopped_early"], day_ok = "filing cap reached", False
                        break
                    if deadline is not None and clock() > deadline:
                        res["stopped_early"], day_ok = "time budget reached", False
                        break
                    try:
                        filing = source.load(entry)
                    except (Form4ParseError, FilingResolutionError, ValueError) as e:
                        res["parse_errors"] += 1  # permanent: skip, do not block the watermark forever
                        logger.warning("skipping %s: %s", entry.accession, e)
                        continue
                    except EdgarBlockedError:
                        raise
                    except EdgarConfigError:
                        raise
                    except EdgarError as e:
                        res["fetch_errors"] += 1
                        day_ok = False
                        consecutive_fail += 1
                        logger.warning("fetch %s failed: %s", entry.accession, e)
                        if consecutive_fail >= MAX_CONSECUTIVE_TRANSIENT:
                            raise IngestAborted(f"{consecutive_fail} consecutive fetch failures; last: {e}") from e
                        continue
                    consecutive_fail = 0
                    batch.append(filing_to_db_rows(filing, entry))
                    if len(batch) >= batch_size:
                        flush()
                        logger.info("ingested %d filings (day %s)", res["filings_ingested"], day)
                flush()
                if day_ok and contiguous and day < today:
                    db.set_watermark(day)
                    res["days_complete"] += 1
                elif not day_ok:
                    contiguous = False
        finally:
            try:
                flush()
            except DBWriteError:
                logger.exception("final flush failed")
            # derived tables always reflect what is stored, even after an early stop
            try:
                mark_superseded(db)
                rebuild_insider_edges(db)
            except Exception:  # noqa: BLE001
                logger.exception("derived-table rebuild failed")
        wm = db.get_watermark()
        res["watermark"] = wm.isoformat() if wm else None
        if res["stopped_early"]:
            logger.warning("ingest stopped early: %s; the next run resumes from the watermark", res["stopped_early"])
        span.set(filings_ingested=res["filings_ingested"], filings_duplicates_skipped=res["duplicates_skipped"])
        return res
