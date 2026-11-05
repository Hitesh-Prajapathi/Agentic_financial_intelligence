"""Stage 1: pull new Form 4 filings from sec-api.io and store them."""
import logging

from telemetry.otel_setup import tracer
from tools.sec_api_tool import ParsingError, UpstreamAPIError  # noqa: F401  (re-exported for callers)

logger = logging.getLogger(__name__)


class DBWriteError(Exception):
    pass


class SecDataAgent:
    def __init__(self, sec_tool, db_tool, settings):
        self.sec_tool, self.db, self.settings = sec_tool, db_tool, settings

    def run(self) -> dict:
        with tracer.start_as_current_span("sec_data_agent.run") as span:
            def page_is_known(rows):  # stop paging once a whole page is already in the DB (caught up)
                accs = [r.get("accessionNo") for r in rows if r.get("accessionNo")]
                return bool(accs) and len(self.db.existing_accessions(accs)) == len(accs)

            raw = self.sec_tool.fetch_recent_filings(page_is_known=page_is_known)
            seen, unique = set(), []
            for r in raw:  # pagination can repeat a filing if new ones arrive mid-fetch
                acc = r.get("accessionNo")
                if acc and acc not in seen:
                    seen.add(acc)
                    unique.append(r)

            existing = self.db.existing_accessions([r["accessionNo"] for r in unique])
            filings, txns, parse_errors = [], [], 0
            for r in unique:
                if r["accessionNo"] in existing:
                    continue
                try:
                    f, t = self.sec_tool.parse_filing(r)
                except ParsingError as e:
                    parse_errors += 1
                    logger.warning("%s", e)
                    continue
                filings.append(f)
                txns.extend(t)

            try:
                with self.db.transaction():
                    self.db.insert_filings(filings)
                    self.db.insert_transactions(txns)
            except Exception as e:  # noqa: BLE001
                raise DBWriteError(f"Database insert failed: {e}") from e

            result = {
                "filings_fetched": len(unique),
                "filings_ingested": len(filings),
                "transactions_found": len(txns),
                "duplicates_skipped": len(unique) - len(filings) - parse_errors,
                "parse_errors": parse_errors,
                "accessions": [f["accession_number"] for f in filings],
                "expected_txns": len(txns),
            }
            span.set_attribute("filings.ingested", result["filings_ingested"])
            span.set_attribute("filings.duplicates_skipped", result["duplicates_skipped"])
            return result
