# ADR-0002: EDGAR-native ingestion instead of sec-api.io

- Status: Accepted
- Date: 2026-11-14

## Context
v2 used the sec-api.io JSON API: needs a key, third-party schema, only P/S rows, a 24h window and a capped number of
filings. It cannot backfill history cheaply and it is not the primary source, which weakens any accuracy claim.

## Decision
Ingest Form 4 and 4/A XML directly from SEC EDGAR: daily form index for backfill and catch-up, current-filings feed for
same-day runs. A polite client sends a `SEC_USER_AGENT` from the environment (startup fails without it), limits itself
to at most 8 requests/s (SEC limit is 10), backs off on 429/503, and caches every raw XML by accession number. The
parser uses stdlib `xml.etree` into pydantic models, stores all transaction codes, all joint owners, footnotes and the
acceptance timestamp (`accepted_at`), and ingestion runs from a stored watermark with upsert by accession.

The `edgar/` module exists; wiring it into `pipeline/ingest.py` in place of sec-api.io is *(in progress)*.

## Consequences
- No API key, official source, history available for evaluation and event study.
- We own the parsing, so correctness must be measured: parse accuracy against the SEC Insider Transactions Data Sets.
- Operational duty: respect SEC rate limits and the declared User-Agent policy.
- Revisit if SEC changes access policy or the XML schema in a way that makes parsing costly to maintain.
