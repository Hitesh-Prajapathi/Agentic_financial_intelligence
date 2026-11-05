# Skill: SEC Form 4 ingestion
sec-api.io Insider Trading API. Query: `documentType:"4"` AND `filedAt` in the last 24h AND transaction code P or S
(filtered server-side). Newest-first, 50 per page, stops early once a page is already in the database.
Rows without a usable price are skipped. Footnotes are scanned for Rule 10b5-1 plan language (negations ignored).
