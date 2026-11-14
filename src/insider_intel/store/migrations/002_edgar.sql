-- B1/B4: EDGAR-native ingestion. Additive only: existing v2 columns/tables are never dropped or renamed.

ALTER TABLE filings ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMP;              -- EDGAR acceptance time (naive UTC); filing_date mirrors it
ALTER TABLE filings ADD COLUMN IF NOT EXISTS owner_ciks TEXT;                    -- '|'-joined sorted CIKs of ALL reporting owners
ALTER TABLE filings ADD COLUMN IF NOT EXISTS price_in_footnote BOOLEAN DEFAULT FALSE;
ALTER TABLE filings ADD COLUMN IF NOT EXISTS is_late BOOLEAN DEFAULT FALSE;      -- accepted > 2 business days after a transaction
ALTER TABLE filings ADD COLUMN IF NOT EXISTS date_of_original_submission DATE;   -- 4/A only

-- All transaction codes are stored now (P, S, A, M, F, G, ...); ranking filters to P/S itself.
-- Direct/indirect ownership lives in the existing `ownership_nature` column.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS price_footnote_ids TEXT;       -- comma list of footnote ids attached to the price
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS superseded BOOLEAN DEFAULT FALSE;  -- replaced by a later 4/A (see pipeline.ingest.mark_superseded)

-- Every reporting owner of a filing (joint filers), keyed by CIK.
CREATE TABLE IF NOT EXISTS reporting_owners (
    accession_number TEXT NOT NULL,
    owner_cik        TEXT NOT NULL,
    owner_name       TEXT,
    is_director      BOOLEAN DEFAULT FALSE,
    is_officer       BOOLEAN DEFAULT FALSE,
    is_ten_pct       BOOLEAN DEFAULT FALSE,
    is_other         BOOLEAN DEFAULT FALSE,
    officer_title    TEXT,
    PRIMARY KEY (accession_number, owner_cik)
);

CREATE TABLE IF NOT EXISTS footnotes (
    accession_number TEXT NOT NULL,
    footnote_id      TEXT NOT NULL,
    text             TEXT,
    PRIMARY KEY (accession_number, footnote_id)
);

-- Last fully ingested EDGAR index date per source (days <= this are complete).
CREATE TABLE IF NOT EXISTS ingest_watermark (
    source          TEXT PRIMARY KEY,
    last_index_date DATE,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
