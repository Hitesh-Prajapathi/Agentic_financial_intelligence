-- B6: deterministic insider network. One edge per (owner CIK, issuer CIK); derived, rebuilt after every ingest
-- by insider_intel.store.graph.rebuild_insider_edges (no LLM involved).
CREATE TABLE IF NOT EXISTS insider_edges (
    owner_cik         TEXT NOT NULL,
    owner_name        TEXT,
    issuer_cik        TEXT NOT NULL,
    issuer_ticker     TEXT,
    issuer_name       TEXT,
    is_director       BOOLEAN,
    is_officer        BOOLEAN,
    is_ten_pct        BOOLEAN,
    officer_title     TEXT,
    first_accepted_at TIMESTAMP,
    last_accepted_at  TIMESTAMP,
    n_filings         INTEGER,
    buy_value         DOUBLE,
    sell_value        DOUBLE,
    n_buys            INTEGER,
    n_sells           INTEGER,
    PRIMARY KEY (owner_cik, issuer_cik)
);
