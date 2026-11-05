-- Schema v2. All timestamps are UTC (naive TIMESTAMP).

CREATE SEQUENCE IF NOT EXISTS txn_seq;
CREATE SEQUENCE IF NOT EXISTS top_trades_seq;
CREATE SEQUENCE IF NOT EXISTS query_log_seq;

-- One row per Form 4 filing (open-market P/S activity only is ingested)
CREATE TABLE IF NOT EXISTS filings (
    accession_number TEXT PRIMARY KEY,
    filing_date      TIMESTAMP,
    issuer_name      TEXT,
    issuer_ticker    TEXT,
    issuer_cik       TEXT,
    insider_name     TEXT,
    insider_title    TEXT,
    is_officer       BOOLEAN DEFAULT FALSE,
    is_director      BOOLEAN DEFAULT FALSE,
    is_ten_pct       BOOLEAN DEFAULT FALSE,
    is_10b5_1        BOOLEAN DEFAULT FALSE,   -- footnotes mention a Rule 10b5-1 trading plan
    form_type        TEXT DEFAULT '4',
    ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Non-derivative open-market transactions inside a filing
CREATE TABLE IF NOT EXISTS transactions (
    id                      INTEGER PRIMARY KEY DEFAULT nextval('txn_seq'),
    accession_number        TEXT NOT NULL,
    row_idx                 INTEGER NOT NULL,
    transaction_date        DATE,
    security_title          TEXT,
    code                    TEXT,             -- 'P' open-market purchase, 'S' open-market sale
    acquired_disposed       TEXT,             -- 'A' / 'D'
    shares                  DOUBLE,
    price_per_share         DOUBLE,
    dollar_value            DOUBLE,           -- shares x price (computed at parse time)
    ownership_nature        TEXT,             -- 'D' direct / 'I' indirect
    post_transaction_shares DOUBLE,
    UNIQUE (accession_number, row_idx)
);

-- Output of the ranking stage (written only after the governance gate passes)
CREATE TABLE IF NOT EXISTS top_trades (
    id                  INTEGER PRIMARY KEY DEFAULT nextval('top_trades_seq'),
    run_id              TEXT,
    run_date            DATE,
    rank                INTEGER,
    issuer_ticker       TEXT,
    issuer_name         TEXT,
    insider_name        TEXT,
    insider_title       TEXT,
    net_dollar_value    DOUBLE,
    buy_value           DOUBLE,
    sell_value          DOUBLE,
    transaction_count   INTEGER,
    insider_count       INTEGER,
    pct_holdings        DOUBLE,               -- largest trade as a share of the insider's position
    planned_pct         DOUBLE,               -- share of gross $ reported under a 10b5-1 plan
    dominant_direction  TEXT,                 -- 'BUY' | 'SELL'
    signal_tags         TEXT,                 -- comma list: CLUSTER_BUY, OFFICER, LARGE_STAKE, PLANNED ...
    validation_checksum TEXT
);

CREATE TABLE IF NOT EXISTS tweets (
    tweet_id        TEXT PRIMARY KEY,
    ticker          TEXT,
    text            TEXT,
    author_username TEXT,
    author_followers INTEGER,
    created_at      TIMESTAMP,
    retweet_count   INTEGER,
    like_count      INTEGER,
    reply_count     INTEGER,
    ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tweet_sentiments (
    tweet_id         TEXT PRIMARY KEY,
    ticker           TEXT,
    sentiment_label  TEXT,                    -- bullish | bearish | neutral
    confidence_score DOUBLE,
    model            TEXT,
    reviewed         BOOLEAN DEFAULT FALSE,   -- re-classified by the learning loop
    classified_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Per ticker per *tweet* day
CREATE TABLE IF NOT EXISTS sentiment_summary (
    ticker          TEXT,
    summary_date    DATE,
    total_tweets    INTEGER,
    bullish_count   INTEGER,
    bearish_count   INTEGER,
    neutral_count   INTEGER,
    avg_confidence  DOUBLE,
    sentiment_index DOUBLE,                   -- (bullish - bearish) / total, in [-1, 1]
    PRIMARY KEY (ticker, summary_date)
);

-- Scrape cache + Apify spend tracking
CREATE TABLE IF NOT EXISTS scrape_log (
    ticker     TEXT,
    scraped_at TIMESTAMP,
    tweets     INTEGER,
    est_cost_usd DOUBLE
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            TEXT PRIMARY KEY,
    started_at        TIMESTAMP,
    completed_at      TIMESTAMP,
    status            TEXT,                   -- running | success | partial | quarantined | failed
    trigger           TEXT,                   -- manual | auto
    stages            JSON,                   -- per-stage status/detail
    filings_ingested  INTEGER DEFAULT 0,
    trades_ranked     INTEGER DEFAULT 0,
    tweets_scraped    INTEGER DEFAULT 0,
    tweets_classified INTEGER DEFAULT 0,
    documents_indexed INTEGER DEFAULT 0,
    error_message     TEXT
);

-- Every chat turn: used by the learning loop and the Health tab
CREATE TABLE IF NOT EXISTS query_log (
    id          INTEGER PRIMARY KEY DEFAULT nextval('query_log_seq'),
    ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    question    TEXT,
    intent      TEXT,
    confidence  TEXT,
    latency_ms  INTEGER,
    model       TEXT,
    used_lightrag BOOLEAN,
    rows_used   INTEGER,
    feedback    INTEGER                       -- +1 / -1 / NULL
);

-- Few-shot examples distilled by the learning loop (only labels two models agreed on)
CREATE TABLE IF NOT EXISTS learned_examples (
    tweet_id   TEXT PRIMARY KEY,
    text       TEXT,
    label      TEXT,
    confidence DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
