CREATE SEQUENCE IF NOT EXISTS txn_seq;
CREATE SEQUENCE IF NOT EXISTS top_trades_seq;

-- Normalized SEC Form 4 filings
CREATE TABLE IF NOT EXISTS filings (
    accession_number TEXT PRIMARY KEY,
    filing_date TIMESTAMP,
    issuer_name TEXT,
    issuer_ticker TEXT,
    insider_name TEXT,
    insider_title TEXT,
    form_type TEXT DEFAULT '4',
    raw_json JSON,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Individual transactions within filings
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY DEFAULT nextval('txn_seq'),
    accession_number TEXT REFERENCES filings(accession_number),
    transaction_date DATE,
    security_title TEXT,
    transaction_type TEXT,        -- 'A' (acquisition) or 'D' (disposal)
    shares DECIMAL(18,4),
    price_per_share DECIMAL(18,4),
    dollar_value DECIMAL(18,2),   -- Computed: shares × price
    ownership_nature TEXT,         -- 'D' (direct) or 'I' (indirect)
    post_transaction_shares DECIMAL(18,4)
);

-- Daily top trades (output of Ranking Agent)
CREATE TABLE IF NOT EXISTS top_trades (
    id INTEGER PRIMARY KEY DEFAULT nextval('top_trades_seq'),
    run_date DATE,
    rank INTEGER,                  -- 1-5
    issuer_ticker TEXT,
    insider_name TEXT,
    net_dollar_value DECIMAL(18,2),
    transaction_count INTEGER,
    dominant_direction TEXT,       -- 'BUY' or 'SELL'
    validation_checksum TEXT       -- SVG integrity hash
);

-- Raw tweets
CREATE TABLE IF NOT EXISTS tweets (
    tweet_id TEXT PRIMARY KEY,
    ticker TEXT,
    text TEXT,
    author_username TEXT,
    created_at TIMESTAMP,
    retweet_count INTEGER,
    like_count INTEGER,
    reply_count INTEGER,
    raw_json JSON,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sentiment labels per tweet
CREATE TABLE IF NOT EXISTS tweet_sentiments (
    tweet_id TEXT PRIMARY KEY REFERENCES tweets(tweet_id),
    ticker TEXT,
    sentiment_label TEXT,         -- 'bullish', 'bearish', 'neutral'
    confidence_score DECIMAL(5,4),
    classified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Aggregated sentiment per ticker per day
CREATE TABLE IF NOT EXISTS sentiment_summary (
    ticker TEXT,
    summary_date DATE,
    total_tweets INTEGER,
    bullish_count INTEGER,
    bearish_count INTEGER,
    neutral_count INTEGER,
    avg_confidence DECIMAL(5,4),
    sentiment_index DECIMAL(5,4), -- (bullish - bearish) / total
    PRIMARY KEY (ticker, summary_date)
);

-- Pipeline run metadata (for telemetry)
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status TEXT,                   -- 'success', 'partial_failure', 'quarantined'
    filings_ingested INTEGER,
    trades_ranked INTEGER,
    tweets_scraped INTEGER,
    tweets_classified INTEGER,
    documents_indexed INTEGER,
    error_log JSON,
    error_message TEXT              -- populated on quarantine/failure
);
