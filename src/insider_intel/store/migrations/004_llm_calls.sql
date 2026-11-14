-- P3: LLM call telemetry + richer chat query log. est_cost_usd is a NOTIONAL list-price estimate
-- (free-tier models are what is actually used); see config.llm_list_prices.
CREATE SEQUENCE IF NOT EXISTS llm_calls_seq;

CREATE TABLE IF NOT EXISTS llm_calls (
    id                INTEGER PRIMARY KEY DEFAULT nextval('llm_calls_seq'),
    ts                TIMESTAMP,
    run_id            TEXT,
    session_id        TEXT,
    purpose           TEXT,
    provider          TEXT,
    model             TEXT,
    prompt_hash       TEXT,
    status            TEXT,
    latency_ms        INTEGER,
    prompt_tokens     INTEGER,
    completion_tokens INTEGER,
    n_tool_calls      INTEGER,
    est_cost_usd      DOUBLE
);

ALTER TABLE query_log ADD COLUMN IF NOT EXISTS mode TEXT;
ALTER TABLE query_log ADD COLUMN IF NOT EXISTS tool_calls INTEGER;
ALTER TABLE query_log ADD COLUMN IF NOT EXISTS unsupported_numbers INTEGER;
ALTER TABLE query_log ADD COLUMN IF NOT EXISTS prompt_hash TEXT;
