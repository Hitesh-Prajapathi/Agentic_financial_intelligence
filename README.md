# SEC Insider Trading Intelligence

Finds the most significant **insider trades** in the last 24 hours from SEC Form 4 filings, adds **social-media sentiment**
for those tickers, and lets you question the results through a **grounded chat assistant** that cites its sources.
Runs entirely on **free-tier** LLMs.

```
SEC Form 4 ──► rank (deterministic) ──► GOVERNANCE GATE ──► tweets (Apify) ──► sentiment (LLM) ──► LightRAG index ──► learning loop
                                              │ fail → quarantine (nothing published, no API spend)
Chat: question ──► SQL facts (+ optional LightRAG graph context) ──► streamed, cited answer
```

## What it does

| Stage | What happens |
|---|---|
| **SEC ingestion** | Pulls open-market buys/sells (Form 4 codes **P** / **S**) from sec-api.io, newest first, paginated, incremental. |
| **Ranking** | Net dollar flow per ticker, with secondary signals: number of insiders (**cluster** buying/selling), officer/director role, trade size vs. the insider's position, and share of the trade under a pre-scheduled **10b5-1 plan**. Joint-filer double reports are removed. No LLM involved. |
| **Governance gate** | Five independent integrity checks (ingest reconciliation, notional reconciliation via a separate SQL path, per-ticker bounds, ranking arithmetic/order, checksum). Any failure → run is **quarantined**: the ranking is never published and no money is spent on tweets/LLMs. |
| **Tweets** | Apify scraper for the ranked tickers; parallel, cached, budget-capped; retweets/spam/non-English dropped. |
| **Sentiment** | Free-model LLM chain classifies tweets (bullish/bearish/neutral); strict output validation; unlabelled stays unlabelled. |
| **LightRAG** (switchable) | Each run's trades + sentiment are indexed into a knowledge graph; chat adds graph context to the SQL facts. Toggle in the sidebar or `ENABLE_LIGHTRAG`. |
| **Learning loop** | Only after the gate passes: low-confidence labels get a second opinion from a different model (disagreement → neutral), agreed labels become few-shot examples, quality metrics are tracked (Health tab). |
| **Chat** | Answers only from retrieved evidence, cites `[RANK …]`/`[SEC Form 4 …]`/`[SENT …]` tags that are verified against the evidence, shows a confidence badge, streams tokens, accepts 👍/👎. Chart requests bypass the LLM and render interactive Plotly charts. |

## Run it

```bash
conda activate ai                      # Python 3.11
pip install -r requirements.txt
cp .env.example .env                   # then fill in your keys
python scripts/check_keys.py           # one tiny live call per service
streamlit run app.py
```

* **Refresh data** (sidebar) runs the pipeline in the background; you can keep chatting. Nothing runs on a schedule — it only runs when you click.
* **Demo mode** switches to a separate database of *fictional* companies, so you can try everything without API keys. Your live data is never touched.
* CLI: `python scripts/run_pipeline.py` · tests: `pip install -r requirements-dev.txt && pytest`

## Keys & cost (all free tiers)

| Service | Used for | Notes |
|---|---|---|
| sec-api.io | Form 4 data | free tier is enough |
| Groq | primary LLM (`gpt-oss-120b`, ~1 s) | ~1000 requests/day free |
| OpenRouter | fallback LLMs, **`:free` models only** | ~50 free requests/day; paid models are rejected at startup unless `ALLOW_PAID_MODELS=true` |
| Apify | tweets | pay-per-tweet ($0.0004). A budget cap (`APIFY_MONTHLY_BUDGET_USD`, default $2) is checked before every scrape. **Apify also bills runs that return nothing.** |

Embeddings are computed locally (fastembed, ~67 MB downloaded once to `data/models`) — no key, no rate limits.

## Known limitations

* Form 4s can be filed up to **two business days** after the trade, so this is "recent", not "real-time".
* Social sentiment is AI-labelled and noisy; small samples are shown with their size.
* The Apify free plan appears to cap results (10 per run) and the scraper can return empty placeholders when throttled;
  the pipeline then reports `partial` instead of failing. Set `APIFY_ACTOR_ID` to try another scraper (its output fields may need mapping in `tools/apify_tool.py`).
* Scraping X may conflict with X's terms of service — review before any non-personal use.
* Informational only; **not investment advice**.

## Layout

```
app.py                  Streamlit UI (Chat · Dashboard · Pipeline · Health)
core/                   system factory, live health checks
agents/                 sec_data, ranking, social_scraper, sentiment, indexing, retrieval, chat, visualization, supervisor
tools/                  sec_api, apify, llm (provider chain), lightrag (own event-loop thread), db (thread-safe DuckDB), charts
skills/                 integrity_checks (governance gate), learning_loop
telemetry/              OpenTelemetry (set OTEL_CONSOLE=true to print spans), quarantine
config/                 settings + prompts      db/schema.sql      scripts/      tests/
```
