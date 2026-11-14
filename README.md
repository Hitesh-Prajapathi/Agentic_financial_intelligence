# SEC Insider Trading Intelligence

Finds the most significant **insider trades** in the last 14 days of SEC Form 4 filings (data is refreshed about twice a month) and lets you question the
results through a **grounded chat assistant** that cites its sources.
Runs entirely on **free-tier** LLMs.

```
SEC Form 4 ──► rank (deterministic) ──► GOVERNANCE GATE ──► publish
                                              │ fail → quarantine (nothing published)
Chat: question ──► SQL facts ──► streamed, cited answer
```

## What it does

| Stage | What happens |
|---|---|
| **SEC ingestion** | Pulls open-market buys/sells (Form 4 codes **P** / **S**) from SEC EDGAR (no API key; set `SEC_USER_AGENT`). Incremental via a watermark; all transaction codes are stored, ranking uses P/S only. |
| **Ranking** | Net dollar flow per ticker, with secondary signals: number of insiders (**cluster** buying/selling), officer/director role, trade size vs. the insider's position, and share of the trade under a pre-scheduled **10b5-1 plan**. Joint-filer double reports are removed. No LLM involved. |
| **Governance gate** | Five independent integrity checks (ingest reconciliation, notional reconciliation via a separate SQL path, per-ticker bounds, ranking arithmetic/order, checksum). Any failure → run is **quarantined**: the ranking is never published. |
| **Chat** | Answers only from retrieved evidence, cites `[RANK …]`/`[SEC Form 4 …]`/`[SENT …]` tags that are verified against the evidence, shows a confidence badge, streams tokens, accepts 👍/👎. Chart requests bypass the LLM and render interactive Plotly charts. |

## Run it

```bash
conda activate ai                      # Python 3.11
pip install -e ".[dev]"                # or: pip install -r requirements.txt
cp .env.example .env                   # then fill in SEC_USER_AGENT and your LLM keys
make check-keys                        # one tiny live call per service
make app                               # streamlit run app/streamlit_app.py
```

* **Refresh data** (sidebar) runs the pipeline in the background; you can keep chatting. The scheduled GitHub Actions ingest runs about twice a month (1st and 15th); between runs the data is a snapshot. With `APP_READ_ONLY=true` the app opens the database read-only and hides the button.
* **Demo mode** switches to a separate database of *fictional* companies, so you can try everything without API keys. Your live data is never touched.
* CLI: `python -m insider_intel run | check-keys | seed-demo` · tests: `make test`

## Keys & cost (all free tiers)

| Service | Used for | Notes |
|---|---|---|
| SEC EDGAR | Form 4 data | no key; requires `SEC_USER_AGENT` (name + contact email) |
| Groq | primary LLM (`gpt-oss-120b`, ~1 s) | ~1000 requests/day free |
| OpenRouter | fallback LLMs, **`:free` models only** | ~50 free requests/day; paid models are rejected at startup unless `ALLOW_PAID_MODELS=true` |

## Known limitations

* Form 4s can be filed up to **two business days** after the trade, so this is "recent", not "real-time".
* Informational only; **not investment advice**.

## Layout

```
app/streamlit_app.py    Streamlit UI (Chat · Dashboard · Pipeline · Health)
src/insider_intel/
  pipeline/             ingest (EDGAR), rank, gate (+ quarantine), run (orchestration)
  chat/                 retrieval, agent (answer + citation check), service (intent routing), metrics
  llm/                  provider fallback chain + prompts      sentiment/  pure classifier
  store/                thread-safe DuckDB + numbered migrations       ui/charts.py  Plotly charts
  config.py  obs.py  system.py  health.py  demo.py  cli.py
docs/architecture.md    current pipeline description         tests/
```
