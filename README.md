# SEC Insider Trading Intelligence

Pulls SEC Form 4 insider-trading filings straight from EDGAR, ranks recent open-market insider buying and selling with
a deterministic, auditable method, blocks publication when integrity checks fail, and lets you question the data
through a tool-calling chat agent whose citations and numbers are checked against the database.

Every headline number below is produced by a script in this repo and saved in [`reports/`](reports/) together with the
git commit and dataset hashes that produced it. Runs on free-tier LLMs only.

> Informational and educational. **Not investment advice.** It does not detect or allege illegal insider trading.

## Results

| What was measured | Result | Report |
|---|---|---|
| **Form 4 parser vs SEC's official dataset, 2025 Q3** (1,000 random filings, 1,711 transaction rows) | **100%** row precision / recall, 100% of filings fully correct | [`reports/parse_accuracy/2025q3`](reports/parse_accuracy/2025q3/latest.md) |
| **Same, held-out 2025 Q2** (1,000 filings, 1,599 rows; scorer frozen after Q3) | **100%** | [`reports/parse_accuracy/2025q2`](reports/parse_accuracy/2025q2/latest.md) |
| Parser vs hand-checked test fixtures | 100% field-level exact match | [`reports/parse_accuracy_fixtures`](reports/parse_accuracy_fixtures/latest.md) |
| **Event study** (pre-registered): do insider buys predict abnormal returns? 2016 Q2 – 2026 Q1 | *pending — see below* | `reports/event_study` |
| Test suite | 438 tests, offline (network blocked in tests) | `make test` |

**How to read the parse-accuracy result.** The ground truth is the SEC's own
[Insider Transactions Data Sets](https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets).
They are derived from the same XML filings, so 100% means the parser reads the XML exactly as the SEC does; it is a
test of parser fidelity, not of whether the filings themselves are true. Getting there needed four documented scorer
fixes, all found on Q3 and then frozen before scoring Q2: the SEC dataset rounds prices and share counts to 2
decimals (rounding-only differences are counted separately: 123 prices and 13 share counts in Q3), uses placeholder
tickers like `NONE`/`N/A`/`-`, and quotes some security titles. One real parser bug was found and fixed (`NA` and
`-` tickers).

<!-- EVENT_STUDY_RESULTS -->
**Event study.** The design was registered before any real data was run
([`docs/event_study_prereg.md`](docs/event_study_prereg.md), commit `864a0a2`): three primary hypotheses tested at 5%
with Holm correction — H1 cluster buys, H2 CEO/CFO buys, H3 discretionary vs 10b5-1-planned sells — using a market
model, CAR over [0, +20] trading days, BMP and Welch tests, bootstrap CIs and calendar-time Fama-French alphas. The
runner refuses any other sample range. Results will be reported here whatever they show, including null results.

## How it works

```
SEC EDGAR (daily index + Form 4 XML)
   │  polite client: declared User-Agent, ≤8 req/s, retries, write-once raw cache
   ▼
Form 4 parser (stdlib XML → pydantic) ──► DuckDB raw tables (filings, owners, transactions, footnotes)
   ▼
Ranking (pandas, deterministic, no LLM) ──► Governance gate (5 independent checks)
                                              ├─ pass → publish ranking + insider graph + snapshot
                                              └─ fail → quarantine, nothing published
   ▼
Streamlit app (read-only DB)  ──►  chat agent ──► 8 typed SQL tools ──► grounding check on the answer
```

| Part | What it does |
|---|---|
| **Ingestion** (`edgar/`, `pipeline/ingest.py`) | Discovers Form 4s from EDGAR's daily index, fetches one full-submission `.txt` per filing, resumes from a watermark. Amendments (4/A) mark the filings they replace as superseded. No paid data API. |
| **Ranking** (`pipeline/rank.py`) | Net dollar flow per company from open-market buys (`P`) and sells (`S`) over a 14-day window, with secondary signals: number of distinct insiders (clusters), officer/director role, trade size vs. holdings, share under pre-scheduled 10b5-1 plans. Removes joint-filer double counting. Output has a SHA-256 checksum. |
| **Governance gate** (`pipeline/gate.py`) | Ingest reconciliation, notional reconciliation via a second independent SQL path, per-ticker bounds, ranking arithmetic and order, checksum. Any failure quarantines the run. |
| **Insider graph** (`store/graph.py`) | Who-sits-with-whom edges built deterministically from SEC identifiers (CIKs). No LLM, no entity guessing. |
| **Chat agent** (`chat/`) | One LLM agent with a tool loop (≤4 rounds) over 8 typed, parameterized, read-only SQL tools: `top_trades`, `filings_for`, `insider_history`, `cluster_events`, `insider_network`, `co_insiders`, `sentiment`, `data_status`. Falls back to a no-tools mode if tool calling fails. |
| **Grounding** (`chat/grounding.py`) | Every citation tag must match a source returned by this turn's tools; every number in the answer must appear in the tool outputs (within rounding); fact-stating answers over empty results are flagged. Known limits are documented in the module. |
| **LLMs** (`llm/`) | Groq `openai/gpt-oss-120b` primary, OpenRouter `:free` models as fallback. Paid models are rejected at startup. Every call is logged to an `llm_calls` table (tokens, latency, provider). |
| **Evaluation** (`eval/`, `market/`) | Shared harness that stamps every report with git SHA, dirty flag, dataset hashes and seed; SEC dataset loader; parse-accuracy scorer; event-study engine with a Yahoo price cache and Ken French factors. |

Design decisions are written up as ADRs in [`docs/adr/`](docs/adr/), for example why there is only one agent, why
EDGAR instead of a paid API, and why the earlier LightRAG knowledge graph was removed.

## Run it

```bash
conda activate ai                      # Python 3.11
pip install -e ".[dev]"
cp .env.example .env                   # set SEC_USER_AGENT ("Name contact@example.com") and LLM keys
make check-keys                        # one tiny live call per service
make app                               # Streamlit: Chat · Dashboard · Pipeline · Health
```

* **Demo mode** uses a separate database of fictional companies, so the app works without any keys
  (`python -m insider_intel seed-demo`).
* **Live data:** `python -m insider_intel run` ingests recent filings, ranks, gates and publishes to `data/live.duckdb`.
  A GitHub Actions workflow runs it on the 1st and 15th of each month; a freshness check alerts if data goes stale.
* **Reproduce the evaluations:**

```bash
python -m insider_intel.eval parse-accuracy --year 2025 --quarter 3 --yes   # downloads the SEC quarter zip + 1,000 filings
python -m insider_intel.eval parse-accuracy-offline                         # fixtures only, no network
python -m insider_intel.eval event-study --yes                              # 40 quarters + daily prices; long
make test                                                                    # 438 tests, no network
make lint                                                                    # ruff
```

## Keys and cost

| Service | Used for | Cost |
|---|---|---|
| SEC EDGAR | filings, official datasets | free; SEC requires a User-Agent with contact info |
| Groq | primary LLM | free tier (~1,000 requests/day) |
| OpenRouter | fallback LLMs, `:free` models only | free |
| Yahoo chart endpoint, Ken French library | event-study prices and factors | free; Yahoo's endpoint is unofficial |

## Limitations

* Form 4s are due within two business days of a trade, so the data is recent, not real-time.
* The live pipeline is tested end to end offline; its scheduled runs depend on the `SEC_USER_AGENT` repository secret.
* The chat agent has a 17-question seed golden set and unit tests for every tool and grounding rule, but no full
  accuracy benchmark yet.
* Event-study prices come from Yahoo's unofficial endpoint; delisted tickers often have no data and are dropped
  (every drop reason is counted in the report), which can bias toward surviving companies.

## Layout

```
app/streamlit_app.py       Streamlit UI
src/insider_intel/
  edgar/                   EDGAR client, daily index, Form 4 parser, quality helpers
  pipeline/                ingest, rank, gate, run
  store/                   DuckDB access, SQL migrations 001–004, insider graph, snapshots
  chat/                    agent, tools, grounding, degraded mode, service
  llm/                     provider chain, tool calling, telemetry
  eval/  market/           evaluation harness and runners, prices, event study
docs/                      architecture, ADRs, runbook, event-study pre-registration
datasets/                  golden sets and expected fixture outputs
reports/                   generated evaluation reports (committed)
tests/                     pytest suite (offline)
```
