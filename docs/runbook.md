# Runbook

Commands marked *(pending)* are assumed by the workflows but not yet implemented.

## Run locally

```bash
conda activate ai
pip install -e ".[dev,eval]"
make test          # offline tests
make lint          # ruff check + format check
make app           # Streamlit UI
make run           # one live pipeline run (python -m insider_intel run)
make seed-demo     # fictional demo database
make check-keys    # one tiny live call per external service
```

The pipeline never runs on a schedule locally; it runs only when you trigger it.

## Environment variables

| Variable | Needed for | Notes |
|---|---|---|
| `SEC_USER_AGENT` | EDGAR ingestion (in progress) | Identifies you to the SEC, format `your-app-name your-email@example.com`. Never commit it. Startup fails without it. |
| `GROQ_API_KEY` | Chat (primary LLM) | Not needed for ingestion, CI or the dashboard. |
| `OPENROUTER_API_KEY` | Chat fallback | Only `:free` models are allowed. |
| `SEC_API_KEY` | Legacy sec-api.io path | Goes away when EDGAR ingestion replaces it. |
| `LIVE_DB_PATH` | Database location | Default `./data/live.duckdb`. |

Copy `.env.example` to `.env` for local use. In GitHub, add `SEC_USER_AGENT` under Settings, Secrets and variables, Actions.

## Scheduled runs, snapshots, alerts

- `.github/workflows/ingest.yml`: twice a month (1st and 15th, 23:30 UTC) and a manual "Run workflow" button. It restores `live.duckdb` from the
  latest Release, runs `python -m insider_intel run`, exports Parquet with `publish-snapshot` *(pending)*, and creates a
  Release tagged `data-YYYYMMDD-HHMM` (about 24 a year). Each run catches up everything since the watermark, about two
  weeks of filings, so it can take an hour or more. Runs never overlap.
- On failure it opens or comments on an issue labelled `pipeline-failure`.
- `.github/workflows/freshness.yml`: on the 3rd and 17th, two days after each ingest. Opens or updates an issue labelled
  `data-stale` if the newest filing is more than 5 business days old (i.e. a scheduled run was missed or failed) (weekday count, no holiday calendar) or the last run is `failed` or `quarantined`.
- `.github/workflows/ci.yml`: lint, format, tests, `pip-audit` on PRs and pushes. No keys needed.
- GitHub disables scheduled workflows after 60 days of repo inactivity. If a `data-stale` issue appears and nothing
  failed, check the Actions tab and re-enable the workflow.

## Rerun a failed day

1. Open the issue and the linked Actions run to see the error.
2. Fix the cause (or wait out an SEC outage), then Actions, Ingest, "Run workflow".
3. Ingestion resumes from the stored watermark *(in progress)*, so a missed day leaves no gap. Locally: `make run`.
4. Close the issue once the run succeeds.

## Backfill

Backfill from the EDGAR daily index for a date range using the EDGAR module *(in progress; a CLI flag such as
`--since YYYY-MM-DD` is pending)*. Raw XML is cached under `data/raw/edgar/`, so re-parsing does not refetch. Keep to
the SEC rate limit (the client enforces at most 8 requests/s).

## Reading quarantine reasons

A run that fails the governance gate has `status = 'quarantined'` and nothing is published. The reason is in
`pipeline_runs.error_message`; per-check detail is in `pipeline_runs.stages` (JSON, `gate` entry).

```sql
select run_id, started_at, status, error_message from pipeline_runs order by started_at desc limit 5;
select stages->'gate' from pipeline_runs where status = 'quarantined' order by started_at desc limit 1;
```

Checks: ingest reconciliation (expected vs stored transactions), notional reconciliation (second SQL path), per-ticker
bounds, ranking arithmetic and order, checksum. A reconciliation failure usually points at a parser or ingestion bug;
a bounds failure at a suspicious source value. Investigate before rerunning, since a rerun on the same data will fail
the same way.
