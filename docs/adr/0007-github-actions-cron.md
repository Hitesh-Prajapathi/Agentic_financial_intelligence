# ADR-0007: GitHub Actions cron instead of Prefect/APScheduler

- Status: Accepted
- Date: 2026-11-14

## Context
The pipeline is a batch job that catches up from a stored watermark. Twice-monthly freshness is enough for this
project (insider-flow signals are evaluated over 5-60 trading days, and Form 4s already lag trades by up to 2 days). Running a scheduler on a personal machine is unreliable and conflicts
with the rule that nothing is scheduled locally. Prefect or an in-process APScheduler needs a long-running host.

## Decision
Schedule `ingest.yml` on GitHub Actions: a twice-monthly cron (1st and 15th, 23:30 UTC) plus a manual
`workflow_dispatch`. It restores the previous snapshot from the latest Release, runs the pipeline, publishes a new
Release, and on failure opens or updates a `pipeline-failure` issue. A concurrency group prevents overlap.
`freshness.yml` independently checks data age and the last run status and opens a `data-stale` issue on breach.
`ci.yml` runs lint, tests (offline, no keys) and `pip-audit` on every PR.

## Consequences
- Free, no host to maintain, logs and manual reruns in the Actions UI.
- Cron timing is best-effort (runs can be delayed minutes to hours).
- GitHub disables scheduled workflows after 60 days of repository inactivity; the freshness check (itself scheduled)
  may also stop, so the manual dispatch button and periodic commits remain the backstop.
- Release-asset snapshots grow over time and need pruning.
- Revisit if runs need to be more frequent than a few per day, need long runtimes or retries across steps, or if the
  data grows beyond the Actions runner limits.
