# ADR-0003: DuckDB + Parquet snapshots instead of Postgres

- Status: Accepted
- Date: 2026-11-14

## Context
The data is small (thousands of filings per month), written by one batch job and read by a dashboard and chat. A hosted
database would add cost, credentials and an always-on service for a portfolio project on free tiers.

## Decision
Use an embedded DuckDB file as the store. The scheduled job restores the previous DB snapshot, ingests, ranks, gates,
and publishes the DB plus derived tables as Parquet to a GitHub Release asset. The hosted app downloads the latest
snapshot and opens it read-only. Schema changes go through numbered SQL migrations with a `schema_version` table
(planned, B4).

## Consequences
- Zero infrastructure and cost; snapshots are versioned, downloadable and reproducible.
- A single writer: concurrent writers are not supported, so the cron uses a non-overlapping concurrency group.
- Data freshness is bounded by snapshot cadence, not real time.
- Release assets have size limits and the history accumulates; old releases may need pruning.
- Revisit if there are multiple writers, per-user data, sub-hour freshness needs, or DB size approaches release limits.
