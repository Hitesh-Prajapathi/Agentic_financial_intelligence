# Architecture Decision Records

| ADR | Title | Status |
|---|---|---|
| [0001](0001-one-agent.md) | One agent, everything else functions | Accepted |
| [0002](0002-edgar-vs-sec-api.md) | EDGAR-native ingestion instead of sec-api.io | Accepted |
| [0003](0003-duckdb-parquet-snapshots.md) | DuckDB + Parquet snapshots instead of Postgres | Accepted |
| [0004](0004-no-lightrag-cik-graph.md) | LightRAG/GraphRAG out; deterministic CIK graph | Accepted |
| [0005](0005-social-data-source.md) | Social data source | Proposed |
| [0006](0006-learning-loop-to-evals.md) | Learning loop replaced by offline evals | Accepted |
| [0007](0007-github-actions-cron.md) | GitHub Actions cron instead of Prefect/APScheduler | Accepted |

Format: Context, Decision, Consequences (including what would make us revisit). New scope needs an ADR.
