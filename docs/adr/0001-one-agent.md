# ADR-0001: One agent, everything else functions

- Status: Accepted
- Date: 2026-11-14

## Context
v2 had a "supervisor" and many "agents" (SEC, ranking, sentiment, indexing, learning, visualization). Most were fixed
sequences of steps with a regex intent router, not components making decisions. Calling them agents added classes,
threads and indirection without adding capability.

## Decision
Use an agent only where a real decision exists: the chat assistant picks which query to run, with which arguments, for
an open-ended question. It is a tool-calling loop over typed, parameterized, read-only SQL tools (at most 4 calls per
turn, pydantic-validated arguments, tool errors returned to the model). Ingestion, ranking, gating and publishing are
fixed control flow and are plain functions (`ingest()`, `rank()`, `gate()`, `publish()`). Chart generation is a plain
function used by the dashboard.

The tool-calling chat agent is *(in progress)*; today's chat uses context stuffing with a citation check.

## Consequences
- Less code, no background-thread orchestration, deterministic stages that are easy to test.
- The numbers shown to users never pass through an LLM except as quoted tool output, which grounding checks verify.
- Models without tool support need a degraded fallback path (context stuffing, labelled as such).
- Trade-off: no free-form text-to-SQL, so some questions need a new tool before they can be answered.
- Revisit if a pipeline stage needs genuine runtime decisions (e.g. adaptive retries across sources) or if tool
  accuracy in the chat golden-set eval is poor enough to justify a different design.
