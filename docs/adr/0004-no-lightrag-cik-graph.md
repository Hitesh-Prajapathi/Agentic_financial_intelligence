# ADR-0004: LightRAG/GraphRAG out; deterministic CIK graph instead

- Status: Accepted
- Date: 2026-11-14

## Context
v2 indexed five text blocks (themselves generated from SQL rows) into LightRAG. Inspection on 13 Nov 2026
(plan section 14) found:
- v2 live graph: 39 entities / 37 relations; most-connected node was the report template (`Insider Trading Report`,
  degree 15); about 10 of 39 entities came from spam tweets pasted into the documents.
- v1 graph: 78 entities / 169 relations; numbers stored as entities and 7+ variants of one concept.
- No entity resolution (`Dutch Bros Inc.` and `BROS` as separate nodes); stale documents never removed.
- No insider-to-company link that SQL did not already have; a "+2 more" insiders cluster lost the other insiders.
- Cost on free tiers: roughly one long LLM call per ~1.2k-token chunk, so a 10-K would take on the order of tens of
  minutes and tens of daily requests (estimate, not measured).

## Decision
Remove LightRAG (and fastembed) from the product path. Build the insider network deterministically from Form 4 CIKs:
nodes are owner CIK and issuer CIK; an `insider_edges` row per (owner, issuer) holds role flags, first/last filing and
P/S totals. Query it with SQL and recursive CTEs; expose chat tools `insider_network(owner)` and
`co_insiders(issuer, window)`. The table and tools are *(in progress)*. LightRAG may return only as an optional,
subset-sized arm of a 10-K retrieval evaluation.

## Consequences
- Zero LLM calls for graph building; CIKs solve name matching; no extra event-loop thread.
- No graph database or networkx in the runtime.
- We do not get free-text relation extraction (e.g. owner linked to a trust via footnote); that is an optional narrow
  extraction step, only if validated against hand labels.
- Revisit if the product adds unstructured narrative corpora (10-K, 8-K) where broad summary questions matter and the
  measured retrieval ablation favours a graph arm.
