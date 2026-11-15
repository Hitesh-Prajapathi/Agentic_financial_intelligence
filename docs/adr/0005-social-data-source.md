# ADR-0005: Social data source

- Status: Proposed (decision pending with the project owner)
- Date: 2026-11-14

## Context
The Apify/X scraper was removed: it returned no results, billed even when empty, and carried terms-of-service risk.
The sentiment classifier (`sentiment/classify.py`) is kept as pure code, but currently has no live source and has not
been evaluated against human labels. Demo data still fills the sentiment tables shown on the dashboard.

## Options
1. **Reddit API** (OAuth, free for non-commercial use per the plan; verify current terms). Plan recommendation, after a
   half-day access spike.
2. **StockTwits**: user-tagged Bullish/Bearish gives free weak labels, but access for new developers has reportedly
   been restricted; verify.
3. **No social data**: evaluate the classifier only on a public labelled dataset, or cut sentiment entirely.

## Decision
Pending. Until decided, no social source is wired into the pipeline and the sentiment UI shows demo data only.

## Consequences
- Whichever source is chosen, sentiment ships only after a labelled-set evaluation (macro-F1, kappa, baselines).
- Dropping social is a legitimate, documented scope decision, not a failure.
- Revisit trigger for the final decision: outcome of the access spike, API terms, or lack of time for labelling.
