# ADR-0006: Learning loop replaced by offline evals

- Status: Accepted
- Date: 2026-11-14

## Context
v2 had a "learning loop": when two models agreed on a sentiment label, the label was treated as confirmed and fed back
as few-shot examples; contested labels were rewritten to neutral. Agreement between models is not correctness, and
feeding self-confirmed labels back as examples can amplify a bias both models share. Nothing measured whether
the loop improved anything.

## Decision
Remove the loop, the `learned_examples` table and few-shot injection. Quality is established offline against frozen,
human-labelled or SQL-verified sets, run as `python -m insider_intel eval <name>`, writing reports stamped with git
SHA, dataset version, model ids and prompt hashes under `reports/`. Prompts and thresholds are tuned on dev splits
only; test splits are scored once per reported version. User thumbs up/down and latency stats are kept as plain logs.

## Consequences
- Claims in the README can link to a report and a reproduce command.
- No online self-improvement; improvements are deliberate, reviewed commits.
- Labelling costs human time.
- Revisit if there is a source of real ground truth at scale (e.g. verified user feedback) that justifies a
  human-in-the-loop update process, evaluated before adoption.
