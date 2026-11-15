# Report: parse_accuracy_fixtures

## Metadata

- **git_sha**: `36ca1d72f4274099c8cef510cfc86d66c4d6d08e`
- **git_dirty**: `False`
- **timestamp_utc**: `2026-11-14T17:35:41+00:00`
- **python**: `3.11.14`
- **dataset**: `tests/edgar/fixtures (reconstructed + synthetic)`
- **expected_csv_sha256**: `8855946c3689b00f597f65d257524cbb828829318224d25267433aec87673094`
- **regressions**: `[]`
- **observed**: `{"filing_all_correct_rate":1.0,"min_field_exact_match":1.0,"row_f1_strict":1.0}`

## Metrics

| metric | value |
|---|---|
| n_accessions | 15 |
| n_official_rows | 30 |
| n_parsed_rows | 30 |
| n_matched_rows | 30 |
| price_rounding_only | 0 |
| shares_rounding_only | 0 |
| parsed_accessions_not_in_official | 0 |
| row_precision | 1.0000 |
| row_recall | 1.0000 |
| row_f1 | 1.0000 |
| row_precision_strict | 1.0000 |
| row_recall_strict | 1.0000 |
| row_f1_strict | 1.0000 |
| filing_all_correct_rate | 1.0000 |
| n_mismatches | 0 |

## Summary

| metric | value |
|---|---|
| n_accessions | 15 |
| n_official_rows | 30 |
| n_parsed_rows | 30 |
| n_matched_rows | 30 |
| row_precision | 1.0000 |
| row_recall | 1.0000 |
| row_f1 | 1.0000 |
| row_precision_strict | 1.0000 |
| row_recall_strict | 1.0000 |
| row_f1_strict | 1.0000 |
| filing_all_correct_rate | 1.0000 |
| price_rounding_only | 0 |
| shares_rounding_only | 0 |

## Per-field exact match

| field | exact_match_rate |
|---|---|
| form_type | 1.0000 |
| issuer_cik | 1.0000 |
| issuer_ticker | 1.0000 |
| owner_ciks | 1.0000 |
| transaction_date | 1.0000 |
| code | 1.0000 |
| shares | 1.0000 |
| price_per_share | 1.0000 |
| acquired_disposed | 1.0000 |
| post_transaction_shares | 1.0000 |
| direct_indirect | 1.0000 |
| security_title | 1.0000 |

## Net open-market dollar flow error

| metric | value |
|---|---|
| n_tickers | 6 |
| mae_usd | 0.0000 |
| median_abs_pct_err | 0.0000 |
| mean_abs_pct_err | 0.0000 |
| share_tickers_exact | 1.0000 |
| mae_usd_cent_rounded | 155.2566 |
| share_tickers_exact_cent_rounded | 0.6667 |

## Amendment / joint-filer handling

| subset | n | filing_exact_rate | row_recall |
|---|---|---|---|
| 4/A | 1 | 1.0000 | 1.0000 |
| joint filers | 1 | 1.0000 | 1.0000 |

## Mismatch categories

_(empty)_

## Mismatches (first 500)

_(empty)_

## Notes

**Fixtures are reconstructed/synthetic** (see tests/edgar/fixtures/README.md): 8 reconstructed from sec-api.io JSON, the rest hand-built. This is a parser regression check against hand-transcribed expected rows, NOT the official-dataset accuracy (E1), which needs real XML and SEC_USER_AGENT.

Recorded baseline: `{"n_accessions": 15, "n_official_rows": 30, "row_f1_strict": 1.0, "filing_all_correct_rate": 1.0, "min_field_exact_match": 1.0}`. Status: OK
