# Report: parse_accuracy/2025q3

## Metadata

- **git_sha**: `36ca1d72f4274099c8cef510cfc86d66c4d6d08e`
- **git_dirty**: `False`
- **timestamp_utc**: `2026-11-14T17:35:35+00:00`
- **python**: `3.11.14`
- **dataset**: `sec_insider_transactions_2025q3`
- **dataset_sha256**: `9c57f272b7230500771f661bc1fe181527ed6b76a6f45d23a910c7fc7e9fcfa4`
- **seed**: `0`
- **sample_n**: `1000`
- **hard_n**: `100`
- **n_sampled**: `1000`
- **n_scored**: `1000`
- **n_fetch_failed**: `0`
- **n_skipped_max_fetch**: `0`
- **n_parser_errors**: `0`
- **http_requests**: `0`
- **official_rows_rejected_at_load**: `{"DERIV_HOLDING":0,"DERIV_TRANS":0,"FOOTNOTES":0,"NONDERIV_HOLDING":0,"NONDERIV_TRANS":0,"OWNER_SIGNATURE":0,"REPORTINGOWNER":0,"SUBMISSION":0}`
- **cache_dir**: `data/raw/edgar`

## Metrics

| metric | value |
|---|---|
| n_accessions | 1000 |
| n_official_rows | 1711 |
| n_parsed_rows | 1711 |
| n_matched_rows | 1711 |
| price_rounding_only | 123 |
| shares_rounding_only | 13 |
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
| n_accessions | 1000 |
| n_official_rows | 1711 |
| n_parsed_rows | 1711 |
| n_matched_rows | 1711 |
| row_precision | 1.0000 |
| row_recall | 1.0000 |
| row_f1 | 1.0000 |
| row_precision_strict | 1.0000 |
| row_recall_strict | 1.0000 |
| row_f1_strict | 1.0000 |
| filing_all_correct_rate | 1.0000 |
| price_rounding_only | 123 |
| shares_rounding_only | 13 |

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
| n_tickers | 358 |
| mae_usd | 32.5023 |
| median_abs_pct_err | 0.0000 |
| mean_abs_pct_err | 0.0322 |
| share_tickers_exact | 0.5726 |
| mae_usd_cent_rounded | 0.3351 |
| share_tickers_exact_cent_rounded | 0.9832 |

## Amendment / joint-filer handling

| subset | n | filing_exact_rate | row_recall |
|---|---|---|---|
| 4/A | 9 | 1.0000 | 1.0000 |
| joint filers | 15 | 1.0000 | 1.0000 |
| hard-case sample | 100 | 1.0000 | 1.0000 |
| random sample | 900 | 1.0000 | 1.0000 |

## Mismatch categories

_(empty)_

## Mismatches (first 500)

_(empty)_

## Notes

Official datasets derive from the same XML, so this measures parser fidelity, not filer truthfulness.

Sampled 1000 accessions; scored 1000. Excluded from scoring (NOT parser errors): 0 fetch failures, 0 not fetched because of --max-fetch.

Parser exceptions counted against the parser: 0.
