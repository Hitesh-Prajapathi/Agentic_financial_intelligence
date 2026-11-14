# Form 4 fixtures

**None of these files is a byte-identical SEC original.** Live EDGAR returned HTTP 403 to the generic
research User-Agent when the fixtures were built, so no network collection was possible. Two kinds exist:

## RECONSTRUCTED (8 files: real filings, rebuilt XML)
Real accession numbers and real values, rebuilt into the EDGAR `ownershipDocument` layout from the
sec-api.io JSON in `tests/fixtures/sec_form4_sample.json` (which mirrors the XML element names). A `.meta.json`
beside each holds the acceptance time (sec-api `filedAt`, converted to naive UTC). Differences from the real
XML: no addresses/signature block fidelity, element order/whitespace normalised, `xml_name` unknown.

| accession | issuer | covers |
|---|---|---|
| 0001193125-26-449032 | PB | 6 rows, direct + indirect (profit-sharing plan, trust), holdings rows, 10b5-1 checkbox set |
| 0001193125-26-448980 | BCBP | shares footnote vs post-transaction footnote |
| 0001193125-26-448969 | BCBP | simple director purchase |
| 0001193125-26-448845 | MMYT | same-day M+S x2, weighted-average price footnotes, derivative table |
| 0001570585-26-000156 | LBTY | indirect via trust, price footnote, remarks |
| 0001058725-26-000011 | LBTY | weighted-average purchase |
| 0002093121-26-000009 | BULL | 10b5-1 checkbox + footnote, weighted-average price footnote |
| 0000080424-26-000271 | PG | same-day M+S, 10-digit-padded short CIK |

## SYNTHETIC (16 files: `0000000000-26-0000NN`)
Hand-built from the documented structure; fictional company ("Acme Widgets Inc"), people, CIKs (9xxxxxx) and
accessions. Each carries a `SYNTHETIC` XML comment.

01 simple purchase - 02 joint filing (fund + GP) - 03 4/A with dateOfOriginalSubmission - 04 derivative-only
(schema X0508) - 05 empty ticker - 06 ticker " none " - 07 gifts (G), zero vs missing price - 08 tax
withholding (F) - 09 price only in footnote (weighted avg) - 10 legacy 10b5-1 footnote (no checkbox) -
11 negated 10b5-1 footnote - 12 checkbox 0 - 13 checkbox 1 - 14 isOther, "true"/"false" booleans, comma numbers,
holdings row, missing optional nodes, equity swap, lower-case code - 15 same-day M+S+S - 16 indirect via trust.

Caveat: assertions over synthetic/reconstructed files prove the parser agrees with our reading of the
structure, not with SEC's actual bytes. Replace with real XML (see below); the golden table in
`test_edgar_form4.py` is keyed by accession, so real files drop in for the 8 reconstructed ones unchanged
(synthetic ones should be kept alongside).

## Fetching real fixtures
```
export SEC_USER_AGENT='your-app-name you@example.com'
PYTHONPATH=src python -m insider_intel.edgar.fetch_fixtures --out tests/edgar/fixtures \
    1068851:0001193125-26-449032 1495153:0001193125-26-448845   # cik:accession
```
The client cache is write-once, so delete a reconstructed `<accession>.xml`/`.meta.json` pair first to replace it.

## tests/edgar/samples
`form.20260310.idx`, `index.json`, `*-index-headers.html` are SYNTHETIC samples of the documented EDGAR
daily-index / folder-listing / header formats (not captured from live EDGAR).
