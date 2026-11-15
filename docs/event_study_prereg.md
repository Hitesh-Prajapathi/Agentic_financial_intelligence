# Event-study pre-registration

> **Status: REGISTERED on 2026-11-14**, before any real-data run. Reviewed by the author and cross-checked against
> the runner code (parameters, decision rules, Holm families, sample range). Nothing here has been run on real
> data yet. Do not change it after the commit below; any deviation goes into section 10 as a dated amendment.

**Pre-registration commit: 864a0a23b7ad7857c323180521d11eecd8a35527**

Code: `src/insider_intel/market/prices.py`, `src/insider_intel/market/event_study.py`, plus the historical
runner `src/insider_intel/eval/event_study_run.py` (`python -m insider_intel.eval event-study`; it refuses to run
the confirmatory study until the commit line above holds a real hash, and records the hash, this file's sha256 and
the per-quarter dataset hashes in the report). Validated only on synthetic data: injected CAR recovery, null
calibration, calendar-time alpha recovery, an offline end-to-end dry run.

**Historical vs live timing (read this first).** The historical study uses the SEC Insider Transactions Data Sets,
which contain `FILING_DATE` but NO EDGAR acceptance time. The information date is therefore `FILING_DATE`, and
day 0 is the first trading session STRICTLY AFTER the filing date (each filing is stamped 23:00 UTC on its
filing date, i.e. after the 16:00 ET close). This is up to one session more conservative than the live
pipeline's acceptance-time rule (section 3): the reaction on the filing day itself is excluded. Same-day filings
of one issuer collapse into one candidate event.

## 1. Question

Do SEC Form 4 open-market insider transactions carry information about subsequent
abnormal stock returns, and which of the candidate rankings (`abs_net`, `pct_mcap`, `cluster`)
orders events best?

## 2. Hypotheses

**Primary (3). Each tested at alpha = 0.05 with Holm correction across these three only.**

* **H1.** Cluster buys (>= 2 distinct insiders buying in the same issuer within 30 calendar days)
  have positive mean market-model CAR over [0, +20] trading days (two-sided test, directional claim: > 0).
  Primary statistic: BMP t. The cross-sectional t and bootstrap CI must agree in sign.
* **H2.** CEO/CFO open-market buys have positive mean CAR [0, +20] (BMP t).
* **H3.** Discretionary sells (code S, not under a 10b5-1 plan) have a *different* (hypothesised negative)
  mean CAR [0, +20] than planned (10b5-1) sells; tested as the difference in mean CAR,
  Welch t-test on per-event CARs.
  **Sample limitation:** in the historical datasets the 10b5-1 status is `SUBMISSION.AFF10B5ONE` (the Form 4
  checkbox). Where that column is absent or blank the status is *unknown* (never "not a plan") and those sells are
  excluded from both sell kinds, so H3's sample covers only the quarters in which the checkbox exists (the report
  prints sell rows with/without the flag per filing quarter) and is much smaller than H1/H2's. No footnote-text
  fallback is used in the historical study.

**Secondary (reported with Holm correction over the whole secondary family, plus unadjusted values)**

* S1. Single-insider buys: CAR [0, +5], [0, +60].
* S2. Cluster buys and officer buys: CAR [0, +5], [0, +60].
* S3. Large-stake buys (filing value >= $100k): CAR [0, +20] (event kind `large_buy`).
* S4. Planned (10b5-1) sells alone; discretionary sells alone: CAR [0, +20].
* S5. Calendar-time FF3 alpha (horizon 60) for each event kind.
* S6. Ranking comparison: top-minus-bottom decile spread in CAR [0, +20] for `abs_net`, `pct_mcap`,
  `cluster` scores (Welch t, Spearman rho). **Status: partial.** Only `abs_net` (filing value) on `single_buy`
  events is run; `pct_mcap` is deferred until shares-outstanding data exist (plan B2) and the `cluster`-score
  ranking is not run in the first version. Until then the default ranking is not changed by this study. The ranking with the larger spread, provided it is
  significant at Holm-adjusted 5% against zero, becomes the default in B2. If none is, the default
  stays `abs_net` and this is reported as inconclusive.
* S7. Diagnostic: pre-event drift CAR [-20, -1] (a significant pre-drift means trades follow price moves, i.e. signal is contaminated by momentum/reversal).
* S8. Robustness: market-adjusted AR (r - r_SPY) instead of market model.

Deferred (listed here so nothing is dropped silently): exercise-and-sell events (needs derivative-table
parsing); the market-cap-floor secondary run (see section 4); the `pct_mcap` and `cluster` parts of S6.

## 3. Event definitions

Events come from the parser output (code P / S only; derivative-table rows excluded). Form 4/A amendments are
excluded from the historical build (the original filing already carries the trade; the count is reported).
Time stamp is the **information date**, never the transaction date: for the live pipeline the EDGAR acceptance time
(`accepted_at`, UTC); for the historical study, which has no acceptance time, `FILING_DATE` stamped after the close
(see the box at the top), so day 0 is the first session strictly after the filing date.

| Kind | Definition | Parameters (fixed) |
|---|---|---|
| `single_buy` | any code-P filing | dedup 30 calendar days per issuer |
| `officer_buy` | code P where `officer_title` matches `CEO / CFO / Chief Executive / Chief Financial / Principal Executive / Principal Financial` | dedup 30 days |
| `cluster_buy` | >= k = 2 distinct owners with P in one issuer within N = 30 calendar days; stamped at the acceptance of the filing completing the k-th owner | dedup 30 days |
| `large_buy` | code-P filing with shares x price >= $100,000 | dedup 30 days |
| `discretionary_sell` | code S with `is_10b5_1` false | dedup 30 days |
| `planned_sell` | code S with `is_10b5_1` true | dedup 30 days |

Rows with unknown 10b5-1 status are excluded from both sell kinds (count reported; historically, unknown means
`AFF10B5ONE` absent or blank).
Sensitivity (secondary, reported only): k = 3, N = 60, dedup 60 days.
Dedup is non-chained: after a kept event, no new event for that issuer within the window.

**Event day (day 0)** = first trading session that opens after the information is public (live definition;
historical study: always the next session after `FILING_DATE`, as stated above):
accepted before 09:30 ET on a trading day -> that day; 09:30-16:00 ET -> next trading day;
at/after 16:00 ET or on a non-trading day -> next trading day. Conversion UTC -> US/Eastern is DST-aware.
Day-0 return is close(t-1) -> close(t). The day-of-filing return for intraday filings is therefore
excluded (conservative). Early-close days are not modelled.

## 4. Sample

* SEC Insider Transactions Data Sets (official quarterly bulk files), **2016Q2 through 2026Q1: 40 quarters (10 years)**.
  2026Q1 is the newest dataset SEC had published at registration (2026Q2 and 2026Q3 returned HTTP 404 on 2026-11-14).
  The runner enforces this range for the confirmatory run (`REGISTERED_RANGE` in `eval/event_study_run.py`) and
  refuses any other; the per-quarter file hashes are written into the report.
* Issuer CIK -> ticker: SEC `company_tickers.json` (current tickers), keeping the filed ticker when it is among the
  CIK's current tickers, else the first current ticker, else the filed ticker; unmapped/fallback counts are reported.
  Delisted issuers are largely absent from that file (survivorship, see section 9).
* Prices: daily adjusted closes from a free source (Yahoo chart endpoint; Stooq currently blocks scripted access), market proxy SPY, factors from Ken French daily FF3.
* **Filters**: reported price per share >= $1.00 (primary), reported without a market-cap floor.
  The secondary run with a market-cap floor of $300M is **deferred until shares-outstanding data exist (plan B2)**:
  there is currently no shares-outstanding data, so it cannot be run and is not run. It will be added with a dated
  amendment, not silently.
* Events dropped for: no event day (too recent), no price data, short estimation window, missing/delisted prices in the window. Counts for every reason are reported (`coverage`).
* Events with the same ticker are not independent; dedup limits overlap, and the calendar-time test is the overlap-robust check.

## 5. Model and windows

* Market model: OLS of stock daily return on SPY return over trading days [-250, -30] relative to day 0; at least 120 valid observations else the event is dropped (`short_estimation_window`).
* AR_t = r_t - (alpha + beta r_m,t). Robustness: AR_t = r_t - r_m,t.
* CAR windows: [0, +5], [0, +20], [0, +60]; diagnostic [-20, -1]. Primary window for H1-H3: [0, +20].
* Returns from `adj_close` (total-return basis with Yahoo; split-adjusted only with Stooq).

## 6. Tests

1. Cross-sectional t-test on CARs.
2. BMP standardized cross-sectional test (Boehmer, Musumeci, Poulsen 1991) with the forecast-error variance correction; **primary** for H1/H2 because it is robust to event-induced variance.
3. Bootstrap 95% CI of mean CAR (2,000 resamples, seed 0).
4. Calendar-time portfolio: equal weight of stocks within [+1, +60] of an event, excess return regressed on Mkt-RF, SMB, HML with Newey-West HAC SEs; at least 5 stocks per day. Alpha reported daily and x252.
5. Holm correction: across the 3 primary hypotheses (family 1); across all secondary tests (family 2).

## 7. What counts as support

* H1/H2: mean CAR[0,20] > 0, BMP p < 0.05 after Holm (family 1), the bootstrap CI excludes 0 and the market-adjusted variant has the same sign. Economic size is reported next to significance.
* H3: Welch p < 0.05 after Holm (family 1) with discretionary CAR < planned CAR.
* A result significant only in the cross-sectional t but not BMP, or only before Holm, is reported as "not supported".
* Calendar-time alpha is the check that clustering of events in time does not drive the result; a primary result where the calendar-time alpha has the opposite sign or t < 1 is described as fragile.
* Secondary results are exploratory regardless of p-values.

## 8. Reporting null results

Every pre-registered hypothesis is reported in the same table with the same format whether positive, null or negative
(N, mean CAR, median, share positive, t, BMP t, bootstrap CI, Holm-adjusted p). The report states the registered commit hash, the
git SHA and data versions of the run, and the number of tests run. A null result is published as a null result.
No hypothesis, window, filter or parameter is added or dropped after seeing results; any deviation goes into a dated "Amendments" section.

## 9. Known biases and limitations

* **Survivorship**: free price data often lacks delisted, acquired or renamed tickers. Missing names are dropped (`no_price_data`), and the direction is typically upward bias in CARs after buys (failed companies vanish), so positive buy results are weaker evidence than negative ones. Price coverage (tickers requested vs found) is reported. A sensitivity treating delisted-mid-window as 0 further return is not done in the primary analysis.
* **Ticker changes / reuse**: the filing ticker may belong to a different company in the price source; changes inside the sample are not mapped (known to lose events or mis-assign).
* **Delistings**: windows truncated by delisting are set to missing, not -100%.
* **Adjustments**: Yahoo `adj_close` is dividend-adjusted; Stooq (if usable) is not. The Yahoo endpoint is unofficial and can change.
* Prices of illiquid small caps: bid-ask bounce and stale prices bias CARs; the $1 and market-cap filters are partial mitigations.
* Overlapping windows and cross-sectional dependence of events (dedup + calendar-time check only).
* Early-close days and filing-time rounding are ignored. Live: EDGAR acceptance time is the public time.
  Historical: only `FILING_DATE` is available, so the first session's reaction to pre-close filings is excluded
  (conservative; a delayed-reaction story would look stronger than an intraday one).
* Historical `AFF10B5ONE` coverage limits H3 (section 2); sells with unknown plan status are excluded, not assumed discretionary.
* A price pass in which any ticker request failed is aborted rather than reported, so provider outages cannot
  masquerade as delistings.
* Abnormal returns are **not tradeable alpha**: transaction costs, liquidity and capacity are not modelled.
* This study makes **no claim of detecting illegal trading**; Form 4 trades are legally reported transactions.
* Multiple event kinds and windows inflate false positives; Holm addresses only what is listed here.

## 10. Amendments

None.
