"""Evaluation CLI:  python -m insider_intel.eval <command> [flags]

parse-accuracy          E1 live: sample an official SEC quarter, fetch+parse the filings, score (needs SEC_USER_AGENT)
parse-accuracy-offline  E1 CI regression check on the committed EDGAR fixtures (no network)
event-study             E4 historical event study (needs a pre-registration commit hash, or --dry-run)
"""

from __future__ import annotations

import argparse
import sys

from .common import MissingUserAgent

DATASETS = "datasets/parse_accuracy"


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="python -m insider_intel.eval", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="command", required=True)

    p = sub.add_parser("parse-accuracy", help="E1 against an official SEC quarter")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--quarter", type=int, required=True, choices=[1, 2, 3, 4])
    p.add_argument("--sample", type=int, default=1000)
    p.add_argument("--hard", type=int, default=100)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--cache-dir", default="data/raw/edgar")
    p.add_argument(
        "--max-fetch", type=int, default=None, help="cap on uncached filings fetched (seeded random subsample)"
    )
    p.add_argument("--data-dir", default="data/sec_datasets")
    p.add_argument("--reports-root", default="reports")
    p.add_argument("--yes", action="store_true", help="proceed when the estimate exceeds 200 requests")

    o = sub.add_parser("parse-accuracy-offline", help="fixture regression check (CI)")
    o.add_argument("--fixtures-dir", default="tests/edgar/fixtures")
    o.add_argument("--expected", default=f"{DATASETS}/fixture_expected.csv")
    o.add_argument("--baseline", default=f"{DATASETS}/fixture_baseline.json")
    o.add_argument("--reports-root", default="reports")

    e = sub.add_parser("event-study", help="E4 historical event study")
    # The confirmatory run always uses the pre-registered range (docs/event_study_prereg.md section 4:
    # 2016Q2..2026Q1) and refuses a different one; these flags only shape dry runs / samples.
    e.add_argument("--from-year", type=int, default=2016)
    e.add_argument("--from-quarter", type=int, default=1, choices=(1, 2, 3, 4))
    e.add_argument("--to-year", type=int, default=2025)
    e.add_argument("--to-quarter", type=int, default=4, choices=(1, 2, 3, 4))
    e.add_argument("--kinds", default="cluster_buy,single_buy,officer_buy,large_buy,discretionary_sell,planned_sell")
    e.add_argument("--prices", default="yahoo", help="yahoo | csvdir:<path>")
    e.add_argument(
        "--max-tickers", type=int, default=None, help="truncate to the N most frequent tickers (non-confirmatory)"
    )
    e.add_argument(
        "--dry-run",
        action="store_true",
        help="last 2 quarters, few tickers, reports/event_study_dryrun/, no prereg check",
    )
    e.add_argument("--n-boot", type=int, default=2000)
    e.add_argument("--yes", action="store_true")
    e.add_argument("--data-dir", default="data/sec_datasets")
    e.add_argument("--rows-cache", default="data/cache/event_rows")
    e.add_argument("--tickers-cache", default="data/cache/company_tickers.json")
    e.add_argument("--price-cache", default="data/prices")
    e.add_argument("--reports-root", default="reports")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "parse-accuracy":
            from .parse_accuracy_run import run_live

            return run_live(args)
        if args.command == "parse-accuracy-offline":
            from .parse_accuracy_run import run_offline

            return run_offline(args)
        from .event_study_run import run_study

        return run_study(args)
    except MissingUserAgent as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    from .common import export_sec_user_agent_from_settings

    export_sec_user_agent_from_settings()
    raise SystemExit(main())
