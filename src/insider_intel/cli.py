"""Command line:  python -m insider_intel <command>

run [--since YYYY-MM-DD] [--max-filings N]   ingest from EDGAR, rank, gate, publish (exit 0 on success/partial)
migrate                                      apply pending schema migrations to the live database
publish-snapshot --out DIR                   export derived tables to Parquet + manifest.json
freshness [--max-business-days N]            exit non-zero if the data is stale or the last run failed
check-keys                                   one tiny live call per external service
seed-demo                                    (re)build the fictional demo database
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

OK_STATUSES = ("success", "partial")


def _run(args) -> int:
    from insider_intel.config import settings
    from insider_intel.system import build_system

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not settings.has_sec_user_agent:
        print(
            "error: SEC_USER_AGENT is not set. SEC requires a descriptive User-Agent with contact info, e.g.\n"
            "  export SEC_USER_AGENT='your-app-name your-email@example.com'",
            file=sys.stderr,
        )
        return 2
    system = build_system("live")
    kwargs = {}
    if args.since:
        kwargs["since"] = args.since
    if args.max_filings is not None:
        kwargs["max_filings"] = args.max_filings
    system.pipeline.ingest_kwargs = kwargs
    try:
        out = system.pipeline.run(trigger="manual")
    finally:
        system.sec.close()
        try:
            system.db.checkpoint()  # fold the WAL into live.duckdb: the file itself is what gets uploaded
        except Exception:  # noqa: BLE001
            logging.getLogger(__name__).exception("checkpoint failed")
    print(f"\nRun {out['run_id']} → {out['status']}")
    for name, st in system.pipeline.progress.snapshot()["stages"].items():
        print(
            f"  {name:<10} {st['status']:<8} {st['seconds'] and round(st['seconds'], 1)}s  {st['detail'] if st['status'] not in ('ok',) else ''}"
        )
    if out.get("error"):
        print("error:", out["error"])
    system.db.close()
    return 0 if out["status"] in OK_STATUSES else 1


def _migrate(_args) -> int:
    from insider_intel.config import settings
    from insider_intel.store.db import migrate_file

    applied = migrate_file(settings.live_db_path)
    print(f"applied migrations: {applied}" if applied else "already up to date")
    return 0


def _publish_snapshot(args) -> int:
    from insider_intel.config import settings
    from insider_intel.store.snapshot import publish_snapshot

    manifest = publish_snapshot(args.db or settings.live_db_path, args.out)
    print(f"wrote {len(manifest['tables'])} tables to {args.out} (schema v{manifest['schema_version']})")
    for t, n in manifest["tables"].items():
        print(f"  {t:<18} {n:>9,} rows")
    return 0


def _freshness(args) -> int:
    from insider_intel.config import settings
    from insider_intel.store.snapshot import check_freshness

    problems = check_freshness(args.db or settings.live_db_path, args.max_business_days)
    if problems:
        print("STALE: " + "; ".join(problems))
        return 1
    print("OK")
    return 0


def _check_keys(_args) -> int:
    from insider_intel.health import check_all
    from insider_intel.system import build_system

    bad = 0
    for r in check_all(build_system("live")):
        print(f"{'✅' if r['ok'] else '❌'} {r['service']:<18} {r['detail']}")
        bad += not r["ok"]
    return 1 if bad else 0


def _seed_demo(_args) -> int:
    from insider_intel.demo import seed_demo

    print(seed_demo())
    return 0


def _date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {s!r}") from e


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="insider_intel", description="SEC Form 4 insider-trade intelligence")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("run", help="run the ingestion pipeline once against the live database")
    p.add_argument(
        "--since", type=_date, help="backfill: start the EDGAR index scan at this date instead of the watermark"
    )
    p.add_argument("--max-filings", type=int, help="cap new filings fetched this run (0 = unlimited)")
    p.set_defaults(func=_run)
    sub.add_parser("migrate", help="apply pending schema migrations to the live database").set_defaults(func=_migrate)
    p = sub.add_parser("publish-snapshot", help="export derived tables to Parquet + manifest.json")
    p.add_argument("--out", required=True, help="output directory")
    p.add_argument("--db", help="database file (default: LIVE_DB_PATH)")
    p.set_defaults(func=_publish_snapshot)
    p = sub.add_parser("freshness", help="fail if the data is stale or the last run failed")
    p.add_argument("--max-business-days", type=int, default=5)
    p.add_argument("--db", help="database file (default: LIVE_DB_PATH)")
    p.set_defaults(func=_freshness)
    sub.add_parser("check-keys", help="one tiny live call per external service").set_defaults(func=_check_keys)
    sub.add_parser("seed-demo", help="(re)build the fictional demo database").set_defaults(func=_seed_demo)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
