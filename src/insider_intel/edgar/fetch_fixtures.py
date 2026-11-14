"""(Re)fetch real Form 4 fixtures from EDGAR by accession list.

    export SEC_USER_AGENT='your-app-name you@example.com'
    python -m insider_intel.edgar.fetch_fixtures --out tests/edgar/fixtures \
        1068851:0001193125-26-449032 1495153:0001193125-26-448845
    python -m insider_intel.edgar.fetch_fixtures --file accessions.txt   # one "cik:accession" per line

Each argument is ``cik:accession`` where cik is any CIK on the filing (issuer or a reporting owner).
Files land as ``<out>/<accession>.xml`` plus ``<accession>.meta.json`` (acceptance time). Existing
files are never overwritten (the cache is immutable). Importing this module does no network I/O.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .client import EdgarClient, EdgarError
from .index import fetch_filing


def parse_pair(s: str) -> tuple[str, str]:
    cik, sep, acc = s.strip().partition(":")
    if not sep or not cik.isdigit():
        raise ValueError(f"expected 'cik:accession', got {s!r}")
    return cik, acc


def fetch_fixtures(pairs: list[tuple[str, str]], out_dir: Path, client: EdgarClient) -> list[str]:
    """Fetch each (cik, accession) into out_dir via the client's cache; returns accessions fetched OK."""
    client.cache_dir = Path(out_dir)
    ok = []
    for cik, acc in pairs:
        try:
            doc = fetch_filing(client, cik, acc)
        except EdgarError as exc:
            print(f"FAILED {acc}: {exc}", file=sys.stderr)
            continue
        print(f"ok {acc} ({doc.xml_name}, accepted_at={doc.accepted_at})")
        ok.append(acc)
    return ok


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pairs", nargs="*", help="cik:accession")
    ap.add_argument("--file", help="text file with one cik:accession per line")
    ap.add_argument("--out", default="tests/edgar/fixtures")
    args = ap.parse_args(argv)

    raw = list(args.pairs)
    if args.file:
        raw += [ln for ln in Path(args.file).read_text().splitlines() if ln.strip() and not ln.startswith("#")]
    if not raw:
        ap.error("no accessions given")
    pairs = [parse_pair(s) for s in raw]
    if len(pairs) > 150:
        ap.error("refusing to fetch more than 150 documents in one run")

    with EdgarClient(cache_dir=args.out) as client:  # raises EdgarConfigError without SEC_USER_AGENT
        ok = fetch_fixtures(pairs, Path(args.out), client)
    return 0 if len(ok) == len(pairs) else 1


if __name__ == "__main__":
    from ..eval.common import export_sec_user_agent_from_settings

    export_sec_user_agent_from_settings()
    raise SystemExit(main())
