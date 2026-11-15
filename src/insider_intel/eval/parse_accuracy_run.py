"""CLI runners for E1: live parse accuracy vs the official SEC quarter, and the offline fixture regression check."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

import numpy as np
import pandas as pd

from ..edgar.client import EdgarBlockedError, EdgarClient, EdgarError
from ..edgar.form4 import Form4ParseError, filing_to_rows, parse_form4
from ..edgar.index import fetch_filing_txt
from . import sec_datasets as sd
from .common import FetchPlan, confirm_plan, require_sec_user_agent
from .harness import ReportWriter
from .parse_accuracy import sample_accessions, score, write_report

REQUESTS_PER_FILING_COLD = 1  # the full submission .txt (edgar.index.fetch_filing_txt; falls back to 3 on a 404)
DATASETS_DIR = Path("datasets/parse_accuracy")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _normalise_parsed(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=sd.OFFICIAL_COLUMNS)
    df["issuer_cik"] = df["issuer_cik"].map(sd.norm_cik)  # same normalisers the scorer applies
    df["owner_ciks"] = df["owner_ciks"].map(sd.norm_owner_ciks)
    df["issuer_ticker"] = df["issuer_ticker"].map(sd.norm_ticker)
    return df


# --------------------------------------------------------------------------- live
def run_live(
    args,
    *,
    client_factory: Callable[..., EdgarClient] = EdgarClient,
    downloader: Callable | None = None,
    out: Callable[[str], None] = print,
) -> int:
    """Returns a process exit code. 0 ok, 2 refused (needs --yes), 4 aborted (SEC blocked us)."""
    require_sec_user_agent()  # raises MissingUserAgent BEFORE any request
    download = downloader or sd.download_quarter
    zpath = download(args.year, args.quarter, args.data_dir)
    con = sd.load_quarter(zpath)
    rejects = dict(con.execute("SELECT table_name, rejected_rows FROM load_rejects").fetchall())
    off = sd.official_rows(con)
    con.close()
    accs, hard = sample_accessions(off, args.sample, args.hard, args.seed)
    sample = off[off["accession_number"].isin(set(accs))]
    cik_of = sample.drop_duplicates("accession_number").set_index("accession_number")["issuer_cik"].to_dict()

    client = client_factory(cache_dir=args.cache_dir)
    try:

        def cached(a: str) -> bool:
            try:
                return client.cache_path(a).exists()
            except ValueError:
                return False

        uncached = [a for a in accs if not cached(a)]
        skipped_cap: list[str] = []
        if args.max_fetch is not None and len(uncached) > args.max_fetch:
            rng = np.random.default_rng(args.seed)
            keep = set(rng.permutation(uncached)[: args.max_fetch])
            skipped_cap = sorted(set(uncached) - keep)
            uncached = [a for a in uncached if a in keep]
        todo = [a for a in accs if a not in set(skipped_cap)]
        plan = FetchPlan(
            f"{len(uncached)} uncached filings of {len(accs)} sampled "
            f"({len(accs) - len(uncached) - len(skipped_cap)} cached, {len(skipped_cap)} skipped by --max-fetch)",
            REQUESTS_PER_FILING_COLD * len(uncached),
        )
        if not confirm_plan(plan, args.yes, out):
            return 2

        rows: list[dict] = []
        fetch_failed: dict[str, str] = {}
        parser_errors: dict[str, str] = {}
        scored: list[str] = []
        for i, a in enumerate(todo, 1):
            if i % 100 == 0:
                out(f"  {i}/{len(todo)} filings, {client.request_count} requests so far")
            try:
                xml = client.read_cached(a, ".xml")
                if xml is None:
                    xml = fetch_filing_txt(client, cik_of[a], a).xml
            except EdgarBlockedError as exc:  # network/blocked problem: never blame the parser
                out(f"ABORT: {exc}. No report written; fix SEC_USER_AGENT / slow down and re-run (the cache resumes).")
                return 4
            except (EdgarError, ValueError) as exc:
                fetch_failed[a] = f"{type(exc).__name__}: {exc}"
                continue
            scored.append(a)
            try:
                rows.extend(filing_to_rows(parse_form4(xml, a)))
            except Form4ParseError as exc:  # a parse failure DOES count against the parser (rows go missing)
                parser_errors[a] = str(exc)
            except Exception as exc:
                parser_errors[a] = f"{type(exc).__name__}: {exc}"
        sample_scored = sample[sample["accession_number"].isin(set(scored))]
        res = score(_normalise_parsed(rows), sample_scored, hard=hard & set(scored))
        notes = [
            "Official datasets derive from the same XML, so this measures parser fidelity, not filer truthfulness.",
            f"Sampled {len(accs)} accessions; scored {len(scored)}. Excluded from scoring (NOT parser errors): "
            f"{len(fetch_failed)} fetch failures, {len(skipped_cap)} not fetched because of --max-fetch.",
            f"Parser exceptions counted against the parser: {len(parser_errors)}.",
        ]
        for label, d in (("Fetch failures", fetch_failed), ("Parser errors", parser_errors)):
            if d:
                notes.append(f"{label} (first 30): " + "; ".join(f"{k}: {v}" for k, v in list(d.items())[:30]))
        path = write_report(
            res,
            ReportWriter(f"parse_accuracy/{args.year}q{args.quarter}", root=args.reports_root),
            notes="\n\n".join(notes),
            metadata={
                "dataset": f"sec_insider_transactions_{args.year}q{args.quarter}",
                "dataset_sha256": _sha256(Path(zpath)),
                "seed": args.seed,
                "sample_n": args.sample,
                "hard_n": args.hard,
                "n_sampled": len(accs),
                "n_scored": len(scored),
                "n_fetch_failed": len(fetch_failed),
                "n_skipped_max_fetch": len(skipped_cap),
                "n_parser_errors": len(parser_errors),
                "http_requests": client.request_count,
                "official_rows_rejected_at_load": rejects,
                "cache_dir": str(args.cache_dir),
            },
        )
        out(
            f"row F1 {res['row_f1']:.4f} (strict {res['row_f1_strict']:.4f}); filing all-correct "
            f"{res['filing_all_correct_rate']:.4f}; report: {path}"
        )
        return 0
    finally:
        client.close()


# --------------------------------------------------------------------------- offline fixtures
def read_expected(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, comment="#", dtype=str, keep_default_na=False)
    missing = set(sd.OFFICIAL_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"{path}: missing columns {sorted(missing)}")
    df = df.astype(object).where(df != "", None)
    df["row_idx"] = df["row_idx"].astype(int)
    for c in ("shares", "price_per_share", "post_transaction_shares"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date
    return df[sd.OFFICIAL_COLUMNS]


def run_offline(args, out: Callable[[str], None] = print) -> int:
    """Score the parser on the committed fixtures vs the hand-transcribed expected rows. Exit 1 on regression."""
    expected = read_expected(Path(args.expected))
    rows: list[dict] = []
    for acc in sorted(expected["accession_number"].unique()):
        xml = (Path(args.fixtures_dir) / f"{acc}.xml").read_bytes()
        rows.extend(filing_to_rows(parse_form4(xml, acc)))
    res = score(_normalise_parsed(rows), expected)
    baseline = json.loads(Path(args.baseline).read_text()) if Path(args.baseline).exists() else {}
    fm = [v for v in res["field_exact_match"].values() if v is not None]
    observed = {
        "row_f1_strict": res["row_f1_strict"],
        "filing_all_correct_rate": res["filing_all_correct_rate"],
        "min_field_exact_match": min(fm) if fm else 0.0,
    }
    regressions = [
        f"{k}: {observed[k]:.4f} < recorded {baseline[k]:.4f}"
        for k in observed
        if k in baseline and observed[k] < baseline[k] - 1e-12
    ]
    for k in ("n_accessions", "n_official_rows"):
        if k in baseline and res[k] != baseline[k]:
            regressions.append(
                f"{k}: {res[k]} != recorded {baseline[k]} (expected-rows file changed without updating the baseline)"
            )
    notes = (
        "**Fixtures are reconstructed/synthetic** (see tests/edgar/fixtures/README.md): 8 reconstructed from "
        "sec-api.io JSON, the rest hand-built. This is a parser regression check against hand-transcribed "
        "expected rows, NOT the official-dataset accuracy (E1), which needs real XML and SEC_USER_AGENT.\n\n"
        f"Recorded baseline: `{json.dumps({k: v for k, v in baseline.items() if not k.startswith('_')})}`. "
        f"Status: {'REGRESSION: ' + '; '.join(regressions) if regressions else 'OK'}"
    )
    path = write_report(
        res,
        ReportWriter("parse_accuracy_fixtures", root=args.reports_root),
        notes=notes,
        metadata={
            "dataset": "tests/edgar/fixtures (reconstructed + synthetic)",
            "expected_csv_sha256": _sha256(Path(args.expected)),
            "regressions": regressions,
            "observed": observed,
        },
    )
    out(
        f"fixtures: {res['n_accessions']} filings, {res['n_official_rows']} rows; strict row F1 "
        f"{res['row_f1_strict']:.4f}, filing all-correct {res['filing_all_correct_rate']:.4f}; report: {path}"
    )
    if regressions:
        out("REGRESSION: " + "; ".join(regressions))
        return 1
    return 0
