"""E4 runner: historical event study over the official SEC datasets, following docs/event_study_prereg.md.

Pipeline: official quarters -> event rows (event_inputs) -> events (market.event_study.build_events) ->
prices (market.prices, cached) -> run_event_study per kind -> pre-registered analysis (Holm, decision rules)
-> reports/<event_study|event_study_dryrun|event_study_sample>/latest.{md,json}.

Guards: a real (confirmatory) run needs a real pre-registration commit hash in the prereg doc; a price-fetch
pass with ANY provider error is a failed pass (no report) because missing prices would otherwise masquerade
as delistings (survivorship) in the drop-reason table.
"""

from __future__ import annotations

import hashlib
import re
import subprocess
from collections.abc import Callable
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from ..market import event_study as es
from ..market import prices as pr
from .common import FetchPlan, confirm_plan, jsonable, require_sec_user_agent
from .event_inputs import build_event_rows, load_quarter_rows, quarter_status
from .harness import ReportWriter
from .ticker_map import load_company_tickers

PREREG_PATH = Path("docs/event_study_prereg.md")
PRIMARY_KINDS = ["cluster_buy", "single_buy", "officer_buy", "large_buy", "discretionary_sell", "planned_sell"]
SENS_NAME = "cluster_buy_sens_k3_n60"
W20, W5, W60, WPRE = es.wname(0, 20), es.wname(0, 5), es.wname(0, 60), es.wname(-20, -1)
_HASH_RE = re.compile(r"Pre-registration commit:[\s*`]*([0-9a-fA-F]{7,40})(?![0-9a-zA-Z])")

NOTE_CAVEATS = (
    "Abnormal returns are NOT tradeable alpha (costs, liquidity, capacity not modelled). This study makes no claim of "
    "detecting illegal trading: Form 4 trades are legally reported transactions.\n\n"
    "Historical timing: the official datasets have no EDGAR acceptance time, so FILING_DATE is the information date and "
    "day 0 is the first trading session STRICTLY AFTER the filing date (conservative vs the live pipeline's acceptance "
    "time; the first-day reaction to pre-close filings is excluded). Same-day filings of one issuer form one event.\n\n"
    "10b5-1 status comes from SUBMISSION.AFF10B5ONE only; sells from quarters without that column are excluded from "
    "BOTH sell kinds (H3 therefore covers only later quarters). Form 4/A is excluded. Prices are Yahoo adjusted closes "
    "(survivorship bias: delisted names are often missing, which flatters buy results). The market-cap-floor analysis is "
    "deferred until shares-outstanding data exist (plan B2)."
)


# --------------------------------------------------------------------------- prereg guard
def check_prereg(path: str | Path = PREREG_PATH, verify_git: bool = True) -> tuple[bool, str | None, str]:
    """(ok, commit_hash, message). ok only if the 'Pre-registration commit:' line holds a real hash."""
    p = Path(path)
    if not p.exists():
        return False, None, f"{p} not found."
    m = _HASH_RE.search(p.read_text())
    if not m:
        return (
            False,
            None,
            (
                f"{p} has no real 'Pre-registration commit:' hash (still a placeholder).\n"
                "To run the confirmatory study:\n"
                f"  1. review/edit {p} (it is a DRAFT) and `git add {p} && git commit`\n"
                f"  2. put that commit's hash on the line `**Pre-registration commit: <hash>**` and commit again\n"
                "  3. re-run. To exercise the pipeline without the check use --dry-run (writes reports/event_study_dryrun/)."
            ),
        )
    h = m.group(1).lower()
    if verify_git:
        try:
            r = subprocess.run(
                ["git", "cat-file", "-e", f"{h}^{{commit}}"],
                cwd=p.resolve().parent,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if r.returncode != 0:
                return False, h, f"commit {h} named in {p} does not exist in this repository."
        except (OSError, subprocess.SubprocessError):
            pass  # git unavailable: format check only
    return True, h, "ok"


# --------------------------------------------------------------------------- helpers
# Pre-registered sample (docs/event_study_prereg.md section 4): 40 quarters, ending at the newest dataset SEC had
# published at registration (2026-11-14: 2026Q1; 2026Q2/Q3 returned 404).
REGISTERED_RANGE = ((2016, 2), (2026, 1))
_CLI_DEFAULT_RANGE = ((2016, 1), (2025, 4))


def last_complete_quarter(today: date | None = None) -> tuple[int, int]:
    t = today or date.today()
    q = (t.month - 1) // 3 + 1
    return (t.year, q - 1) if q > 1 else (t.year - 1, 4)


def quarters_in_range(
    from_year: int, to_year: int, today: date | None = None, from_quarter: int = 1, to_quarter: int = 4
) -> list[tuple[int, int]]:
    """Quarters from (from_year, from_quarter) to (to_year, to_quarter), capped at the last complete calendar quarter.

    SEC publishes each quarter's dataset some weeks after it ends, so the pre-registered range is stated explicitly
    (docs/event_study_prereg.md section 4) rather than derived from today's date."""
    ly, lq = last_complete_quarter(today)
    return [
        (y, q)
        for y in range(from_year, to_year + 1)
        for q in range(1, 5)
        if (from_year, from_quarter) <= (y, q) <= (to_year, to_quarter) and (y, q) <= (ly, lq)
    ]


def parse_provider(spec: str, min_interval: float = 0.5) -> tuple[pr.PriceProvider, float, str]:
    if spec == "yahoo":
        return pr.YahooChartProvider(), min_interval, "yahoo_chart"
    if spec.startswith("csvdir:"):
        return pr.CSVDirProvider(spec.split(":", 1)[1]), 0.0, "csv_dir"
    raise ValueError(f"unknown --prices {spec!r} (use 'yahoo' or 'csvdir:<path>')")


def build_all_events(rows: pd.DataFrame, kinds: list[str]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for k in kinds:
        out[k] = es.build_events(rows, k, min_price=1.0, dedup_days=30)
    if "cluster_buy" in kinds:  # pre-registered sensitivity (reported only)
        out[SENS_NAME] = es.build_events(rows, "cluster_buy", min_price=1.0, dedup_days=60, k=3, window_days=60)
    return out


def restrict_tickers(
    events: dict[str, pd.DataFrame], max_tickers: int | None
) -> tuple[dict[str, pd.DataFrame], list[str]]:
    allt = (
        pd.concat([e["ticker"] for e in events.values() if len(e)], ignore_index=True)
        if events
        else pd.Series(dtype=str)
    )
    counts = allt.value_counts()
    order = sorted(counts.index, key=lambda t: (-counts[t], t))
    keep = order if max_tickers is None else order[:max_tickers]
    ks = set(keep)
    return {k: e[e["ticker"].isin(ks)].reset_index(drop=True) for k, e in events.items()}, sorted(ks)


def plan_price_requests(cache: pr.PriceCache, tickers: list[str], start, end) -> int:
    meta = cache._load_meta()
    s, e = pd.Timestamp(start).normalize(), min(pd.Timestamp(end).normalize(), cache._yesterday())
    return sum(len(cache.missing_ranges(meta.get(t.upper()), s, e)) for t in tickers)


# --------------------------------------------------------------------------- analysis
def _cell(out: dict, model: str, w: str) -> dict | None:
    return (out.get("results", {}).get(model) or {}).get(w)


def _nz(v):
    return None if v is None or (isinstance(v, float) and np.isnan(v)) else v


def analyze(results: dict[str, dict], alpha: float = 0.05) -> dict:
    """Pre-registered analysis (docs/event_study_prereg.md sections 2, 6, 7). Mechanical, no discretion."""
    # ---- family 1: H1, H2, H3
    prim: list[dict] = []
    for hid, kind, label in (
        ("H1", "cluster_buy", "cluster buys: BMP t, CAR[0,20] > 0"),
        ("H2", "officer_buy", "CEO/CFO buys: BMP t, CAR[0,20] > 0"),
    ):
        out = results.get(kind)
        c = _cell(out, "market_model", W20) if out else None
        ma = _cell(out, "market_adjusted", W20) if out else None
        cts = (out or {}).get("calendar_time") or {}
        row = {"id": hid, "hypothesis": label, "kind": kind, "n": c["n"] if c else 0}
        if c and c["n"] >= 2 and _nz(c["p_bmp"]) is not None:
            row.update(
                mean_car=c["mean_car"],
                t_bmp=c["t_bmp"],
                p_bmp=c["p_bmp"],
                t_cs=c["t_cs"],
                ci_lo=c["ci_lo"],
                ci_hi=c["ci_hi"],
                mean_car_market_adjusted=ma["mean_car"] if ma else None,
                calendar_alpha_annual=cts.get("alpha_annual") if cts.get("ok") else None,
                calendar_alpha_t=cts.get("alpha_t") if cts.get("ok") else None,
                p=c["p_bmp"],
            )
        prim.append(row)
    d = results.get("discretionary_sell")
    pl = results.get("planned_sell")
    h3 = {
        "id": "H3",
        "hypothesis": "discretionary sells vs planned (10b5-1) sells: Welch t on CAR[0,20]",
        "kind": "discretionary_sell - planned_sell",
        "n": 0,
    }
    if d and pl:
        x = d["events"].get(f"car_mm_{W20}", pd.Series(dtype=float)).dropna().to_numpy(float)
        y = pl["events"].get(f"car_mm_{W20}", pd.Series(dtype=float)).dropna().to_numpy(float)
        h3["n"] = f"{len(x)} vs {len(y)}"
        if len(x) >= 2 and len(y) >= 2:
            t, p = stats.ttest_ind(x, y, equal_var=False)
            h3.update(
                mean_car=float(x.mean() - y.mean()),
                mean_discretionary=float(x.mean()),
                mean_planned=float(y.mean()),
                t_welch=float(t),
                p=float(p),
            )
    prim.append(h3)
    ps = [r.get("p", np.nan) if r.get("p") is not None else np.nan for r in prim]
    adj, rej = es.holm_correction(ps)
    for r, a, j in zip(prim, adj, rej):
        r["p_holm"] = None if np.isnan(a) else float(a)
        r["verdict"] = _verdict(r, bool(j), alpha)
    n_fam1 = int(np.sum(~np.isnan(np.asarray(ps, float))))

    # ---- family 2: secondary BMP tests over kinds x windows (primary cells excluded)
    sec: list[dict] = []
    primary_cells = {("cluster_buy", W20), ("officer_buy", W20)}
    for kind, out in results.items():
        for w in (W5, W20, W60):
            c = _cell(out, "market_model", w)
            if not c:
                continue
            sec.append(
                {
                    "kind": kind,
                    "window": w,
                    **{
                        k: c[k]
                        for k in (
                            "n",
                            "mean_car",
                            "median_car",
                            "share_positive",
                            "t_cs",
                            "p_cs",
                            "t_bmp",
                            "p_bmp",
                            "ci_lo",
                            "ci_hi",
                        )
                    },
                    "primary_cell": (kind, w) in primary_cells,
                }
            )
    fam2 = [i for i, r in enumerate(sec) if not r["primary_cell"] and _nz(r["p_bmp"]) is not None]
    if fam2:
        a2, r2 = es.holm_correction([sec[i]["p_bmp"] for i in fam2])
        for i, a, j in zip(fam2, a2, r2):
            sec[i]["p_bmp_holm"] = float(a)
            sec[i]["significant_holm"] = bool(j)
    drift, robust = [], []
    for kind, out in results.items():
        c = _cell(out, "market_model", WPRE)
        if c:
            drift.append(
                {
                    "kind": kind,
                    "window": WPRE,
                    "n": c["n"],
                    "mean_car": c["mean_car"],
                    "t_bmp": c["t_bmp"],
                    "p_bmp": c["p_bmp"],
                    "note": "diagnostic; significant pre-drift = momentum/reversal contamination",
                }
            )
        c = _cell(out, "market_adjusted", W20)
        if c:
            robust.append(
                {
                    "kind": kind,
                    "window": W20,
                    "model": "market_adjusted",
                    "n": c["n"],
                    "mean_car": c["mean_car"],
                    "t_cs": c["t_cs"],
                    "p_cs": c["p_cs"],
                    "ci_lo": c["ci_lo"],
                    "ci_hi": c["ci_hi"],
                }
            )
    cal = []
    for kind, out in results.items():
        ct = out.get("calendar_time")
        if ct is None:
            continue
        cal.append(
            {
                "kind": kind,
                **(
                    {k: ct.get(k) for k in ("alpha_annual", "alpha_t", "alpha_p", "n_days", "mean_n_stocks")}
                    if ct.get("ok")
                    else {"note": ct.get("reason")}
                ),
            }
        )

    # ---- S6 ranking comparison (abs_net only; pct_mcap and cluster deferred)
    s6: dict = {
        "status": "partial",
        "note": "abs_net (total_value) on single_buy events only; pct_mcap deferred "
        "(needs shares outstanding, plan B2); cluster-score ranking not run.",
    }
    sb = results.get("single_buy")
    if sb is not None and len(sb["events"]):
        ev = sb["events"]
        dd = es.score_decile_analysis(ev, "total_value", f"car_mm_{W20}") if "total_value" in ev else {"ok": False}
        s6["abs_net"] = {k: v for k, v in dd.items() if k != "table"}
    return {
        "primary": prim,
        "secondary": sec,
        "drift": drift,
        "robustness": robust,
        "calendar": cal,
        "s6": s6,
        "n_tests_family1": n_fam1,
        "n_tests_family2": len(fam2),
    }


def _verdict(r: dict, rejected: bool, alpha: float) -> str:
    """Section 7 decision rules, applied mechanically."""
    if r.get("p") is None or r.get("p_holm") is None:
        return "no data"
    if r["id"] == "H3":
        ok = rejected and r["mean_discretionary"] < r["mean_planned"]
        return "supported" if ok else "not supported"
    ci_excl = r["ci_lo"] > 0
    ok = (
        r["mean_car"] > 0
        and rejected
        and ci_excl
        and (r.get("mean_car_market_adjusted") or 0) > 0
        and np.sign(r["t_cs"]) == np.sign(r["mean_car"])
    )
    if not ok:
        return "not supported"
    at, aa = r.get("calendar_alpha_t"), r.get("calendar_alpha_annual")
    if aa is not None and at is not None and (aa < 0 or abs(at) < 1):
        return "supported but FRAGILE (calendar-time alpha has opposite sign or |t| < 1)"
    return "supported"


PREREG_STATUS = [
    ("H1", "run", ""),
    ("H2", "run", ""),
    ("H3", "run", "only quarters with SUBMISSION.AFF10B5ONE populated"),
    ("S1", "run", "single_buy CAR[0,5],[0,60]"),
    ("S2", "run", "cluster/officer CAR[0,5],[0,60]"),
    ("S3", "run", "large_buy CAR[0,20]"),
    ("S4", "run", "planned and discretionary sells alone"),
    ("S5", "run", "calendar-time FF3 alpha per kind (needs Ken French factors)"),
    (
        "S6",
        "partial",
        "abs_net only; pct_mcap deferred until shares-outstanding data (plan B2); cluster ranking not run",
    ),
    ("S7", "run", "pre-event drift CAR[-20,-1]"),
    ("S8", "run", "market-adjusted AR"),
    ("Market-cap-floor ($300M) analysis", "deferred", "no shares-outstanding data until plan B2"),
    ("Exercise-and-sell events", "deferred", "needs derivative-table parsing (as pre-registered)"),
    ("Sensitivity k=3, N=60, dedup 60d", "run", f"event kind {SENS_NAME}, reported only"),
]


# --------------------------------------------------------------------------- orchestration
def run_study(
    args,
    *,
    today: date | None = None,
    out: Callable[[str], None] = print,
    downloader: Callable | None = None,
    client_factory: Callable | None = None,
    prereg_path: str | Path | None = None,
    verify_git: bool = True,
) -> int:
    """Exit codes: 0 ok, 2 refused (prereg / --yes), 3 price pass incomplete, 5 no usable data."""
    sample_mode = bool(args.dry_run or args.max_tickers is not None)
    name = "event_study_dryrun" if args.dry_run else ("event_study_sample" if sample_mode else "event_study")
    prereg_hash, prereg_sha = None, None
    pp = Path(prereg_path or PREREG_PATH)
    if not sample_mode:
        ok, prereg_hash, msg = check_prereg(pp, verify_git=verify_git)
        if not ok:
            out("REFUSING to run the confirmatory event study: " + msg)
            return 2
    if pp.exists():
        prereg_sha = hashlib.sha256(pp.read_bytes()).hexdigest()

    rng = ((args.from_year, getattr(args, "from_quarter", 1)), (args.to_year, getattr(args, "to_quarter", 4)))
    if not sample_mode:
        # confirmatory: exactly the registered sample. Defaults mean "use it"; anything else is a deviation.
        if rng != REGISTERED_RANGE and rng != _CLI_DEFAULT_RANGE:
            out(
                f"REFUSING: the pre-registered sample is {REGISTERED_RANGE[0][0]}Q{REGISTERED_RANGE[0][1]}.."
                f"{REGISTERED_RANGE[1][0]}Q{REGISTERED_RANGE[1][1]}; a different range needs a dated amendment "
                "in docs/event_study_prereg.md first."
            )
            return 2
        rng = REGISTERED_RANGE
    quarters = quarters_in_range(rng[0][0], rng[1][0], today, rng[0][1], rng[1][1])
    if args.dry_run:
        quarters = quarters[-2:]  # tiny sample: the last two complete quarters in range
    if not quarters:
        out("no complete quarters in the requested range")
        return 5
    kinds = [k.strip() for k in args.kinds.split(",") if k.strip()]
    bad = [k for k in kinds if k not in PRIMARY_KINDS]
    if bad:
        out(f"unknown kinds {bad}; choose from {PRIMARY_KINDS}")
        return 2
    max_tickers = args.max_tickers if args.max_tickers is not None else (8 if args.dry_run else None)

    # SEC access needed? (uncached quarters or company_tickers.json)
    need_dl = [yq for yq in quarters if quarter_status(*yq, args.data_dir, args.rows_cache) == "missing"]
    tickers_cache = Path(args.tickers_cache)
    need_tickers = not tickers_cache.exists()
    need_sec = bool(need_dl) or need_tickers
    out(
        f"quarters {quarters[0][0]}q{quarters[0][1]}..{quarters[-1][0]}q{quarters[-1][1]} ({len(quarters)}); "
        f"{len(need_dl)} need download; company_tickers cached: {not need_tickers}"
    )
    client = None
    if need_sec:
        require_sec_user_agent()  # before ANY sec.gov request
        if not confirm_plan(
            FetchPlan(f"SEC bulk downloads ({len(need_dl)} zips + tickers)", len(need_dl) + int(need_tickers)),
            args.yes,
            out,
        ):
            return 2
        from ..edgar.client import EdgarClient

        client = (client_factory or EdgarClient)(cache_dir=None)

    frames, infos = [], []
    for y, q in quarters:
        df, info = load_quarter_rows(y, q, args.data_dir, args.rows_cache, downloader)
        frames.append(df)
        infos.append(info)
    sec_tickers = load_company_tickers(tickers_cache, client=client)
    if client is not None:
        client.close()
    rows, input_info = build_event_rows(frames, sec_tickers, infos)
    events = build_all_events(rows, kinds)
    events, tickers = restrict_tickers(events, max_tickers)
    n_events_built = {k: int(len(v)) for k, v in events.items()}
    if not tickers:
        out("no events built; nothing to study")
        return 5

    allacc = pd.concat([e["accepted_at"] for e in events.values() if len(e)])
    start = allacc.min().normalize() - pd.Timedelta(days=430)
    end = allacc.max().normalize() + pd.Timedelta(days=130)
    provider, min_interval, pname = parse_provider(args.prices)
    cache = pr.PriceCache(provider, args.price_cache, min_interval=min_interval, today=today)
    want = sorted(set(tickers) | {"SPY"})
    if pname == "yahoo_chart":
        plan = FetchPlan(
            f"price fetch ({len(want)} tickers incl. SPY; only gaps are requested)",
            plan_price_requests(cache, want, start, end),
            (1.0 / min_interval) if min_interval > 0 else 8.0,
        )
        if not confirm_plan(plan, args.yes, out):
            return 2
    got = cache.get_many(want, start, end)
    cov = cache.coverage.to_dict()
    cov["not_attempted"] = sorted(set(want) - set(cache.coverage.requested))
    if cov["errors"] or cov["not_attempted"]:
        out(
            f"PRICE PASS INCOMPLETE: {len(cov['errors'])} provider errors, {len(cov['not_attempted'])} tickers never "
            f"requested. No report written (missing prices would be mistaken for delistings). The cache resumes on re-run."
        )
        return 3
    if "SPY" not in got:
        out("no SPY prices; cannot build the trading calendar")
        return 3
    market = got.pop("SPY")
    try:
        ff = pr.fama_french_daily(args.price_cache)
        ff_note = None
    except Exception as exc:  # network down and no cache
        ff, ff_note = None, f"Fama-French factors unavailable ({type(exc).__name__}); calendar-time alpha skipped"
        out(ff_note)

    results = {}
    for kind, ev in events.items():
        if len(ev):
            results[kind] = es.run_event_study(ev, got, market, ff=ff, n_boot=args.n_boot, seed=0)
    ana = analyze(results)

    cov_rows = []
    for kind, o in results.items():
        c = o["coverage"]
        cov_rows.append(
            {
                "kind": kind,
                "events_built": n_events_built.get(kind),
                "events_ok": c["n_events_ok"],
                "ok_rate": c["ok_rate"],
                "tickers_requested": c["tickers_requested"],
                "tickers_with_prices": c["tickers_with_prices"],
                **{f"drop:{k}": v for k, v in c["drop_reasons"].items()},
            }
        )
    for kind in events:
        if kind not in results:
            cov_rows.append({"kind": kind, "events_built": n_events_built.get(kind), "events_ok": 0})
    status = [{"item": i, "status": s, "note": n} for i, s, n in PREREG_STATUS]
    funnel = [{"step": k, "rows": v} for k, v in input_info["funnel"].items()]
    aff_rows = [
        {"filing_quarter": q, **v, "flag_share": v["with_flag"] / v["sell_rows"] if v["sell_rows"] else None}
        for q, v in sorted(input_info["aff10b5one_by_filing_quarter"].items())
    ]
    price_cov = [
        {"metric": k, "value": v if not isinstance(v, list) else len(v)}
        for k, v in cov.items()
        if k not in ("missing", "errors")
    ]
    miss = cov.get("missing") or []
    confirmatory = not sample_mode
    banner = (
        "CONFIRMATORY run under the pre-registration."
        if confirmatory
        else "NON-CONFIRMATORY "
        + ("dry run" if args.dry_run else "sample run (--max-tickers)")
        + ": truncated sample, results are not evidence and must not be cited."
    )
    notes = "\n\n".join(
        [banner, NOTE_CAVEATS]
        + ([ff_note] if ff_note else [])
        + [f"S6: {ana['s6']['note']}", f"Tickers without price data ({len(miss)}), first 100: {', '.join(miss[:100])}"]
    )
    metrics = {
        "confirmatory": confirmatory,
        "n_tests_family1": ana["n_tests_family1"],
        "n_tests_family2": ana["n_tests_family2"],
        "events_built": n_events_built,
        "s6": ana["s6"],
        "price_coverage": {k: v for k, v in cov.items() if k != "missing"},
        "ticker_mapping": input_info["ticker_mapping"],
        "input_funnel": input_info["funnel"],
        "primary": ana["primary"],
    }
    writer = ReportWriter(name, root=args.reports_root)
    path = writer.write(
        metrics=jsonable(metrics),
        tables=jsonable(
            {
                "Primary hypotheses (Holm family 1)": ana["primary"],
                "Pre-registered items: status": status,
                "Secondary: market-model CAR by kind and window (BMP p, Holm family 2)": ana["secondary"],
                "S5 calendar-time FF3 alpha": ana["calendar"],
                "S8 robustness: market-adjusted CAR[0,20]": ana["robustness"],
                "S7 diagnostic: pre-event drift": ana["drift"],
                "Coverage and drop reasons by kind": cov_rows,
                "Price coverage": price_cov,
                "Event-input funnel": funnel,
                "10b5-1 flag availability (sell rows by filing quarter)": aff_rows,
                "Ticker mapping by source": [
                    {"source": k, "ciks": v} for k, v in input_info["ticker_mapping"]["by_source"].items()
                ],
            }
        ),
        notes=notes,
        metadata=jsonable(
            {
                "prereg_commit": prereg_hash,
                "prereg_sha256": prereg_sha,
                "confirmatory": confirmatory,
                "dataset": "sec_insider_transactions "
                + f"{quarters[0][0]}q{quarters[0][1]}..{quarters[-1][0]}q{quarters[-1][1]}",
                "quarter_zip_sha256": {i.get("quarter"): i.get("zip_sha256") for i in infos},
                "price_provider": pname,
                "kinds": list(events),
                "seed": 0,
                "n_boot": args.n_boot,
                "windows": [list(w) for w in es.DEFAULT_WINDOWS],
                "est_window": list(es.EST_WINDOW),
                "market_proxy": "SPY",
                "factors": "Ken French daily FF3" if ff is not None else None,
                "max_tickers": max_tickers,
                "min_price": 1.0,
                "dedup_days": 30,
                "event_timing": "FILING_DATE; day 0 = first trading session strictly after the filing date",
            }
        ),
    )
    out(f"{banner}\nreport: {path}")
    for r in ana["primary"]:
        out(f"  {r['id']}: n={r['n']} mean={r.get('mean_car')} p_holm={r.get('p_holm')} -> {r['verdict']}")
    return 0
