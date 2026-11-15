"""E1: parse-accuracy scorer (parsed Form 4 rows vs SEC official Insider Transactions Data Sets)."""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
from scipy.optimize import linear_sum_assignment

from .sec_datasets import OFFICIAL_COLUMNS, norm_cik, norm_owner_ciks, norm_ticker

REL_TOL = 1e-4
MIN_SIM = 3.0  # of 5 match-features must agree for two rows to be considered the same transaction
NUM_FIELDS = ["shares", "price_per_share", "post_transaction_shares"]
STR_FIELDS = [
    "form_type",
    "issuer_cik",
    "issuer_ticker",
    "owner_ciks",
    "code",
    "acquired_disposed",
    "direct_indirect",
    "security_title",
]
DATE_FIELDS = ["transaction_date"]
FIELDS = [
    "form_type",
    "issuer_cik",
    "issuer_ticker",
    "owner_ciks",
    "transaction_date",
    "code",
    "shares",
    "price_per_share",
    "acquired_disposed",
    "post_transaction_shares",
    "direct_indirect",
    "security_title",
]


# ------------------------------------------------------------------ normalisation
def _isnull(v: Any) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NaT or (v is pd.NA)


def _s(v: Any) -> str | None:
    if _isnull(v):
        return None
    s = " ".join(str(v).split()).lower()
    return s or None


def _num(v: Any) -> float:
    if _isnull(v) or (isinstance(v, str) and not v.strip()):
        return float("nan")
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def _date(v: Any):
    if _isnull(v) or (isinstance(v, str) and not v.strip()):
        return None
    try:
        return pd.to_datetime(v).date()
    except Exception:
        return None


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    d = pd.DataFrame(index=range(len(df)))
    src = df.reset_index(drop=True)
    d["accession_number"] = src["accession_number"].astype(str).str.strip()
    d["row_idx"] = pd.to_numeric(src.get("row_idx", pd.Series(range(len(src)))), errors="coerce").fillna(1e9)
    d["form_type"] = (
        src["form_type"].map(lambda v: None if _isnull(v) else str(v).strip().upper() or None)
        if "form_type" in src
        else None
    )
    d["issuer_cik"] = src["issuer_cik"].map(norm_cik) if "issuer_cik" in src else None
    d["issuer_ticker"] = src["issuer_ticker"].map(norm_ticker) if "issuer_ticker" in src else None
    d["owner_ciks"] = src["owner_ciks"].map(norm_owner_ciks) if "owner_ciks" in src else None
    d["transaction_date"] = src["transaction_date"].map(_date) if "transaction_date" in src else None
    for c in ("code", "acquired_disposed", "direct_indirect"):
        d[c] = src[c].map(lambda v: None if _s(v) is None else _s(v).upper()) if c in src else None
    d["security_title"] = src["security_title"].map(_title) if "security_title" in src else None
    for c in NUM_FIELDS:
        d[c] = src[c].map(_num) if c in src else float("nan")
    return d


def _num_eq(a: float, b: float) -> bool:
    an, bn = math.isnan(a), math.isnan(b)
    if an or bn:
        return an and bn
    return math.isclose(a, b, rel_tol=REL_TOL, abs_tol=1e-9)


# The SEC Insider Transactions Data Sets store TRANS_PRICEPERSHARE rounded to the cent (verified on 2025Q3: 0 of
# 55,125 prices have more than 2 decimals), while filers often report 4 decimals (e.g. weighted averages like
# 16.6067). Price is therefore compared at the official dataset's own precision; rows where cent-rounding was the
# ONLY difference are counted separately (`price_rounding_only`) so the report shows it instead of hiding it.
PRICE_CENT_TOL = 0.005 + 1e-9


def _price_cent_eq(a: float, b: float) -> bool:
    an, bn = math.isnan(a), math.isnan(b)
    if an or bn:
        return an and bn
    return abs(a - b) <= PRICE_CENT_TOL


# Fields the official dataset stores rounded to 2 decimals (verified on 2025Q3 for both).
ROUNDED_2DP_FIELDS = ("price_per_share", "shares")


def _title(v) -> str | None:
    """Undo CSV-style quoting kept literally in the TSV: '"a (""b"")"' -> 'a ("b")'."""
    s = _s(v)
    if s is None:
        return None
    if len(s) >= 2 and s[0] == s[-1] == '"':
        s = s[1:-1].replace('""', '"')
    return s.strip() or None


def _field_eq(field: str, a: Any, b: Any) -> bool:
    if field in ROUNDED_2DP_FIELDS:
        return _num_eq(a, b) or _price_cent_eq(a, b)
    if field in NUM_FIELDS:
        return _num_eq(a, b)
    return a == b


# ------------------------------------------------------------------ matching
def _similarity(p: pd.Series, o: pd.Series) -> float:
    s = 0.0
    s += p["transaction_date"] == o["transaction_date"]
    s += p["code"] == o["code"]
    s += _num_eq(p["shares"], o["shares"])
    s += _field_eq("price_per_share", p["price_per_share"], o["price_per_share"])
    s += p["acquired_disposed"] == o["acquired_disposed"]
    return float(s)


def match_accession(parsed: pd.DataFrame, official: pd.DataFrame) -> list[tuple[int, int]]:
    """Optimal one-to-one assignment (positions into the given frames) with similarity >= MIN_SIM."""
    if parsed.empty or official.empty:
        return []
    P = parsed.to_dict("records")
    O = official.to_dict("records")  # noqa: E741  (P/O = parsed/official, mirrors the cost matrix axes)
    cost = np.zeros((len(P), len(O)))
    sim = np.zeros_like(cost)
    for i, p in enumerate(P):
        for j, o in enumerate(O):
            sim[i, j] = _similarity(pd.Series(p), pd.Series(o))
            # row_idx proximity only as tie-break (<1 total, scaled so it never outweighs one feature)
            tb = min(abs(p["row_idx"] - o["row_idx"]), 100) / 1000.0
            cost[i, j] = -sim[i, j] + tb
    ri, ci = linear_sum_assignment(cost)
    return [(int(i), int(j)) for i, j in zip(ri, ci) if sim[i, j] >= MIN_SIM]


# ------------------------------------------------------------------ scoring
def _flow_by_ticker(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    d = df[df["code"].isin(["P", "S"]) & df["issuer_ticker"].notna()].copy()
    val = (d["shares"] * d["price_per_share"]).fillna(0.0)
    d["flow"] = np.where(d["code"] == "P", val, -val)
    return d.groupby("issuer_ticker")["flow"].sum()


def score(parsed_df: pd.DataFrame, official_df: pd.DataFrame, hard: set[str] | None = None) -> dict:
    """Score parsed rows against official rows. Scored accessions = those present in official_df.

    ``hard``: optional set of accession numbers (the stratified hard-case sample) reported as ``hard_cases``."""
    off = normalise(official_df)
    par = normalise(parsed_df) if len(parsed_df) else normalise(pd.DataFrame(columns=OFFICIAL_COLUMNS))
    accs = sorted(off["accession_number"].unique())
    acc_set = set(accs)
    parsed_extra_accs = sorted(set(par["accession_number"]) - acc_set)
    par_scored = par[par["accession_number"].isin(acc_set)]

    field_ok = {f: 0 for f in FIELDS}
    n_matched = 0
    tp_strict = 0
    rounding_only = {f: 0 for f in ROUNDED_2DP_FIELDS}
    mismatches: list[dict] = []
    filing_ok: dict[str, bool] = {}
    filing_recall: dict[str, tuple[int, int]] = {}
    po = {a: g for a, g in par_scored.groupby("accession_number")}
    oo = {a: g for a, g in off.groupby("accession_number")}
    n_parsed = len(par_scored)
    n_official = len(off)

    for a in accs:
        o = oo[a].reset_index(drop=True)
        p = po.get(a, par_scored.iloc[0:0]).reset_index(drop=True)
        pairs = match_accession(p, o)
        mp = {i for i, _ in pairs}
        mo = {j for _, j in pairs}
        all_ok = len(pairs) == len(o) == len(p)
        for i, j in pairs:
            n_matched += 1
            row_ok = True
            for f in FIELDS:
                if _field_eq(f, p.at[i, f], o.at[j, f]):
                    field_ok[f] += 1
                    if f in ROUNDED_2DP_FIELDS and not _num_eq(p.at[i, f], o.at[j, f]):
                        rounding_only[f] += 1
                else:
                    row_ok = False
                    mismatches.append(
                        {
                            "accession_number": a,
                            "row_idx": int(o.at[j, "row_idx"]),
                            "field": f,
                            "ours": _show(p.at[i, f]),
                            "official": _show(o.at[j, f]),
                            "kind": "field",
                        }
                    )
            tp_strict += row_ok
            all_ok &= row_ok
        for j in range(len(o)):
            if j not in mo:
                mismatches.append(
                    {
                        "accession_number": a,
                        "row_idx": int(o.at[j, "row_idx"]),
                        "field": "__row__",
                        "ours": None,
                        "official": _row_repr(o.loc[j]),
                        "kind": "missing_row",
                    }
                )
        for i in range(len(p)):
            if i not in mp:
                mismatches.append(
                    {
                        "accession_number": a,
                        "row_idx": int(p.at[i, "row_idx"]),
                        "field": "__row__",
                        "ours": _row_repr(p.loc[i]),
                        "official": None,
                        "kind": "extra_row",
                    }
                )
        filing_ok[a] = bool(all_ok)
        filing_recall[a] = (len(pairs), len(o))

    prec = n_matched / n_parsed if n_parsed else 0.0
    rec = n_matched / n_official if n_official else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    sprec = tp_strict / n_parsed if n_parsed else 0.0
    srec = tp_strict / n_official if n_official else 0.0
    sf1 = 2 * sprec * srec / (sprec + srec) if sprec + srec else 0.0

    # net open-market dollar flow per ticker
    fo, fp = _flow_by_ticker(off), _flow_by_ticker(par_scored)
    # like-for-like: our shares and prices rounded to 2 decimals, as the official dataset stores them
    par_cents = par_scored.assign(
        price_per_share=pd.to_numeric(par_scored["price_per_share"], errors="coerce").round(2),
        shares=pd.to_numeric(par_scored["shares"], errors="coerce").round(2),
    )
    fpc = _flow_by_ticker(par_cents)
    tick = fo.index.union(fp.index).union(fpc.index)
    err_c = (fpc.reindex(tick).fillna(0.0) - fo.reindex(tick).fillna(0.0)).abs()
    fo_, fp_ = fo.reindex(tick).fillna(0.0), fp.reindex(tick).fillna(0.0)
    err = (fp_ - fo_).abs()
    nz = fo_.abs() > 0
    pct = (err[nz] / fo_[nz].abs()) * 100 if nz.any() else pd.Series(dtype=float)
    flow = {
        "n_tickers": int(len(tick)),
        "mae_usd": float(err.mean()) if len(err) else 0.0,
        "median_abs_pct_err": float(pct.median()) if len(pct) else 0.0,
        "mean_abs_pct_err": float(pct.mean()) if len(pct) else 0.0,
        "share_tickers_exact": float((err <= 0.01).mean()) if len(err) else 1.0,
        "mae_usd_cent_rounded": float(err_c.mean()) if len(err_c) else 0.0,
        "share_tickers_exact_cent_rounded": float((err_c <= 0.01).mean()) if len(err_c) else 1.0,
    }

    def subset_stats(accessions: list[str]) -> dict:
        n = len(accessions)
        if not n:
            return {"n": 0, "filing_exact_rate": None, "row_recall": None}
        m = sum(filing_recall[a][0] for a in accessions)
        t = sum(filing_recall[a][1] for a in accessions)
        return {
            "n": n,
            "filing_exact_rate": sum(filing_ok[a] for a in accessions) / n,
            "row_recall": m / t if t else None,
        }

    first = off.drop_duplicates("accession_number").set_index("accession_number")
    amend = [a for a in accs if first.at[a, "form_type"] == "4/A"]
    joint = [a for a in accs if first.at[a, "owner_ciks"] and "|" in first.at[a, "owner_ciks"]]

    return {
        "n_accessions": len(accs),
        "n_official_rows": n_official,
        "n_parsed_rows": n_parsed,
        "n_matched_rows": n_matched,
        "price_rounding_only": rounding_only["price_per_share"],
        "shares_rounding_only": rounding_only["shares"],
        "parsed_accessions_not_in_official": len(parsed_extra_accs),
        "field_exact_match": {f: (field_ok[f] / n_matched if n_matched else None) for f in FIELDS},
        "row_precision": prec,
        "row_recall": rec,
        "row_f1": f1,
        "row_precision_strict": sprec,
        "row_recall_strict": srec,
        "row_f1_strict": sf1,
        "filing_all_correct_rate": (sum(filing_ok.values()) / len(accs)) if accs else 0.0,
        "net_flow": flow,
        "amendments_4A": subset_stats(amend),
        "joint_filers": subset_stats(joint),
        **(
            {
                "hard_cases": subset_stats([a for a in accs if a in hard]),
                "random_cases": subset_stats([a for a in accs if a not in hard]),
            }
            if hard is not None
            else {}
        ),
        "mismatches": mismatches,
    }


def _show(v: Any):
    if isinstance(v, float) and math.isnan(v):
        return None
    return str(v) if not isinstance(v, (int, float, str, type(None))) else v


def _row_repr(r: pd.Series) -> str:
    return (
        f"{r['transaction_date']} {r['code']} {r['shares']}@{r['price_per_share']} "
        f"{r['acquired_disposed']} {r['issuer_ticker']}"
    )


# ------------------------------------------------------------------ reporting
def write_report(result: dict, writer, metadata: dict | None = None, notes: str = "", max_mismatches: int = 500):
    """Write the scorer result through a harness.ReportWriter."""
    fields = [{"field": f, "exact_match_rate": v} for f, v in result["field_exact_match"].items()]
    summary = [
        {"metric": k, "value": result[k]}
        for k in (
            "n_accessions",
            "n_official_rows",
            "n_parsed_rows",
            "n_matched_rows",
            "row_precision",
            "row_recall",
            "row_f1",
            "row_precision_strict",
            "row_recall_strict",
            "row_f1_strict",
            "filing_all_correct_rate",
            "price_rounding_only",
            "shares_rounding_only",
        )
    ]
    flow = [{"metric": k, "value": v} for k, v in result["net_flow"].items()]
    sub = [{"subset": "4/A", **result["amendments_4A"]}, {"subset": "joint filers", **result["joint_filers"]}]
    for key, label in (("hard_cases", "hard-case sample"), ("random_cases", "random sample")):
        if key in result:
            sub.append({"subset": label, **result[key]})
    mm = result["mismatches"]
    cats: dict[str, int] = {}
    for m in mm:
        key = m["kind"] if m["kind"] != "field" else f"field:{m['field']}"
        cats[key] = cats.get(key, 0) + 1
    cat_rows = [{"category": k, "count": v} for k, v in sorted(cats.items(), key=lambda kv: -kv[1])]
    metrics = {k: v for k, v in result.items() if k != "mismatches"}
    metrics["n_mismatches"] = len(mm)
    return writer.write(
        metrics=metrics,
        tables={
            "Summary": summary,
            "Per-field exact match": fields,
            "Net open-market dollar flow error": flow,
            "Amendment / joint-filer handling": sub,
            "Mismatch categories": cat_rows,
            f"Mismatches (first {max_mismatches})": mm[:max_mismatches],
        },
        notes=notes
        or ("Official datasets derive from the same XML, so this measures parser fidelity, not filer truthfulness."),
        metadata=metadata,
    )


# ------------------------------------------------------------------ sampling + CLI
def sample_accessions(
    official_df: pd.DataFrame, sample_n: int = 1000, hard_n: int = 100, seed: int = 0
) -> tuple[list[str], set[str]]:
    """Stratified sample of Form 4 / 4/A accessions: (sample_n - hard_n) random + hard_n hard cases.

    Hard = joint filer (>1 owner), 4/A, >5 rows, or any row with missing price.
    Returns (accessions, hard_set).
    """
    df = official_df[official_df["form_type"].isin(["4", "4/A"])]
    g = df.groupby("accession_number")
    info = pd.DataFrame(
        {
            "n_rows": g.size(),
            "amend": g["form_type"].first() == "4/A",
            "joint": g["owner_ciks"].first().map(lambda v: isinstance(v, str) and "|" in v),
            "noprice": g["price_per_share"].apply(lambda s: bool(s.isna().any())),
        }
    )
    info["hard"] = info["amend"] | info["joint"] | (info["n_rows"] > 5) | info["noprice"]
    rng = np.random.default_rng(seed)
    hard = sorted(info.index[info["hard"]])
    easy = sorted(info.index[~info["hard"]])
    hard_pick = list(rng.permutation(hard)[:hard_n])
    rest = sample_n - len(hard_pick)
    easy_pick = list(rng.permutation(easy)[: max(rest, 0)])
    # if there are too few easy ones, fill from remaining hard
    if len(easy_pick) < rest:
        left = [a for a in hard if a not in set(hard_pick)]
        hard_pick += list(rng.permutation(left)[: rest - len(easy_pick)])
    return sorted(map(str, hard_pick + easy_pick)), set(map(str, hard_pick))


def run(
    year: int,
    q: int,
    sample_n: int = 1000,
    hard_n: int = 100,
    seed: int = 0,
    parser: Callable[[str], list[dict]] | None = None,
    data_dir: str = "data/sec_datasets",
    reports_root: str = "reports",
    write: bool = True,
) -> dict:
    """Download+load the quarter, sample accessions, and (if a parser is given) score it."""
    from .harness import ReportWriter
    from .sec_datasets import download_quarter, load_quarter, official_rows

    zpath = download_quarter(year, q, data_dir)
    con = load_quarter(zpath)
    rejects = dict(con.execute("SELECT table_name, rejected_rows FROM load_rejects").fetchall())
    off = official_rows(con)
    accs, hard = sample_accessions(off, sample_n, hard_n, seed)
    sample = off[off["accession_number"].isin(set(accs))]
    out: dict = {"accessions": accs, "hard": sorted(hard), "official": sample, "load_rejects": rejects}
    if parser is None:
        return out
    rows: list[dict] = []
    for a in accs:
        try:
            rows.extend(parser(a) or [])
        except Exception as e:  # a parser crash counts as all rows missing
            out.setdefault("parser_errors", {})[a] = repr(e)
    parsed = pd.DataFrame(rows, columns=None) if rows else pd.DataFrame(columns=OFFICIAL_COLUMNS)
    res = score(parsed, sample)
    out["result"] = res
    if write:
        write_report(
            res,
            ReportWriter(f"parse_accuracy/{year}q{q}", root=reports_root),
            metadata={
                "dataset": f"sec_insider_transactions_{year}q{q}",
                "dataset_sha256": __import__("hashlib").sha256(open(zpath, "rb").read()).hexdigest(),
                "seed": seed,
                "sample_n": sample_n,
                "hard_n": hard_n,
                "n_parser_errors": len(out.get("parser_errors", {})),
                "official_rows_rejected_at_load": rejects,
            },
        )
    return out
