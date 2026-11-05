"""Symbolic validation gate: deterministic ranking of insider activity. ZERO LLM calls.

Signal definition (why this is not over-simplified)
  Primary   : net open-market dollar flow per ticker (code P = buy, code S = sell).
  Secondary : shown next to every ranked ticker so a reader can judge conviction quality
    - insider_count   how many distinct insiders traded (cluster buying is a stronger signal)
    - OFFICER / DIRECTOR role of the insiders on the dominant side
    - pct_holdings    size of the largest trade relative to that insider's position
    - planned_pct     share of gross $ reported under a pre-scheduled Rule 10b5-1 plan
                      (planned sales carry less information than discretionary ones)
  Grants, option exercises, tax withholding and gifts (codes A/M/F/G) are deliberately excluded:
  they are compensation mechanics, not conviction.
"""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd

REQUIRED = ["accession_number", "row_idx", "issuer_ticker", "code", "shares", "price_per_share"]
LARGE_STAKE = 0.25
CLUSTER_MIN_BUYERS = 2
CLUSTER_MIN_SELLERS = 3
PLANNED_THRESHOLD = 0.5


def compute_checksum(records: list[dict]) -> str:
    """Canonical SHA-256 over the fields that define a ranking."""
    canon = [[r["rank"], r["issuer_ticker"], round(float(r["net_dollar_value"]), 2),
              round(float(r["buy_value"]), 2), round(float(r["sell_value"]), 2)] for r in records]
    return hashlib.sha256(json.dumps(canon).encode()).hexdigest()


def _empty(stats: dict) -> tuple[pd.DataFrame, str, dict]:
    return pd.DataFrame(), hashlib.sha256(b"empty").hexdigest(), stats


def validate_and_rank(rows: list[dict] | pd.DataFrame, top_n: int = 5,
                      min_notional: float = 0.0) -> tuple[pd.DataFrame, str, dict]:
    """Return (ranked_df, checksum, stats). `stats` feeds the independent integrity checks."""
    df = pd.DataFrame(rows).copy() if not isinstance(rows, pd.DataFrame) else rows.copy()
    stats = {"rows_in": len(df), "dropped_invalid": 0, "dropped_inconsistent": 0,
             "dropped_joint_dups": 0, "notional_valid": 0.0, "notional_dups": 0.0, "notional_kept": 0.0,
             "notional_inconsistent": 0.0, "tickers": 0}
    if df.empty:
        return _empty(stats)
    for col in REQUIRED:
        if col not in df.columns:
            raise ValueError(f"validate_and_rank: missing column '{col}'")

    # 1. numeric hygiene ----------------------------------------------------
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce")
    df["price_per_share"] = pd.to_numeric(df["price_per_share"], errors="coerce")
    df["code"] = df["code"].astype(str).str.upper()
    valid = (df["shares"] > 0) & (df["price_per_share"] > 0) & df["code"].isin(["P", "S"]) \
        & df["issuer_ticker"].notna() & (df["issuer_ticker"].astype(str).str.len() > 0)
    stats["dropped_invalid"] = int((~valid).sum())
    df = df[valid].copy()
    if df.empty:
        return _empty(stats)
    df["dollar_value"] = (df["shares"] * df["price_per_share"]).round(2)

    # 2. the filer's code and acquired/disposed flag must agree (P→A, S→D) ----
    if "acquired_disposed" in df.columns:
        ad = df["acquired_disposed"].astype(str).str.upper()
        bad = ((df["code"] == "P") & (ad == "D")) | ((df["code"] == "S") & (ad == "A"))
        stats["dropped_inconsistent"] = int(bad.sum())
        stats["notional_inconsistent"] = float(df.loc[bad, "dollar_value"].sum())
        df = df[~bad].copy()
    stats["notional_valid"] = float(df["dollar_value"].sum())
    if df.empty:
        return _empty(stats)

    # 3. joint-filer double reporting: identical trade filed under several accession numbers ----
    key = ["issuer_ticker", "transaction_date", "code", "shares", "price_per_share"]
    for col in key:
        if col not in df.columns:
            df[col] = None
    df = df.sort_values(["accession_number", "row_idx"])
    dup = df.duplicated(subset=key, keep="first")
    stats["dropped_joint_dups"] = int(dup.sum())
    stats["notional_dups"] = float(df.loc[dup, "dollar_value"].sum())
    df = df[~dup].copy()
    stats["notional_kept"] = float(df["dollar_value"].sum())

    # 4. enrich -------------------------------------------------------------
    for col, default in [("insider_name", "Unknown"), ("insider_title", ""), ("issuer_name", "")]:
        df[col] = df[col].fillna(default).astype(str) if col in df.columns else default
    for col in ("is_officer", "is_director", "is_10b5_1"):
        df[col] = df[col].fillna(False).astype(bool) if col in df.columns else False
    post = pd.to_numeric(df.get("post_transaction_shares"), errors="coerce") if "post_transaction_shares" in df.columns \
        else pd.Series(np.nan, index=df.index)
    # buy: share of the *resulting* position that was just bought; sell: share of the *prior* position sold
    denom = np.where(df["code"] == "P", post, post + df["shares"])
    df["pct_holdings"] = np.where((denom > 0), np.clip(df["shares"] / denom, 0, 1), np.nan)
    df["signed"] = np.where(df["code"] == "P", df["dollar_value"], -df["dollar_value"])
    df["planned_value"] = np.where(df["is_10b5_1"], df["dollar_value"], 0.0)

    # 5. aggregate per ticker ---------------------------------------------------
    out = []
    for ticker, g in df.groupby("issuer_ticker"):
        buy = float(g.loc[g.code == "P", "dollar_value"].sum())
        sell = float(g.loc[g.code == "S", "dollar_value"].sum())
        gross = buy + sell
        if gross < min_notional or gross == 0:
            continue
        net = round(buy - sell, 2)
        direction = "BUY" if net > 0 else "SELL"
        side = g[g.code == ("P" if net > 0 else "S")]
        lead = side.groupby("insider_name")["dollar_value"].sum().sort_values(ascending=False)
        lead_name = lead.index[0]
        lead_row = side[side.insider_name == lead_name].sort_values("dollar_value", ascending=False).iloc[0]
        others = g["insider_name"].nunique() - 1
        side_insiders = side["insider_name"].nunique()
        pct = side["pct_holdings"].max()
        planned_pct = float(g["planned_value"].sum() / gross)

        tags = []
        if direction == "BUY" and side_insiders >= CLUSTER_MIN_BUYERS:
            tags.append("CLUSTER_BUY")
        if direction == "SELL" and side_insiders >= CLUSTER_MIN_SELLERS:
            tags.append("CLUSTER_SELL")
        if side["is_officer"].any():
            tags.append("OFFICER")
        elif side["is_director"].any():
            tags.append("DIRECTOR")
        if pd.notna(pct) and pct >= LARGE_STAKE:
            tags.append("LARGE_STAKE")
        if planned_pct >= PLANNED_THRESHOLD:
            tags.append("PLANNED_10b5-1")

        out.append({
            "issuer_ticker": ticker,
            "issuer_name": str(g["issuer_name"].iloc[0]),
            "insider_name": lead_name + (f" +{others} more" if others > 0 else ""),
            "insider_title": str(lead_row["insider_title"]),
            "net_dollar_value": net,
            "buy_value": round(buy, 2),
            "sell_value": round(sell, 2),
            "transaction_count": int(len(g)),
            "insider_count": int(g["insider_name"].nunique()),
            "pct_holdings": None if pd.isna(pct) else round(float(pct), 4),
            "planned_pct": round(planned_pct, 4),
            "dominant_direction": direction,
            "signal_tags": ",".join(tags),
        })

    stats["tickers"] = len(out)
    if not out:
        return _empty(stats)

    ranked = pd.DataFrame(out)
    ranked["abs_net"] = ranked["net_dollar_value"].abs()
    ranked = ranked.sort_values(["abs_net", "issuer_ticker"], ascending=[False, True]).head(top_n).reset_index(drop=True)
    ranked["rank"] = np.arange(1, len(ranked) + 1)
    checksum = compute_checksum(ranked.to_dict("records"))
    return ranked, checksum, stats
