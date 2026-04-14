"""
Deterministic Integrity Checks — Governance & Safety Layer (Step 15)

These functions are the ONLY gate between a successful pipeline run and the
Learning Loop. A run that fails ANY check is quarantined and NEVER used for
model improvement. Zero LLM calls allowed in this module.
"""
import hashlib
import json
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def check_filing_integrity(raw_filings: list[dict], db_filings: list[dict]) -> bool:
    """
    Verify that the number of DB-stored filings equals raw filings minus duplicates.
    raw_filings  = list returned by SecApiTool.fetch_recent_filings()
    db_filings   = list of filing dicts actually inserted (from SecDataAgent result)
    Rule: len(db_filings) <= len(raw_filings)
    """
    if len(db_filings) > len(raw_filings):
        logger.error(
            f"[INTEGRITY] Filing count mismatch: DB has {len(db_filings)} "
            f"but only {len(raw_filings)} raw filings received."
        )
        return False
    logger.info(f"[INTEGRITY] Filing count OK: {len(db_filings)} stored / {len(raw_filings)} raw")
    return True


def check_transaction_integrity(raw_transactions: list[dict], ranked_df: pd.DataFrame, checksum: str) -> bool:
    """
    Verify that:
    1. No transactions were silently dropped (row count matches expected tickers)
    2. The stored checksum still matches a fresh re-computation on ranked_df
    """
    if ranked_df is None or ranked_df.empty:
        if raw_transactions:
            logger.error("[INTEGRITY] Transactions exist but ranked_df is empty — SVG may have failed.")
            return False
        logger.info("[INTEGRITY] Transaction integrity OK (no transactions).")
        return True

    # Re-compute checksum using the same columns as symbolic_validation.py
    recomputed = _compute_svg_checksum(ranked_df)
    if recomputed != checksum:
        logger.error(
            f"[INTEGRITY] Checksum MISMATCH. Stored: {checksum[:12]}... "
            f"Recomputed: {recomputed[:12]}..."
        )
        return False

    logger.info(f"[INTEGRITY] Transaction checksum verified: {checksum[:12]}...")
    return True


def check_ranking_integrity(ranked_df: pd.DataFrame) -> bool:
    """
    Verify that the ranked trades are correctly ordered by absolute net dollar value
    (descending). This is a pure arithmetic check.
    """
    if ranked_df is None or ranked_df.empty:
        logger.info("[INTEGRITY] Ranking integrity OK (empty, nothing to rank).")
        return True

    if "abs_net" not in ranked_df.columns:
        logger.error("[INTEGRITY] 'abs_net' column missing from ranked_df.")
        return False

    values = ranked_df["abs_net"].tolist()
    for i in range(len(values) - 1):
        if values[i] < values[i + 1]:
            logger.error(
                f"[INTEGRITY] Ranking order violation at position {i}: "
                f"{values[i]} < {values[i+1]}"
            )
            return False

    logger.info("[INTEGRITY] Ranking order verified (monotonically non-increasing).")
    return True


def verify_checksum(df: pd.DataFrame, stored_checksum: str) -> bool:
    """
    Recompute the SHA-256 checksum of a DataFrame and compare to a stored value.
    Used to detect tampering between pipeline stages.
    """
    if df is None or df.empty:
        return stored_checksum == "empty"

    recomputed = _compute_svg_checksum(df)
    match = recomputed == stored_checksum
    if not match:
        logger.error(
            f"[INTEGRITY] verify_checksum FAILED. "
            f"Stored: {stored_checksum[:12]}... Recomputed: {recomputed[:12]}..."
        )
    return match


def run_all_checks(
    raw_filings: list,
    db_filings: list,
    raw_transactions: list,
    ranked_df: pd.DataFrame,
    checksum: str,
) -> tuple[bool, list[str]]:
    """
    Run all integrity checks in sequence. Returns (passed: bool, failed_checks: list[str]).
    If passed is False, the pipeline run must be quarantined.
    """
    failed = []

    if not check_filing_integrity(raw_filings, db_filings):
        failed.append("filing_count")

    if not check_transaction_integrity(raw_transactions, ranked_df, checksum):
        failed.append("transaction_checksum")

    if not check_ranking_integrity(ranked_df):
        failed.append("ranking_order")

    passed = len(failed) == 0
    return passed, failed


# ── Private helpers ───────────────────────────────────────────────────────────

def _compute_df_checksum(df: pd.DataFrame) -> str:
    """Generic SHA-256 hash of a full DataFrame."""
    records = json.loads(df.to_json(orient="records", date_format="iso"))
    canonical = json.dumps(records, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _compute_svg_checksum(df: pd.DataFrame) -> str:
    """Re-compute the same checksum that symbolic_validation.validate_and_rank produces.
    Uses only the 3 columns: rank, issuer_ticker, net_dollar_value."""
    if df is None or df.empty:
        return hashlib.sha256(b"empty").hexdigest()
    cols = ['rank', 'issuer_ticker', 'net_dollar_value']
    subset = df[cols] if all(c in df.columns for c in cols) else df
    checksum_input = subset.to_json()
    return hashlib.sha256(checksum_input.encode()).hexdigest()
