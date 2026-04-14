"""
Tests for Step 15: Governance & Safety Layer
Covers: integrity_checks.py and learning_loop.py
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock
from skills.integrity_checks import (
    check_filing_integrity,
    check_transaction_integrity,
    check_ranking_integrity,
    verify_checksum,
    run_all_checks,
    _compute_df_checksum,
    _compute_svg_checksum,
)
from skills.learning_loop import LearningLoop


# ── integrity_checks tests ────────────────────────────────────────────────────

def test_check_filing_integrity_pass():
    raw = [{"accessionNo": "A"}, {"accessionNo": "B"}, {"accessionNo": "C"}]
    db  = [{"accession_number": "A"}, {"accession_number": "B"}]  # 1 dup skipped
    assert check_filing_integrity(raw, db) is True


def test_check_filing_integrity_fail_db_has_more():
    raw = [{"accessionNo": "A"}]
    db  = [{"accession_number": "A"}, {"accession_number": "GHOST"}]  # DB has extra
    assert check_filing_integrity(raw, db) is False


def test_check_transaction_integrity_pass():
    df = pd.DataFrame([{
        "issuer_ticker": "AAPL", "net_dollar_value": -150000.0,
        "abs_net": 150000.0, "transaction_count": 1,
        "total_buys": 0.0, "total_sells": 150000.0,
        "dominant_direction": "SELL", "rank": 1
    }])
    checksum = _compute_svg_checksum(df)
    raw_txns = [{"accession_number": "X", "shares": 1000, "price_per_share": 150.0}]
    assert check_transaction_integrity(raw_txns, df, checksum) is True


def test_check_transaction_integrity_fail_tampered_checksum():
    df = pd.DataFrame([{
        "issuer_ticker": "AAPL", "net_dollar_value": -150000.0,
        "abs_net": 150000.0, "transaction_count": 1,
        "total_buys": 0.0, "total_sells": 150000.0,
        "dominant_direction": "SELL", "rank": 1
    }])
    raw_txns = [{"shares": 1000}]
    assert check_transaction_integrity(raw_txns, df, "tampered_checksum_xyz") is False


def test_check_ranking_integrity_pass():
    df = pd.DataFrame([
        {"issuer_ticker": "AAPL", "abs_net": 500000.0, "rank": 1},
        {"issuer_ticker": "TSLA", "abs_net": 200000.0, "rank": 2},
        {"issuer_ticker": "MSFT", "abs_net": 100000.0, "rank": 3},
    ])
    assert check_ranking_integrity(df) is True


def test_check_ranking_integrity_fail_out_of_order():
    df = pd.DataFrame([
        {"issuer_ticker": "AAPL", "abs_net": 100000.0, "rank": 1},  # WRONG: rank 1 should be biggest
        {"issuer_ticker": "TSLA", "abs_net": 500000.0, "rank": 2},
    ])
    assert check_ranking_integrity(df) is False


def test_check_ranking_integrity_empty():
    assert check_ranking_integrity(pd.DataFrame()) is True


def test_verify_checksum_pass():
    df = pd.DataFrame([{"issuer_ticker": "AAPL", "net_dollar_value": -150000.0, "abs_net": 150000.0}])
    checksum = _compute_svg_checksum(df)
    assert verify_checksum(df, checksum) is True


def test_verify_checksum_fail():
    df = pd.DataFrame([{"issuer_ticker": "AAPL", "net_dollar_value": -150000.0, "abs_net": 150000.0}])
    assert verify_checksum(df, "bad_checksum_000") is False


def test_verify_checksum_empty_df():
    assert verify_checksum(pd.DataFrame(), "empty") is True


def test_run_all_checks_pass():
    df = pd.DataFrame([{
        "issuer_ticker": "AAPL", "net_dollar_value": -150000.0,
        "abs_net": 150000.0, "transaction_count": 1,
        "total_buys": 0.0, "total_sells": 150000.0,
        "dominant_direction": "SELL", "rank": 1
    }])
    checksum = _compute_svg_checksum(df)
    raw_filings = [{"accessionNo": "A"}]
    db_filings  = [{"accession_number": "A"}]
    raw_txns    = [{"shares": 1000, "price_per_share": 150.0}]

    passed, failed = run_all_checks(raw_filings, db_filings, raw_txns, df, checksum)
    assert passed is True
    assert failed == []


def test_run_all_checks_fail_multiple():
    df = pd.DataFrame([
        {"issuer_ticker": "AAPL", "abs_net": 100.0, "rank": 1},
        {"issuer_ticker": "TSLA", "abs_net": 500.0, "rank": 2},  # out of order
    ])
    bad_checksum = "totally_wrong"
    # DB has more than raw → filing count fails
    raw_filings = [{"accessionNo": "A"}]
    db_filings  = [{"accession_number": "A"}, {"accession_number": "B"}]

    passed, failed = run_all_checks(raw_filings, db_filings, [], df, bad_checksum)
    assert passed is False
    assert "filing_count" in failed
    assert "transaction_checksum" in failed
    assert "ranking_order" in failed


# ── learning_loop tests ───────────────────────────────────────────────────────

def test_learning_loop_only_reads_success_runs():
    """Safety: The SQL WHERE clause must filter status='success' only."""
    db_tool = MagicMock()
    db_tool.query.return_value = []  # no approved runs

    loop = LearningLoop(db_tool, MagicMock())
    result = loop.get_approved_runs(limit=10)

    assert result == []
    # Verify the SQL contains the safety filter
    call_args = db_tool.query.call_args[0][0]
    assert "status = 'success'" in call_args, \
        "CRITICAL: LearningLoop SQL must filter status='success'"


def test_learning_loop_quarantine_marks_run():
    db_tool = MagicMock()
    loop = LearningLoop(db_tool, MagicMock())
    loop.quarantine_run("run-123", "Checksum mismatch", db_tool=db_tool)

    # Check that the UPDATE was called with 'quarantined' status
    call_args = db_tool.query.call_args
    sql = call_args[0][0]
    params = call_args[0][1]
    assert "quarantined" in sql or "quarantined" in str(params), \
        "Quarantine must set status='quarantined'"
    assert "run-123" in str(params)


def test_learning_loop_distill_approved_only():
    db_tool = MagicMock()
    db_tool.query.return_value = [
        ("run-001", "2026-04-14T01:00:00", "2026-04-14T01:05:00", 5, 5, 100, 100, 5)
    ]
    lightrag_tool = MagicMock()
    loop = LearningLoop(db_tool, lightrag_tool)
    result = loop.distill(limit=5)

    assert result["distilled"] == 1
    lightrag_tool.insert_texts.assert_called_once()
    inserted = lightrag_tool.insert_texts.call_args[0][0]
    assert "run-001" in inserted[0]


def test_learning_loop_distill_no_approved_runs():
    db_tool = MagicMock()
    db_tool.query.return_value = []
    loop = LearningLoop(db_tool, MagicMock())
    result = loop.distill()
    assert result["distilled"] == 0
