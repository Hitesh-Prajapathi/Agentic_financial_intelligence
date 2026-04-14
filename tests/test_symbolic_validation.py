import pytest
import pandas as pd
from agents.symbolic_validation import validate_and_rank

def test_basic_dollar_value_computation():
    txns = [{
        'issuer_ticker': 'AAPL',
        'shares': 100,
        'price_per_share': 50.00,
        'transaction_type': 'A'
    }]
    ranked, check = validate_and_rank(txns, top_n=1)
    assert ranked.iloc[0]['net_dollar_value'] == 5000.00

def test_signed_values_buy_vs_sell():
    txns = [
        {'issuer_ticker': 'AAPL', 'shares': 100, 'price_per_share': 50.00, 'transaction_type': 'A'},
        {'issuer_ticker': 'TSLA', 'shares': 100, 'price_per_share': 50.00, 'transaction_type': 'D'}
    ]
    ranked, check = validate_and_rank(txns, top_n=5)
    
    aapl_val = ranked[ranked['issuer_ticker'] == 'AAPL'].iloc[0]['net_dollar_value']
    tsla_val = ranked[ranked['issuer_ticker'] == 'TSLA'].iloc[0]['net_dollar_value']
    
    assert aapl_val == 5000.0
    assert tsla_val == -5000.0

def test_deduplication_removes_double_ups():
    txns = [
        {'transaction_date': '2024-04-12', 'issuer_ticker': 'AAPL', 'shares': 100, 'price_per_share': 50.00, 'transaction_type': 'A'},
        {'transaction_date': '2024-04-12', 'issuer_ticker': 'AAPL', 'shares': 100, 'price_per_share': 50.00, 'transaction_type': 'A'}
    ]
    ranked, check = validate_and_rank(txns, top_n=5)
    assert ranked.iloc[0]['net_dollar_value'] == 5000.0
    assert ranked.iloc[0]['transaction_count'] == 1

def test_deduplication_preserves_different_transactions():
    txns = [
        {'transaction_date': '2024-04-12', 'issuer_ticker': 'AAPL', 'shares': 100, 'price_per_share': 50.00, 'transaction_type': 'A'},
        {'transaction_date': '2024-04-13', 'issuer_ticker': 'AAPL', 'shares': 100, 'price_per_share': 50.00, 'transaction_type': 'A'}
    ]
    ranked, check = validate_and_rank(txns, top_n=5)
    assert ranked.iloc[0]['net_dollar_value'] == 10000.0
    assert ranked.iloc[0]['transaction_count'] == 2

def test_net_capital_flow_aggregation():
    txns = [
        {'issuer_ticker': 'AAPL', 'shares': 200, 'price_per_share': 50.00, 'transaction_type': 'A'}, # +10000
        {'issuer_ticker': 'AAPL', 'shares': 60, 'price_per_share': 50.00, 'transaction_type': 'D'} # -3000
    ]
    ranked, check = validate_and_rank(txns, top_n=5)
    assert ranked.iloc[0]['net_dollar_value'] == 7000.0

def test_ranking_by_absolute_value():
    txns = [
        {'issuer_ticker': 'AAPL', 'shares': 200, 'price_per_share': 50.00, 'transaction_type': 'A'}, # +10000
        {'issuer_ticker': 'TSLA', 'shares': 1000, 'price_per_share': 50.00, 'transaction_type': 'D'} # -50000
    ]
    ranked, check = validate_and_rank(txns, top_n=5)
    assert ranked.iloc[0]['issuer_ticker'] == 'TSLA'
    assert ranked.iloc[1]['issuer_ticker'] == 'AAPL'

def test_top_n_parameter():
    txns = [
        {'issuer_ticker': f'TICKER{i}', 'shares': 100, 'price_per_share': float(i), 'transaction_type': 'A'}
        for i in range(1, 10)
    ]
    ranked, check = validate_and_rank(txns, top_n=3)
    assert len(ranked) == 3

def test_checksum_determinism():
    txns = [{'issuer_ticker': 'AAPL', 'shares': 100, 'price_per_share': 50.00, 'transaction_type': 'A'}]
    ranked1, check1 = validate_and_rank(txns, top_n=5)
    ranked2, check2 = validate_and_rank(txns, top_n=5)
    assert check1 == check2

def test_checksum_changes_with_data():
    txns1 = [{'issuer_ticker': 'AAPL', 'shares': 100, 'price_per_share': 50.00, 'transaction_type': 'A'}]
    txns2 = [{'issuer_ticker': 'AAPL', 'shares': 101, 'price_per_share': 50.00, 'transaction_type': 'A'}]
    ranked1, check1 = validate_and_rank(txns1, top_n=5)
    ranked2, check2 = validate_and_rank(txns2, top_n=5)
    assert check1 != check2

def test_empty_transactions_handled_gracefully():
    ranked, check = validate_and_rank([], top_n=5)
    assert len(ranked) == 0
    assert isinstance(check, str)

def test_string_numeric_casting():
    txns = [{
        'issuer_ticker': 'AAPL',
        'shares': "100",
        'price_per_share': "50.00",
        'transaction_type': 'A'
    }]
    ranked, check = validate_and_rank(txns, top_n=1)
    assert ranked.iloc[0]['net_dollar_value'] == 5000.00
