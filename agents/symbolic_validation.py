"""
SYMBOLIC VALIDATION GATE (from Doc 1)
=====================================
This module is DETERMINISTIC. It uses ZERO LLM calls.
All math is done via Pandas. This is the single most important
defense against arithmetic hallucination in the entire system.
"""

import pandas as pd
import hashlib
import json
from typing import Tuple

def validate_and_rank(
    transactions: list[dict],
    top_n: int = 5
) -> Tuple[pd.DataFrame, str]:
    """
    Takes raw transaction records from the SEC Data Agent,
    performs deterministic mathematical validation, and returns
    the top N trades by absolute net dollar value.
    
    Returns:
        (ranked_df, checksum) where checksum is a SHA-256 hash
        of the output for integrity verification in the 
        Governance Telemetry Schema.
    """
    if not transactions:
        # Handle empty transactions gracefully
        checksum = hashlib.sha256(b"empty").hexdigest()
        return pd.DataFrame(), checksum

    # 1. Load into DataFrame
    df = pd.DataFrame(transactions)
    
    # 2. Compute dollar value deterministically
    df['shares'] = pd.to_numeric(df['shares'], errors='coerce').fillna(0)
    df['price_per_share'] = pd.to_numeric(df['price_per_share'], errors='coerce').fillna(0)
    
    df['dollar_value'] = (df['shares'] * df['price_per_share']).round(2)
    
    # 3. Sign the values based on transaction type
    def get_signed_value(row):
        ttype = str(row.get('transaction_type', '')).upper()
        if ttype in ('A', 'P'):
            return row['dollar_value']
        else:
            return -row['dollar_value']

    df['signed_value'] = df.apply(get_signed_value, axis=1)
    
    # 4. Deduplicate EDGAR "double-ups"
    dedup_cols = ['transaction_date', 'issuer_ticker', 'shares', 
                  'price_per_share', 'transaction_type']
    
    # Ensure columns exist to avoid KeyError if the df is partly unpopulated
    for col in dedup_cols:
        if col not in df.columns:
            df[col] = None

    df = df.drop_duplicates(subset=dedup_cols, keep='first')
    
    # 5. Aggregate net capital flow per ticker
    if 'insider_name' not in df.columns:
        df['insider_name'] = 'Unknown'
        
    df['insider_name'] = df['insider_name'].fillna('Unknown').astype(str)
    
    ticker_summary = df.groupby('issuer_ticker').agg(
        net_dollar_value=('signed_value', 'sum'),
        transaction_count=('signed_value', 'count'),
        total_buys=('signed_value', lambda x: (x > 0).sum()),
        total_sells=('signed_value', lambda x: (x < 0).sum()),
        insider_name=('insider_name', lambda x: ', '.join(x.unique()))
    ).reset_index()
    
    # Check if empty after dedup
    if ticker_summary.empty:
        checksum = hashlib.sha256(b"empty").hexdigest()
        return pd.DataFrame(), checksum

    # 6. Rank by absolute net dollar value
    ticker_summary['abs_net'] = ticker_summary['net_dollar_value'].abs()
    
    # nlargest requires at least one row, which we have if not empty
    ranked = ticker_summary.nlargest(top_n, 'abs_net').copy()
    
    # If tie, deterministic ordering
    ranked = ranked.sort_values(by=['abs_net', 'issuer_ticker'], ascending=[False, True])
    
    ranked['rank'] = range(1, len(ranked) + 1)
    
    def get_direction(x):
        return 'BUY' if x > 0 else 'SELL'
        
    ranked['dominant_direction'] = ranked['net_dollar_value'].apply(get_direction)
    
    # 7. Generate integrity checksum
    # Drop index so output list doesn't get messed up if ranked is out of index order
    ranked = ranked.reset_index(drop=True)
    checksum_input = ranked[['rank', 'issuer_ticker', 'net_dollar_value']].to_json()
    checksum = hashlib.sha256(checksum_input.encode()).hexdigest()
    
    return ranked, checksum
