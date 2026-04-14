import sys
import os
import json
from datetime import datetime, timezone, timedelta
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tools.db_tool import db_tool
from agents.symbolic_validation import validate_and_rank

def seed_db():
    print("Initializing DB...")
    db_tool.init_db()
    
    # Clear old demo data to prevent duplicates
    db_tool.query("DELETE FROM tweet_sentiments")
    db_tool.query("DELETE FROM tweets")
    db_tool.query("DELETE FROM top_trades")
    db_tool.query("DELETE FROM transactions")
    db_tool.query("DELETE FROM filings")
    print("  Cleared old data.")
    
    # 1. Create simulated filings
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    
    filings = [
        {
            "accession_number": "000123-26-001",
            "filing_date": yesterday.isoformat(),
            "issuer_name": "Tesla, Inc.",
            "issuer_ticker": "TSLA",
            "insider_name": "Elon Musk",
            "insider_title": "CEO",
            "form_type": "4",
            "raw_json": json.dumps({"demo": True})
        },
        {
            "accession_number": "000124-26-002",
            "filing_date": yesterday.isoformat(),
            "issuer_name": "Apple Inc.",
            "issuer_ticker": "AAPL",
            "insider_name": "Tim Cook",
            "insider_title": "CEO",
            "form_type": "4",
            "raw_json": json.dumps({"demo": True})
        },
        {
            "accession_number": "000125-26-003",
            "filing_date": yesterday.isoformat(),
            "issuer_name": "Microsoft Corporation",
            "issuer_ticker": "MSFT",
            "insider_name": "Satya Nadella",
            "insider_title": "CEO",
            "form_type": "4",
            "raw_json": json.dumps({"demo": True})
        }
    ]
    db_tool.insert_filings(filings)

    # 2. Simulated trades
    transactions = [
        {
            "accession_number": "000123-26-001",
            "transaction_date": yesterday.strftime('%Y-%m-%d'),
            "security_title": "Common Stock",
            "transaction_type": "P", # Purchase
            "shares": 10000,
            "price_per_share": 180.50,
            "ownership_nature": "D",
            "post_transaction_shares": 500000
        },
        {
            "accession_number": "000124-26-002",
            "transaction_date": yesterday.strftime('%Y-%m-%d'),
            "security_title": "Common Stock",
            "transaction_type": "S", # Sell
            "shares": 50000,
            "price_per_share": 175.25,
            "ownership_nature": "D",
            "post_transaction_shares": 1000000
        },
        {
            "accession_number": "000125-26-003",
            "transaction_date": yesterday.strftime('%Y-%m-%d'),
            "security_title": "Common Stock",
            "transaction_type": "P", # Purchase
            "shares": 5000,
            "price_per_share": 420.00,
            "ownership_nature": "D",
            "post_transaction_shares": 80000
        }
    ]
    db_tool.insert_transactions(transactions)

    # 3. Use actual validation script to calculate rankings
    # Reconstruct data like RankingAgent would
    df_txns = db_tool.query_df("""
        SELECT t.*, f.filing_date, f.issuer_ticker, f.insider_name 
        FROM transactions t JOIN filings f ON t.accession_number = f.accession_number 
    """)
    ranked_df, checksum = validate_and_rank(df_txns.to_dict('records'))
    
    top_trades_payload = []
    for t in ranked_df.to_dict('records'):
        top_trades_payload.append({
            'run_date': now.strftime('%Y-%m-%d'),
            'rank': t['rank'],
            'issuer_ticker': t['issuer_ticker'],
            'insider_name': t.get('insider_name', 'Unknown'),
            'net_dollar_value': t['net_dollar_value'],
            'transaction_count': t['transaction_count'],
            'dominant_direction': t['dominant_direction'],
            'validation_checksum': checksum
        })
    db_tool.insert_top_trades(top_trades_payload)
    
    # 4. Insert some Tweets and Sentiments for these tickers
    tweets = [
        {
            "tweet_id": "T1",
            "ticker": "TSLA",
            "text": "Tesla's AI day was huge, the stock is heavily undervalued right now. Buying more!",
            "author_username": "bull_trader",
            "created_at": yesterday.isoformat(),
            "retweet_count": 50,
            "like_count": 200,
            "reply_count": 10,
            "raw_json": json.dumps({"demo": True})
        },
        {
            "tweet_id": "T2",
            "ticker": "AAPL",
            "text": "Apple sales look weak for this quarter. I think we will see a massive drop soon 📉",
            "author_username": "bear_investor",
            "created_at": yesterday.isoformat(),
            "retweet_count": 20,
            "like_count": 50,
            "reply_count": 5,
            "raw_json": json.dumps({"demo": True})
        },
        {
            "tweet_id": "T3",
            "ticker": "MSFT",
            "text": "Microsoft is just releasing updates as usual. Looks like steady growth.",
            "author_username": "neutral_nancy",
            "created_at": yesterday.isoformat(),
            "retweet_count": 5,
            "like_count": 15,
            "reply_count": 2,
            "raw_json": json.dumps({"demo": True})
        }
    ]
    db_tool.insert_tweets(tweets)

    sentiments = [
        {"tweet_id": "T1", "ticker": "TSLA", "sentiment_label": "bullish", "confidence_score": 0.95},
        {"tweet_id": "T2", "ticker": "AAPL", "sentiment_label": "bearish", "confidence_score": 0.88},
        {"tweet_id": "T3", "ticker": "MSFT", "sentiment_label": "neutral", "confidence_score": 0.90}
    ]
    for s in sentiments:
        db_tool.query(
            "INSERT INTO tweet_sentiments (tweet_id, ticker, sentiment_label, confidence_score, classified_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP) ON CONFLICT(tweet_id) DO NOTHING",
            [s['tweet_id'], s['ticker'], s['sentiment_label'], s['confidence_score']]
        )
    
    print("✅ Seeded DB with Demo Data. Refresh your Streamlit App!")

if __name__ == "__main__":
    seed_db()
