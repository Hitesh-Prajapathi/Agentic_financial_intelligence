import os
import json
import duckdb
from config.settings import settings

class DBTool:
    def __init__(self):
        self.db_type = settings.db_type
        self.db_path = settings.db_path
        
        if self.db_type == 'duckdb':
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.conn = duckdb.connect(self.db_path)
            
    def init_db(self):
        with open('db/schema.sql', 'r') as f:
            schema = f.read()
            
        if self.db_type == 'duckdb':
            self.conn.execute(schema)
            
    def query(self, sql: str, params: dict | list = None):
        if params is None:
            params = []
        if isinstance(params, dict):
            # DuckDB named parameter syntax uses $param_name, we assume standard parameterized placeholders
            return self.conn.execute(sql, params).fetchall()
        else:
            return self.conn.execute(sql, params).fetchall()

    def query_df(self, sql: str, params: dict | list = None):
        if params is None:
            params = []
        return self.conn.execute(sql, params).df()

    def insert_filings(self, filings: list[dict]):
        if not filings:
            return
        query = """
        INSERT INTO filings 
        (accession_number, filing_date, issuer_name, issuer_ticker, insider_name, insider_title, form_type, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(accession_number) DO NOTHING
        """
        params = [
            (f.get('accession_number'), f.get('filing_date'), f.get('issuer_name'), f.get('issuer_ticker'),
             f.get('insider_name'), f.get('insider_title'), f.get('form_type', '4'), json.dumps(f.get('raw_json', {})))
            for f in filings
        ]
        self.conn.executemany(query, params)

    def insert_transactions(self, txns: list[dict]):
        if not txns:
            return
        query = """
        INSERT INTO transactions 
        (accession_number, transaction_date, security_title, transaction_type, shares, price_per_share, dollar_value, ownership_nature, post_transaction_shares)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = [
            (t.get('accession_number'), t.get('transaction_date'), t.get('security_title'), t.get('transaction_type'),
             t.get('shares'), t.get('price_per_share'), t.get('dollar_value'), t.get('ownership_nature'), t.get('post_transaction_shares'))
            for t in txns
        ]
        self.conn.executemany(query, params)

    def insert_top_trades(self, trades: list[dict]):
        if not trades:
            return
        query = """
        INSERT INTO top_trades 
        (run_date, rank, issuer_ticker, insider_name, net_dollar_value, transaction_count, dominant_direction, validation_checksum)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = [
            (t.get('run_date'), t.get('rank'), t.get('issuer_ticker'), t.get('insider_name'),
             t.get('net_dollar_value'), t.get('transaction_count'), t.get('dominant_direction'), t.get('validation_checksum'))
            for t in trades
        ]
        self.conn.executemany(query, params)

    def insert_tweets(self, tweets: list[dict]):
        if not tweets:
            return
        query = """
        INSERT INTO tweets 
        (tweet_id, ticker, text, author_username, created_at, retweet_count, like_count, reply_count, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tweet_id) DO NOTHING
        """
        params = [
            (t.get('tweet_id'), t.get('ticker'), t.get('text'), t.get('author_username'),
             t.get('created_at'), t.get('retweet_count'), t.get('like_count'), t.get('reply_count'), json.dumps(t.get('raw_json', {})))
            for t in tweets
        ]
        self.conn.executemany(query, params)

    def insert_sentiments(self, sentiments: list[dict]):
        if not sentiments:
            return
        query = """
        INSERT INTO tweet_sentiments 
        (tweet_id, ticker, sentiment_label, confidence_score, classified_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(tweet_id) DO UPDATE SET 
            sentiment_label = EXCLUDED.sentiment_label,
            confidence_score = EXCLUDED.confidence_score,
            classified_at = EXCLUDED.classified_at
        """
        params = [
            (s.get('tweet_id'), s.get('ticker'), s.get('sentiment_label'), s.get('confidence_score'))
            for s in sentiments
        ]
        self.conn.executemany(query, params)

    def upsert_sentiment_summary(self, ticker: str, date: str, counts: dict):
        query = """
        INSERT INTO sentiment_summary 
        (ticker, summary_date, total_tweets, bullish_count, bearish_count, neutral_count, avg_confidence, sentiment_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, summary_date) DO UPDATE SET
            total_tweets = EXCLUDED.total_tweets,
            bullish_count = EXCLUDED.bullish_count,
            bearish_count = EXCLUDED.bearish_count,
            neutral_count = EXCLUDED.neutral_count,
            avg_confidence = EXCLUDED.avg_confidence,
            sentiment_index = EXCLUDED.sentiment_index
        """
        params = [
            ticker, date, counts.get('total_tweets', 0), counts.get('bullish_count', 0),
            counts.get('bearish_count', 0), counts.get('neutral_count', 0),
            counts.get('avg_confidence', 0.0), counts.get('sentiment_index', 0.0)
        ]
        self.conn.execute(query, params)
        
    def get_top_trades(self, run_date: str):
        query = "SELECT * FROM top_trades WHERE run_date = ? ORDER BY rank"
        return self.conn.execute(query, [run_date]).fetchdf().to_dict('records')

    def get_sentiment_timeseries(self, ticker: str, days: int):
        query = "SELECT * FROM sentiment_summary WHERE ticker = ? ORDER BY summary_date DESC LIMIT ?"
        return self.conn.execute(query, [ticker, days]).fetchdf().to_dict('records')

db_tool = DBTool()
