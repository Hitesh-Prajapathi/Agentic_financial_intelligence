from telemetry.otel_setup import tracer
import logging
from datetime import datetime, timezone

class IndexingAgent:
    def __init__(self, lightrag_tool, db_tool, settings=None):
        self.lightrag_tool = lightrag_tool
        self.db_tool = db_tool
        self.settings = settings
        self.run_id = None
        self.logger = logging.getLogger(__name__)

    def build_context_block(self, ticker: str, date_str: str) -> str:
        trades = self.db_tool.query(
            "SELECT rank, insider_name, net_dollar_value, dominant_direction FROM top_trades WHERE issuer_ticker=? AND run_date=?", 
            [ticker, date_str]
        )
        
        sentiment = self.db_tool.query(
            "SELECT total_tweets, bullish_count, bearish_count, neutral_count, avg_confidence, sentiment_index FROM sentiment_summary WHERE ticker=? AND summary_date=?",
            [ticker, date_str]
        )
        
        tweets = self.db_tool.query(
            "SELECT text FROM tweets WHERE ticker=? AND DATE(created_at)=? ORDER BY like_count DESC LIMIT 5",
            [ticker, date_str]
        )
        
        block = f"DATA REPORT FOR TICKER: {ticker} ON DATE: {date_str}\n"
        block += "="*40 + "\n"
        
        if trades:
            block += "INSIDER TRADING ACTIVITY:\n"
            for t in trades:
                block += f"- {t[1]} executing dominant {t[3]} with net dollar flow of ${t[2]}\n"
            block += "\n"
            
        if sentiment:
            s = sentiment[0]
            dominant_sent = "Bullish" if s[5] > 0 else "Bearish" if s[5] < 0 else "Neutral"
            block += f"SOCIAL SENTIMENT SUMMARY:\n"
            block += f"- Total related tweets: {s[0]}\n"
            block += f"- Sentiment split: {s[1]} Bullish, {s[2]} Bearish, {s[3]} Neutral\n"
            block += f"- Overall Net Sentiment Index: {s[5]:.2f} ({dominant_sent})\n"
            block += f"- AI Confidence Score: {s[4]:.2f}\n"
            block += "\n"
            
        if tweets:
            block += "KEY SOCIAL MEDIA POSTS:\n"
            for tw in tweets:
                clean_text = str(tw[0]).replace('\n', ' ')
                block += f"- \"{clean_text}\"\n"
                
        return block

    def run(self, build_all: bool = False) -> dict:
        with tracer.start_as_current_span("indexing_agent.run") as span:
            span.set_attribute("agent.name", "IndexingAgent")
            if self.run_id:
                span.set_attribute("agent.run_id", self.run_id)
            
            today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            
            query = "SELECT DISTINCT issuer_ticker FROM top_trades WHERE run_date=?"
            params = [today_str]
            
            if build_all:
                query = "SELECT DISTINCT issuer_ticker FROM top_trades"
                params = []
                
            tickers_result = self.db_tool.query(query, params)
            tickers = [row[0] for row in tickers_result] if tickers_result else []
            
            blocks = []
            for tck in tickers:
                dates_query = "SELECT DISTINCT run_date FROM top_trades WHERE issuer_ticker=?"
                if not build_all:
                    dates_query += f" AND run_date='{today_str}'"
                    
                dates_result = self.db_tool.query(dates_query, [tck])
                dates = [row[0] for row in dates_result] if dates_result else []
                
                for d in dates:
                    block = self.build_context_block(tck, d)
                    blocks.append(block)
                    
            if blocks:
                self.logger.info(f"Indexing {len(blocks)} context blocks into LightRAG...")
                self.lightrag_tool.insert(blocks)
                
            span.set_attribute("lightrag.indexed_blocks", len(blocks))
            return {
                "indexed_blocks": len(blocks),
                "tickers_updated": len(tickers)
            }
