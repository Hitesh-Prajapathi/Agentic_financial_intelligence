from config.settings import settings
from telemetry.otel_setup import tracer
from tools.apify_tool import ApifyTool
import logging

class SocialScraperAgent:
    def __init__(self, apify_tool: ApifyTool, db_tool, settings=settings):
        self.apify_tool = apify_tool
        self.db_tool = db_tool
        self.settings = settings
        self.run_id = None
        self.logger = logging.getLogger(__name__)

    def run(self, top_trades: list[dict] = None) -> dict:
        with tracer.start_as_current_span("social_scraper_agent.run") as span:
            span.set_attribute("agent.name", "SocialScraperAgent")
            if self.run_id:
                span.set_attribute("agent.run_id", self.run_id)
            
            results = {
                "tickers_scraped": 0,
                "total_tweets": 0,
                "per_ticker": {}
            }
            
            if not top_trades:
                self.logger.warning("No top trades provided for scraping.")
                return results

            tickers_processed = set()
            
            for trade in top_trades:
                ticker = trade.get('issuer_ticker')
                
                if not ticker or ticker in tickers_processed:
                    continue
                    
                tickers_processed.add(ticker)
                
                # Fetch company_name if possible via a direct query
                com = self.db_tool.query("SELECT issuer_name FROM filings WHERE issuer_ticker = ? LIMIT 1", [ticker])
                c_name = com[0][0] if com else ticker

                # Construct search query: "$TICKER OR company_name"
                query = f"${ticker} OR \"{c_name}\""
                
                try:
                    self.logger.info(f"Scraping tweets for {ticker}...")
                    tweets = self.apify_tool.scrape_tweets(query=query, days=self.settings.tweet_lookback_days)
                    
                    if tweets:
                        # Embellish with ticker
                        for t in tweets:
                            t['ticker'] = ticker
                            
                        # Insert deduplicated tweets
                        self.db_tool.insert_tweets(tweets)
                        
                        results["tickers_scraped"] += 1
                        results["total_tweets"] += len(tweets)
                        results["per_ticker"][ticker] = len(tweets)
                except Exception as e:
                    self.logger.error(f"Failed to scrape tweets for {ticker}: {e}")
                    # Allow partial results (don't break entire pipeline because one ticker failed)
            
            return results
