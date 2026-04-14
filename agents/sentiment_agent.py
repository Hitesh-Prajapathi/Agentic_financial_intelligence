import json
import logging
from datetime import datetime, timezone
from config.settings import settings
from telemetry.otel_setup import tracer

class SentimentAgent:
    def __init__(self, llm_tool, db_tool, settings=settings):
        self.llm_tool = llm_tool
        self.db_tool = db_tool
        self.settings = settings
        self.run_id = None
        self.logger = logging.getLogger(__name__)

    def run(self, ticker: str = None) -> dict:
        with tracer.start_as_current_span("sentiment_agent.run") as span:
            span.set_attribute("agent.name", "SentimentAgent")
            if self.run_id:
                span.set_attribute("agent.run_id", self.run_id)
            if ticker:
                span.set_attribute("filter.ticker", ticker)

            # Load prompt
            with open("config/prompts/sentiment_agent.txt", "r") as f:
                system_prompt = f.read()

            # Query unclassified tweets
            query = """
            SELECT tweet_id, ticker, text, DATE(created_at) as created_date
            FROM tweets 
            WHERE tweet_id NOT IN (SELECT tweet_id FROM tweet_sentiments)
            """
            params = []
            if ticker:
                query += " AND ticker = ?"
                params.append(ticker)

            unclassified = self.db_tool.query(query, params)
            tweets_dist = [{'tweet_id': row[0], 'ticker': row[1], 'text': row[2], 'date': str(row[3])} for row in unclassified]
            
            affected_dates_tickers = set()
            sentiments_to_insert = []
            
            # Batch by 10
            batch_size = 10
            for i in range(0, len(tweets_dist), batch_size):
                batch = tweets_dist[i:i + batch_size]
                
                batch_input = json.dumps([{"id": t['tweet_id'], "text": t['text']} for t in batch])
                
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": batch_input}
                ]
                
                try:
                    response_json = self.llm_tool.call_llm_structured(
                        messages=messages, 
                        model=self.settings.openrouter_model_classify,
                        response_format={"type": "json_object"}
                    )
                    
                    for t in batch:
                        tid = str(t['tweet_id'])
                        
                        # Match IDs as JSON keys
                        if tid in response_json:
                            label = response_json[tid].get('label', 'neutral')
                            conf = response_json[tid].get('confidence', 0.5)
                        else:
                            # Fallback if structure mismatches slightly
                            label, conf = 'neutral', 0.5
                            
                        sentiments_to_insert.append({
                            'tweet_id': tid,
                            'ticker': t['ticker'],
                            'sentiment_label': label,
                            'confidence_score': float(conf)
                        })
                        affected_dates_tickers.add((t['ticker'], t['date']))
                except Exception as e:
                    self.logger.error(f"Failed to classify batch. Error: {e}")

            if sentiments_to_insert:
                self.db_tool.insert_sentiments(sentiments_to_insert)

            # Compute aggregated summaries
            today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            
            for tck, str_date in affected_dates_tickers:
                agg_query = """
                SELECT sentiment_label, COUNT(*) as cnt, AVG(confidence_score) as avg_conf
                FROM tweet_sentiments 
                WHERE ticker=? AND DATE(classified_at)=? 
                GROUP BY sentiment_label
                """
                rows = self.db_tool.query(agg_query, [tck, today_str])
                
                bullish = bearish = neutral = 0
                avg_conf_sum = 0
                total_conf_count = 0
                
                for row in rows:
                    lbl, cnt, avg_conf = row[0], int(row[1]), float(row[2])
                    if lbl == 'bullish': bullish = cnt
                    elif lbl == 'bearish': bearish = cnt
                    else: neutral = cnt
                        
                    avg_conf_sum += avg_conf * cnt
                    total_conf_count += cnt
                    
                total = bullish + bearish + neutral
                if total > 0:
                    sentiment_index = (bullish - bearish) / total
                    avg_confidence = avg_conf_sum / total_conf_count
                    
                    summary_counts = {
                        'total_tweets': total,
                        'bullish_count': bullish,
                        'bearish_count': bearish,
                        'neutral_count': neutral,
                        'avg_confidence': avg_confidence,
                        'sentiment_index': sentiment_index
                    }
                    self.db_tool.upsert_sentiment_summary(tck, today_str, summary_counts)

            unique_tickers_affected = len({t[0] for t in affected_dates_tickers})

            return {
                "tweets_classified": len(sentiments_to_insert),
                "tickers_updated": unique_tickers_affected
            }
