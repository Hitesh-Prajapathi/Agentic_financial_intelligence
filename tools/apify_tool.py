from apify_client import ApifyClient
from config.settings import settings
from telemetry.otel_setup import tracer
from datetime import datetime, timedelta, timezone
import logging

class ApifyScraperError(Exception):
    pass

class ApifyTool:
    def __init__(self):
        self.client = ApifyClient(settings.apify_api_token)
        # Using a reliable Twitter scraper actor ID
        self.actor_id = "apidojo/tweet-scraper"
        self.logger = logging.getLogger(__name__)

    def scrape_tweets(self, query: str, days: int = 7, max_tweets: int = 100) -> list[dict]:
        with tracer.start_as_current_span("apify.scrape_tweets") as span:
            span.set_attribute("apify.actor_id", self.actor_id)
            span.set_attribute("apify.query", query)
            span.set_attribute("apify.max_tweets", max_tweets)
            span.set_attribute("apify.days_lookback", days)
            
            now = datetime.now(timezone.utc)
            start_date = (now - timedelta(days=days)).isoformat()
            end_date = now.isoformat()
            
            run_input = {
                "searchTerms": [query],
                "maxItems": max_tweets,
                "tweetsDesired": max_tweets,
                "since": start_date,
                "until": end_date,
                "language": "en"
            }
            
            start_ms = datetime.now().timestamp() * 1000
            
            try:
                run = self.client.actor(self.actor_id).call(run_input=run_input, wait_secs=120)
                
                duration_ms = (datetime.now().timestamp() * 1000) - start_ms
                span.set_attribute("apify.run_duration_ms", duration_ms)
                
                if not run:
                    self.logger.warning("Actor run timed out (no run object returned).")
                    return []
                    
                status = run.get('status')
                if status not in ['SUCCEEDED', 'READY', 'RUNNING']:
                    self.logger.warning(f"Actor run finished with status: {status}")
                
                dataset_id = run.get('defaultDatasetId')
                if not dataset_id:
                    return []
                    
                items_client = self.client.dataset(dataset_id).iterate_items()
                tweets = []
                
                for item in items_client:
                    tweets.append({
                        "tweet_id": str(item.get("id", item.get("tweet_id", ""))),
                        "text": item.get("full_text", item.get("text", "")),
                        "author_username": item.get("author", {}).get("userName", item.get("user", {}).get("screen_name", "")),
                        "created_at": item.get("createdAt", item.get("created_at")),
                        "retweet_count": int(item.get("retweetCount", item.get("retweet_count", 0))),
                        "like_count": int(item.get("likeCount", item.get("favorite_count", 0))),
                        "reply_count": int(item.get("replyCount", item.get("reply_count", 0))),
                        "raw_json": item
                    })
                
                span.set_attribute("apify.tweets_returned", len(tweets))
                return tweets

            except Exception as e:
                self.logger.error(f"Apify Scraper Error: {e}")
                raise ApifyScraperError(f"Scraper failed: {e}")
