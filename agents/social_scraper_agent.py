"""Stage 3: scrape recent tweets for the ranked tickers (parallel, cached, budget-guarded)."""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from telemetry.otel_setup import tracer
from tools.apify_tool import ApifyTool, BudgetExceeded, build_search_query, keep_tweet

logger = logging.getLogger(__name__)


class SocialScraperAgent:
    def __init__(self, apify_tool: ApifyTool, db_tool, settings):
        self.apify, self.db, self.settings = apify_tool, db_tool, settings

    def _recently_scraped(self, ticker: str) -> bool:
        """Cached if we got tweets recently, or got *nothing* very recently (Apify bills empty runs too)."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        return bool(self.db.query(
            """SELECT 1 FROM scrape_log WHERE ticker = ?
               AND ((tweets > 0 AND scraped_at >= ?) OR (tweets = 0 AND scraped_at >= ?)) LIMIT 1""",
            [ticker, now - timedelta(hours=self.settings.tweet_cache_hours),
             now - timedelta(hours=self.settings.empty_scrape_cooldown_hours)]))

    def _scrape_one(self, ticker: str, company: str | None) -> list[dict]:
        query = build_search_query(ticker, company, self.settings.tweet_lookback_days)
        raw = self.apify.scrape(query, self.settings.tweets_per_ticker)
        kept = [t for t in raw if keep_tweet(t)]
        for t in kept:
            t["ticker"] = ticker
        return kept

    def run(self, top_trades: list[dict] | None) -> dict:
        res = {"tickers_scraped": 0, "tickers_cached": 0, "tickers_failed": 0, "tickers_empty": [], "total_tweets": 0,
               "per_ticker": {}, "errors": [], "budget_blocked": False}
        with tracer.start_as_current_span("social_scraper_agent.run") as span:
            targets, seen = [], set()
            for t in top_trades or []:
                tk = t.get("issuer_ticker")
                if tk and tk not in seen:
                    seen.add(tk)
                    targets.append((tk, t.get("issuer_name")))
            if not targets:
                return res

            todo = []
            for tk, name in targets:
                if self._recently_scraped(tk):
                    res["tickers_cached"] += 1
                else:
                    todo.append((tk, name))
            if not todo:
                return res

            try:
                self.apify.check_budget(len(todo) * self.settings.tweets_per_ticker)
            except BudgetExceeded as e:
                res["budget_blocked"] = True
                res["errors"].append(str(e))
                logger.warning("%s", e)
                return res

            with ThreadPoolExecutor(max_workers=min(5, len(todo))) as pool:  # Apify allows 5 concurrent runs
                futures = {pool.submit(self._scrape_one, tk, name): tk for tk, name in todo}
                for fut in as_completed(futures):
                    tk = futures[fut]
                    try:
                        tweets = fut.result()
                        new = self.db.insert_tweets(tweets) if tweets else 0
                        self.db.execute("INSERT INTO scrape_log VALUES (?, ?, ?, ?)",
                                        [tk, datetime.now(timezone.utc).replace(tzinfo=None), len(tweets),
                                         len(tweets) * self.settings.apify_price_per_tweet_usd])
                        res["tickers_scraped"] += 1
                        res["total_tweets"] += new
                        res["per_ticker"][tk] = len(tweets)
                        if not tweets:  # retried after the (shorter) empty-result cooldown
                            res["tickers_empty"].append(tk)
                    except Exception as e:  # noqa: BLE001  one ticker failing must not sink the run
                        res["tickers_failed"] += 1
                        res["errors"].append(f"{tk}: {str(e)[:100]}")
                        logger.warning("Scrape failed for %s: %s", tk, e)
            if res["tickers_empty"]:
                res["errors"].append(f"No tweets returned for {', '.join(res['tickers_empty'])} (scraper blocked/rate-limited, "
                                     f"free-plan cap, or genuinely no matching posts); retrying after {self.settings.empty_scrape_cooldown_hours:g}h.")
            if res["tickers_empty"] and len(res["tickers_empty"]) == res["tickers_scraped"]:
                res["tickers_failed"] = res["tickers_scraped"]  # every attempted ticker came back empty → stage shows 'partial'
            span.set_attribute("tweets.new", res["total_tweets"])
            return res
