"""Apify tweet scraper (apidojo/tweet-scraper) with budget guard and defensive parsing."""
from __future__ import annotations

import logging
import re
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import httpx
from apify_client import ApifyClient

from config.settings import settings as default_settings
from telemetry.otel_setup import tracer

logger = logging.getLogger(__name__)
ACTOR_ID = "apidojo/tweet-scraper"  # default; override with APIFY_ACTOR_ID
_CASHTAG_RE = re.compile(r"\$[A-Za-z]{1,6}\b")
_SUFFIX_RE = re.compile(r"[,.]?\s+(inc|corp|corporation|co|company|ltd|limited|plc|llc|holdings?|group|n\.?v|s\.?a|class [a-c])\b\.?",
                        re.IGNORECASE)


class ApifyScraperError(Exception):
    pass


class BudgetExceeded(ApifyScraperError):
    pass


def parse_twitter_date(value) -> datetime | None:
    """'Thu Nov 05 16:37:21 +0000 2026' (or ISO) → naive UTC datetime."""
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(str(value))
    except (TypeError, ValueError):
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def short_company_name(name: str | None) -> str:
    return _SUFFIX_RE.sub("", name or "").strip(" ,.")


def build_search_query(ticker: str, company: str | None, days: int) -> str:
    """Cashtag + company name. Very short tickers (A, ON, IT) are ambiguous words, so cashtag only."""
    name = short_company_name(company)
    terms = f"${ticker}"
    if len(ticker) > 2 and name and name.upper() != ticker:
        terms = f'(${ticker} OR "{name}")'
    now = datetime.now(timezone.utc)
    since, until = (now - timedelta(days=days)).strftime("%Y-%m-%d"), (now + timedelta(days=1)).strftime("%Y-%m-%d")
    return f"{terms} lang:en -filter:retweets since:{since} until:{until}"


class ApifyTool:
    def __init__(self, settings=default_settings):
        self.settings = settings
        self.client = ApifyClient(settings.apify_api_token)

    # ── budget ──────────────────────────────────────────────────────────────
    def usage(self) -> dict:
        """Month-to-date spend and plan limit (USD)."""
        try:
            r = httpx.get("https://api.apify.com/v2/users/me/limits",
                          headers={"Authorization": f"Bearer {self.settings.apify_api_token}"}, timeout=15)
            r.raise_for_status()
            d = r.json()["data"]
            return {"ok": True, "used_usd": float(d["current"]["monthlyUsageUsd"]),
                    "plan_limit_usd": float(d["limits"]["maxMonthlyUsageUsd"])}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": str(e)[:100]}

    def check_budget(self, planned_tweets: int) -> None:
        u = self.usage()
        if not u.get("ok"):
            logger.warning("Could not read Apify usage (%s); proceeding on local cap only", u.get("error"))
            return
        projected = u["used_usd"] + planned_tweets * self.settings.apify_price_per_tweet_usd
        cap = min(self.settings.apify_monthly_budget_usd, u["plan_limit_usd"])
        if projected > cap:
            raise BudgetExceeded(f"Apify budget: ${u['used_usd']:.2f} used + ~${planned_tweets * self.settings.apify_price_per_tweet_usd:.2f} "
                                 f"planned exceeds cap ${cap:.2f}")

    # ── scraping ────────────────────────────────────────────────────────────
    def scrape(self, query: str, max_tweets: int) -> list[dict]:
        with tracer.start_as_current_span("apify.scrape") as span:
            span.set_attribute("apify.max_tweets", max_tweets)
            run_input = {"searchTerms": [query], "maxItems": max_tweets, "sort": "Latest"}
            try:
                run = self.client.actor(self.settings.apify_actor_id).call(
                    run_input=run_input,
                    max_items=max_tweets,
                    # hard per-run spend cap: even a misbehaving actor cannot exceed this
                    max_total_charge_usd=Decimal(str(round(max_tweets * self.settings.apify_price_per_tweet_usd * 1.5 + 0.005, 4))),
                    run_timeout=timedelta(seconds=self.settings.apify_run_timeout_s),
                    wait_duration=timedelta(seconds=self.settings.apify_run_timeout_s + 15),
                    logger=None,
                )
            except Exception as e:  # noqa: BLE001
                raise ApifyScraperError(f"Scraper failed: {e}") from e
            dataset_id = getattr(run, "default_dataset_id", None) or (run.get("defaultDatasetId") if isinstance(run, dict) else None)
            if not dataset_id:
                return []
            tweets, no_results = [], 0
            for item in self.client.dataset(dataset_id).iterate_items():
                t = self._normalise(item)
                if t:
                    tweets.append(t)
                elif item.get("noResults"):
                    no_results += 1
            if no_results and not tweets:
                logger.warning("Apify returned only 'noResults' items for query %r", query[:60])
            span.set_attribute("apify.tweets", len(tweets))
            span.set_attribute("apify.no_results_items", no_results)
            return tweets

    @staticmethod
    def _normalise(item: dict) -> dict | None:
        if item.get("noResults") or not item.get("id"):
            return None
        text = (item.get("fullText") or item.get("text") or "").strip()
        author = item.get("author") or {}
        return {
            "tweet_id": str(item["id"]),
            "text": text,
            "author_username": author.get("userName"),
            "author_followers": int(author.get("followers") or 0),
            "created_at": parse_twitter_date(item.get("createdAt")),
            "retweet_count": int(item.get("retweetCount") or 0),
            "like_count": int(item.get("likeCount") or 0),
            "reply_count": int(item.get("replyCount") or 0),
            "is_retweet": bool(item.get("isRetweet")),
            "lang": item.get("lang"),
        }


def keep_tweet(t: dict, max_cashtags: int = 2, min_len: int = 15) -> bool:
    """Quality filter: drop retweets, non-English, tiny posts and cashtag spam."""
    if t.get("is_retweet") or t.get("lang") not in (None, "en"):
        return False
    text = t.get("text") or ""
    if len(text) < min_len or len(_CASHTAG_RE.findall(text)) > max_cashtags or not t.get("created_at"):
        return False
    return True
