import json
from datetime import datetime

from agents.sentiment_agent import SentimentAgent, clean_tweet, parse_batch
from config.settings import settings
from tools.apify_tool import ApifyTool, build_search_query, keep_tweet, parse_twitter_date, short_company_name
from tools.llm_tool import LLMError, LLMResult, extract_json


class FakeLLM:
    """Returns a canned JSON answer for every batch."""
    def __init__(self, answer, fail=False):
        self.answer, self.fail = answer, fail

    def chat_json(self, messages, **kw):
        if self.fail:
            raise LLMError("down")
        return self.answer, LLMResult("{}", "fake", "m", 1)


def test_parse_batch_validates_everything():
    out = parse_batch({"1": {"label": "Bullish", "confidence": 3}, "2": {"label": "moon", "confidence": .9},
                       "3": {"confidence": .5}, "x": {"label": "neutral"}, "9": {"label": "neutral", "confidence": .5},
                       "2b": "junk", "4": {"label": "bearish", "confidence": "bad"}}, n=4)
    assert out == {1: ("bullish", 1.0)}  # invalid label, missing label, bad ids, out-of-range ids, bad confidence → all rejected


def test_missing_answers_stay_unclassified(db):
    db.insert_tweets([{"tweet_id": str(i), "ticker": "AAA", "text": f"tweet number {i} about stock", "created_at": datetime(2026, 11, 5)} for i in range(3)])
    agent = SentimentAgent(FakeLLM({"1": {"label": "bullish", "confidence": .9}}), db, settings)  # only 1 of 3 answered
    res = agent.run()
    assert res["tweets_classified"] == 1 and res["tweets_pending"] == 2
    assert db.query("SELECT COUNT(*) FROM tweet_sentiments")[0][0] == 1  # no fabricated 'neutral 0.5' rows


def test_llm_failure_degrades_gracefully(db):
    db.insert_tweets([{"tweet_id": "1", "ticker": "AAA", "text": "some tweet text here", "created_at": datetime(2026, 11, 5)}])
    res = SentimentAgent(FakeLLM({}, fail=True), db, settings).run()
    assert res["tweets_classified"] == 0 and res["batches_failed"] == 1


def test_summary_uses_tweet_date_not_classification_date(db):
    db.insert_tweets([{"tweet_id": "1", "ticker": "AAA", "text": "bullish tweet one", "created_at": datetime(2026, 11, 1, 10)},
                      {"tweet_id": "2", "ticker": "AAA", "text": "bearish tweet two", "created_at": datetime(2026, 11, 2, 10)}])
    db.insert_sentiments([{"tweet_id": "1", "ticker": "AAA", "sentiment_label": "bullish", "confidence_score": .9},
                          {"tweet_id": "2", "ticker": "AAA", "sentiment_label": "bearish", "confidence_score": .8}])
    db.refresh_sentiment_summary()
    rows = db.query("SELECT summary_date, sentiment_index FROM sentiment_summary ORDER BY 1")
    assert [(str(r[0]), r[1]) for r in rows] == [("2026-11-01", 1.0), ("2026-11-02", -1.0)]


def test_extract_json_variants():
    assert extract_json('{"a": 1}') == {"a": 1}
    assert extract_json('```json\n{"a": 1}\n```') == {"a": 1}
    assert extract_json('Sure! Here you go: {"a": {"b": 2}} hope it helps') == {"a": {"b": 2}}
    assert extract_json('<think>hmm {x}</think>{"ok": true}') == {"ok": True}


def test_clean_tweet():
    assert clean_tweet("Buy $X now https://t.co/abc\n\n  fast") == "Buy $X now fast"
    assert len(clean_tweet("a" * 1000)) == 280


def test_twitter_date_parsing():
    assert parse_twitter_date("Thu Nov 05 16:37:21 +0000 2026") == datetime(2026, 11, 5, 16, 37, 21)
    assert parse_twitter_date("2026-11-05T16:37:21Z") == datetime(2026, 11, 5, 16, 37, 21)
    assert parse_twitter_date("garbage") is None and parse_twitter_date(None) is None


def test_search_query_construction():
    assert short_company_name("Dutch Bros Inc.") == "Dutch Bros"
    assert short_company_name("CrowdStrike Holdings, Inc.") == "CrowdStrike"
    q = build_search_query("BROS", "Dutch Bros Inc.", 3)
    assert '$BROS OR "Dutch Bros"' in q and "-filter:retweets" in q and "since:" in q
    assert build_search_query("ON", "ON Semiconductor Corp", 3).startswith("$ON lang:en")  # ambiguous ticker → cashtag only


def test_tweet_quality_filter():
    base = dict(is_retweet=False, lang="en", text="$AAA looks strong into earnings", created_at=datetime(2026, 11, 5))
    assert keep_tweet(base)
    assert not keep_tweet({**base, "is_retweet": True})
    assert not keep_tweet({**base, "lang": "fr"})
    assert not keep_tweet({**base, "text": "$AAA $BBB $CCC $DDD to the moon"})   # cashtag spam
    assert not keep_tweet({**base, "text": "ok"})
    assert not keep_tweet({**base, "created_at": None})


def test_normalise_skips_no_results_items():
    assert ApifyTool._normalise({"noResults": True}) is None
    t = ApifyTool._normalise({"id": 7, "fullText": "hi there friend", "createdAt": "Thu Nov 05 16:37:21 +0000 2026", "author": {"userName": "u", "followers": 5}})
    assert t["tweet_id"] == "7" and t["author_followers"] == 5 and t["created_at"].year == 2026
