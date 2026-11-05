from datetime import datetime

from agents.social_scraper_agent import SocialScraperAgent
from config.settings import settings
from skills.learning_loop import LearningLoop
from tools.apify_tool import BudgetExceeded


class FakeApify:
    def __init__(self, tweets=None, over_budget=False):
        self.tweets, self.over_budget, self.calls = tweets or [], over_budget, 0

    def check_budget(self, n):
        if self.over_budget:
            raise BudgetExceeded("cap")

    def scrape(self, query, n):
        self.calls += 1
        return [dict(t) for t in self.tweets]


def tweet(i):
    return dict(tweet_id=str(i), text=f"$AAA tweet number {i} looks interesting", created_at=datetime(2026, 11, 5), is_retweet=False, lang="en")


TRADES = [{"issuer_ticker": "AAA", "issuer_name": "Alpha Inc."}]


def test_empty_scrapes_are_not_paid_for_repeatedly(db):
    ap = FakeApify([])
    agent = SocialScraperAgent(ap, db, settings)
    first = agent.run(TRADES)
    assert ap.calls == 1 and first["tickers_empty"] == ["AAA"]
    agent.run(TRADES)
    assert ap.calls == 1   # cooldown: no second billed run


def test_successful_scrape_is_cached_and_stored(db):
    ap = FakeApify([tweet(1), tweet(2)])
    agent = SocialScraperAgent(ap, db, settings)
    assert agent.run(TRADES)["total_tweets"] == 2
    again = agent.run(TRADES)
    assert ap.calls == 1 and again["tickers_cached"] == 1


def test_budget_block_prevents_scraping(db):
    ap = FakeApify([tweet(1)], over_budget=True)
    res = SocialScraperAgent(ap, db, settings).run(TRADES)
    assert res["budget_blocked"] and ap.calls == 0


class FakeSentiment:
    def __init__(self, label):
        self.label = label

    def classify(self, tweets, models=None):
        return [{"tweet_id": t["tweet_id"], "ticker": t["ticker"], "sentiment_label": self.label, "confidence_score": 0.9,
                 "model": "reviewer"} for t in tweets], {}


class FakeLLM:
    def chain(self):
        return [("groq", "a"), ("groq", "b")]


def _seed(db):
    db.insert_tweets([dict(tweet_id="1", ticker="AAA", text="x" * 60, created_at=datetime(2026, 11, 5)),
                      dict(tweet_id="2", ticker="AAA", text="y" * 60, created_at=datetime(2026, 11, 5))])
    db.insert_sentiments([dict(tweet_id="1", ticker="AAA", sentiment_label="bullish", confidence_score=0.4),
                          dict(tweet_id="2", ticker="AAA", sentiment_label="bearish", confidence_score=0.3)])


def test_learning_loop_confirms_and_contests(db):
    _seed(db)
    loop = LearningLoop(db, FakeLLM(), FakeSentiment("bullish"), settings)   # reviewer agrees with #1, disagrees with #2
    res = loop.review_low_confidence()
    assert res == {"reviewed": 2, "confirmed": 1, "changed": 1}
    rows = dict((r[0], (r[1], r[2])) for r in db.query("SELECT tweet_id, sentiment_label, reviewed FROM tweet_sentiments"))
    assert rows["1"] == ("bullish", True) and rows["2"] == ("neutral", True)   # contested → neutral, never acted on
    assert loop.review_low_confidence()["reviewed"] == 0                         # reviewed items are not re-reviewed


def test_learning_loop_only_trusts_agreed_labels_as_examples(db):
    _seed(db)
    loop = LearningLoop(db, FakeLLM(), FakeSentiment("bullish"), settings)
    loop.review_low_confidence()
    assert loop.distill_examples()["examples_added"] == 1   # only the confirmed bullish one qualifies (conf raised to 0.9)
    assert db.query("SELECT label FROM learned_examples") == [("bullish",)]


def test_learning_loop_only_reads_approved_runs(db):
    now = datetime(2026, 11, 5)
    for rid, status in [("a", "success"), ("b", "partial"), ("c", "quarantined"), ("d", "failed"), ("e", "running")]:
        db.start_run(rid, now, "manual")
        db.update_run(rid, status=status, completed_at=now)
    assert {r["run_id"] for r in LearningLoop(db, FakeLLM(), None, settings).approved_runs()} == {"a", "b"}
