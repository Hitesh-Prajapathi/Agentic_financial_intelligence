from insider_intel.llm.client import LLMError, LLMResult, extract_json
from insider_intel.sentiment.classify import classify, clean_tweet, parse_batch


class FakeLLM:
    """Returns a canned JSON answer for every batch."""

    def __init__(self, answer, fail=False):
        self.answer, self.fail = answer, fail

    def chat_json(self, messages, **kw):
        if self.fail:
            raise LLMError("down")
        return self.answer, LLMResult("{}", "fake", "m", 1)


def tweets(n):
    return [{"tweet_id": str(i), "ticker": "AAA", "text": f"tweet number {i} about stock"} for i in range(n)]


def test_parse_batch_validates_everything():
    out = parse_batch(
        {
            "1": {"label": "Bullish", "confidence": 3},
            "2": {"label": "moon", "confidence": 0.9},
            "3": {"confidence": 0.5},
            "x": {"label": "neutral"},
            "9": {"label": "neutral", "confidence": 0.5},
            "2b": "junk",
            "4": {"label": "bearish", "confidence": "bad"},
        },
        n=4,
    )
    assert out == {
        1: ("bullish", 1.0)
    }  # invalid label, missing label, bad ids, out-of-range ids, bad confidence -> all rejected


def test_missing_answers_stay_unclassified():
    rows, meta = classify(FakeLLM({"1": {"label": "bullish", "confidence": 0.9}}), tweets(3))  # only 1 of 3 answered
    assert [r["tweet_id"] for r in rows] == ["0"] and rows[0]["sentiment_label"] == "bullish"
    assert meta["batches_failed"] == 0  # no fabricated 'neutral 0.5' rows for the other two


def test_llm_failure_degrades_gracefully():
    rows, meta = classify(FakeLLM({}, fail=True), tweets(1))
    assert rows == [] and meta["batches_failed"] == 1 and meta["errors"]


def test_batching_splits_requests():
    rows, meta = classify(FakeLLM({"1": {"label": "neutral", "confidence": 0.7}}), tweets(5), batch_size=2)
    assert meta["batches"] == 3 and len(rows) == 3  # first item of each batch was answered


def test_summary_uses_tweet_date_not_classification_date(db):
    from datetime import datetime

    db.insert_tweets(
        [
            {"tweet_id": "1", "ticker": "AAA", "text": "bullish tweet one", "created_at": datetime(2026, 11, 1, 10)},
            {"tweet_id": "2", "ticker": "AAA", "text": "bearish tweet two", "created_at": datetime(2026, 11, 2, 10)},
        ]
    )
    db.insert_sentiments(
        [
            {"tweet_id": "1", "ticker": "AAA", "sentiment_label": "bullish", "confidence_score": 0.9},
            {"tweet_id": "2", "ticker": "AAA", "sentiment_label": "bearish", "confidence_score": 0.8},
        ]
    )
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
