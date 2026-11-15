from datetime import datetime
from types import SimpleNamespace

from insider_intel.chat.retrieval import Retriever, citation_tags
from insider_intel.chat.service import ChatService, classify_intent
from insider_intel.config import settings
from insider_intel.llm.client import LLMError


class FakeLLM:
    last_label = "fake:model"

    def __init__(self, text="ok", fail=False):
        self.text, self.fail = text, fail

    def stream(self, messages, **kw):
        if self.fail:
            raise LLMError("all down")
        yield from (w + " " for w in self.text.split(" "))

    def chain(self):
        return []


def build(db, llm=None):
    """Chat-side test harness. Pipeline tests (ingest, gate, quarantine, rerun) live in tests/pipeline/."""
    retriever = Retriever(db, settings)
    return SimpleNamespace(chat=ChatService(db, llm or FakeLLM(), retriever, settings))


def test_intent_routing_uses_word_boundaries():
    c = classify_intent
    assert c("draw me a chart of the top trades") == "chart"
    assert c("Plot sentiment for $AAPL") == "chart"
    assert c("I want to withdraw my money, any trades?") == "chat"  # 'withdraw' contains 'draw'
    assert c("when was the data last updated?") == "status"


def test_no_data_means_low_confidence_and_no_llm_call(db):
    llm = FakeLLM(fail=True)  # would raise if called
    sup = build(db, llm)
    events = list(sup.chat.stream_query("what are the biggest trades?"))
    meta = next(e for e in events if e["type"] == "meta")
    assert meta["confidence"] == "low" and meta["rows_used"] == 0
    assert "don't have any data" in "".join(e["text"] for e in events if e["type"] == "delta")


def test_llm_outage_falls_back_to_raw_data(db):
    from insider_intel.demo import seed_demo

    seed_demo(db)
    sup = build(db, FakeLLM(fail=True))
    text = "".join(e.get("text", "") for e in sup.chat.stream_query("top trades?"))
    assert "unavailable" in text and "ACME" in text  # user still gets the data


def test_citations_are_verified_against_evidence(db):
    from insider_intel.demo import seed_demo

    seed_demo(db)
    sup = build(
        db,
        FakeLLM(
            "ACME sold a lot [RANK {d} #1] and also [RANK 1999-01-01 #9]".format(
                d=datetime.utcnow().strftime("%Y-%m-%d")
            )
        ),
    )
    done = [e for e in sup.chat.stream_query("top trades?") if e["type"] == "done"][0]
    assert len(done["citations"]["valid"]) == 1 and done["citations"]["invalid"] == ["RANK 1999-01-01 #9"]


def test_unknown_ticker_gets_low_confidence(db):
    from insider_intel.demo import seed_demo

    seed_demo(db)
    ev = Retriever(db, settings).run("what about $ZZZZ?")
    assert ev["confidence"] == "low" and "ZZZZ" in ev["confidence_reason"]


def test_ticker_extraction_ignores_common_words(db):
    from insider_intel.demo import seed_demo

    seed_demo(db)
    r = Retriever(db, settings)
    assert r.extract_tickers("Is the CEO of ACME selling? and what about $GLBX") == ["ACME", "GLBX"]
    assert r.extract_tickers("tell me about Globex Energy") == ["GLBX"]
    assert r.extract_tickers("IT IS ON") == []


def test_citation_tag_normalisation():
    assert citation_tags("x [**RANK 2026‑11‑05 #1**] 【SENT BROS】") == {"RANK 2026-11-05 #1", "SENT BROS"}


def test_chart_for_known_ticker_and_fallback(db):
    from insider_intel.demo import seed_demo

    seed_demo(db)
    sup = build(db)
    ev = list(sup.chat.stream_query("chart sentiment for $GLBX"))
    assert any(e["type"] == "chart" for e in ev)
    assert any(e["type"] == "chart" for e in sup.chat.stream_query("draw the top trades"))
