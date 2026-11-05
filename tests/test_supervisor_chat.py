from datetime import datetime, timedelta

from agents.chat_agent import ChatAgent
from agents.indexing_agent import IndexingAgent
from agents.ranking_agent import RankingAgent
from agents.retrieval_agent import RetrievalAgent, citation_tags
from agents.sec_data_agent import SecDataAgent
from agents.supervisor import SupervisorAgent
from agents.visualization_agent import VisualizationAgent
from config.settings import settings
from skills.learning_loop import LearningLoop
from tools.chart_tool import ChartTool
from tools.llm_tool import LLMError
from tools.sec_api_tool import SecApiTool


class NoRag:
    enabled, ready, error = False, False, None

    def query_context(self, *a, **k):
        return ""


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


class FakeSec(SecApiTool):
    def __init__(self, raw):
        super().__init__()
        self._raw = raw

    def fetch_recent_filings(self, *a, **k):
        return self._raw


class Unused:
    def run(self, *a, **k):
        return {}


def build(db, llm=None, raw=None, ranking_cls=RankingAgent):
    llm = llm or FakeLLM()
    rag = NoRag()
    retrieval = RetrievalAgent(rag, db, settings)
    return SupervisorAgent(
        sec_data_agent=SecDataAgent(FakeSec(raw or []), db, settings), ranking_agent=ranking_cls(db, settings),
        social_scraper_agent=Unused(), sentiment_agent=Unused(), indexing_agent=IndexingAgent(rag, db, settings),
        retrieval_agent=retrieval, chat_agent=ChatAgent(llm, settings), visualization_agent=VisualizationAgent(ChartTool(), db, settings),
        learning_loop=LearningLoop(db, llm, None, settings), db_tool=db, llm_tool=llm, settings=settings)


def fresh(raw):
    out = []
    for r in raw:
        r = dict(r)
        r["filedAt"] = (datetime.utcnow() - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        out.append(r)
    return out


class BadChecksumRanking(RankingAgent):
    def run(self):
        res = super().run()
        res["checksum"] = "f" * 64
        return res


def test_quarantine_status_survives_and_nothing_is_published(db, raw_filings):
    """Regression: the old code raised after quarantining and the generic handler overwrote the status."""
    sup = build(db, raw=fresh(raw_filings), ranking_cls=BadChecksumRanking)
    out = sup.run_ingestion_pipeline()
    assert out["status"] == "quarantined"
    assert db.query("SELECT status FROM pipeline_runs")[0][0] == "quarantined"
    assert db.query("SELECT COUNT(*) FROM top_trades")[0][0] == 0           # ranking was never published
    assert sup.progress.snapshot()["stages"]["gate"]["status"] == "failed"
    assert sup.progress.snapshot()["stages"]["social"]["status"] == "pending"  # no API spend after a failed gate


def test_successful_run_publishes_ranking(db, raw_filings):
    sup = build(db, raw=fresh(raw_filings))
    out = sup.run_ingestion_pipeline()
    assert out["status"] in ("success", "partial")
    assert db.query("SELECT COUNT(*) FROM top_trades")[0][0] > 0
    assert db.query("SELECT status FROM pipeline_runs")[0][0] == out["status"]


def test_rerunning_does_not_duplicate_rankings(db, raw_filings):
    sup = build(db, raw=fresh(raw_filings))
    sup.run_ingestion_pipeline()
    n = db.query("SELECT COUNT(*) FROM top_trades")[0][0]
    sup.run_ingestion_pipeline()
    assert db.query("SELECT COUNT(*) FROM top_trades")[0][0] == n


def test_intent_routing_uses_word_boundaries():
    c = SupervisorAgent.classify_intent
    assert c("draw me a chart of the top trades") == "chart"
    assert c("Plot sentiment for $AAPL") == "chart"
    assert c("I want to withdraw my money, any trades?") == "chat"      # 'withdraw' contains 'draw'
    assert c("when was the data last updated?") == "status"


def test_no_data_means_low_confidence_and_no_llm_call(db):
    llm = FakeLLM(fail=True)   # would raise if called
    sup = build(db, llm)
    events = list(sup.stream_query("what are the biggest trades?"))
    meta = next(e for e in events if e["type"] == "meta")
    assert meta["confidence"] == "low" and meta["rows_used"] == 0
    assert "don't have any data" in "".join(e["text"] for e in events if e["type"] == "delta")


def test_llm_outage_falls_back_to_raw_data(db):
    from scripts.seed_test_data import seed_demo
    seed_demo(db)
    sup = build(db, FakeLLM(fail=True))
    text = "".join(e.get("text", "") for e in sup.stream_query("top trades?"))
    assert "unavailable" in text and "ACME" in text                      # user still gets the data


def test_citations_are_verified_against_evidence(db):
    from scripts.seed_test_data import seed_demo
    seed_demo(db)
    sup = build(db, FakeLLM("ACME sold a lot [RANK {d} #1] and also [RANK 1999-01-01 #9]".format(d=datetime.utcnow().strftime("%Y-%m-%d"))))
    done = [e for e in sup.stream_query("top trades?") if e["type"] == "done"][0]
    assert len(done["citations"]["valid"]) == 1 and done["citations"]["invalid"] == ["RANK 1999-01-01 #9"]


def test_unknown_ticker_gets_low_confidence(db):
    from scripts.seed_test_data import seed_demo
    seed_demo(db)
    ev = RetrievalAgent(NoRag(), db, settings).run("what about $ZZZZ?")
    assert ev["confidence"] == "low" and "ZZZZ" in ev["confidence_reason"]


def test_ticker_extraction_ignores_common_words(db):
    from scripts.seed_test_data import seed_demo
    seed_demo(db)
    r = RetrievalAgent(NoRag(), db, settings)
    assert r.extract_tickers("Is the CEO of ACME selling? and what about $GLBX") == ["ACME", "GLBX"]
    assert r.extract_tickers("tell me about Globex Energy") == ["GLBX"]
    assert r.extract_tickers("IT IS ON") == []


def test_citation_tag_normalisation():
    assert citation_tags("x [**RANK 2026‑11‑05 #1**] 【SENT BROS】") == {"RANK 2026-11-05 #1", "SENT BROS"}


def test_chart_for_known_ticker_and_fallback(db):
    from scripts.seed_test_data import seed_demo
    seed_demo(db)
    sup = build(db)
    ev = list(sup.stream_query("chart sentiment for $GLBX"))
    assert any(e["type"] == "chart" for e in ev)
    assert any(e["type"] == "chart" for e in sup.stream_query("draw the top trades"))
