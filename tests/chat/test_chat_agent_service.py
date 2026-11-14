import json

import pytest
from chat_testkit import FakeToolLLM, MemDB, call, reply

from insider_intel.chat.agent import CUT_OFF_TEXT, ChatAgent, chunk_text
from insider_intel.chat.retrieval import Retriever
from insider_intel.chat.service import ChatService
from insider_intel.chat.tools import ToolContext, build_registry
from insider_intel.config import Settings


@pytest.fixture
def cfg():
    return Settings(_env_file=None)


def svc(db, llm, cfg):
    return ChatService(db, llm, Retriever(db, cfg), cfg)


def events(service, q="what are the biggest trades?", history=None):
    return list(service.stream_query(q, history))


def text(evs):
    return "".join(e["text"] for e in evs if e["type"] == "delta")


def done(evs):
    return next(e for e in evs if e["type"] == "done")


def test_chunk_text_roundtrips():
    s = "Hello  there, this is\na test."
    assert "".join(chunk_text(s)) == s


def test_two_round_flow_streams_tool_events_then_meta_then_answer(memdb, cfg):
    llm = FakeToolLLM([reply("", call("top_trades", {"n": 1})),
                       reply("The top trade is NOVA selling about $1.95M [RANK 2026-11-13 #1]. Largest trade was 33% of his position.")])
    evs = events(svc(memdb, llm, cfg))
    kinds = [e["type"] for e in evs]
    assert kinds[0] == "tool" and kinds.index("meta") > kinds.index("tool") and kinds.index("meta") < kinds.index("delta")
    tool = evs[0]
    assert (tool["name"], tool["args"], tool["rows"], tool["ok"]) == ("top_trades", {"n": 1}, 1, True)
    d = done(evs)
    assert d["mode"] == "agent" and d["confidence"] == "high" and d["model"] == "fake:tool-model" and d["tool_calls"] == 1
    assert d["grounding"]["unsupported"] == [] and d["citations"]["valid"] == ["RANK 2026-11-13 #1"]
    assert d["citations"]["invalid"] == [] and d["citations"]["grounded"] is True
    meta = next(e for e in evs if e["type"] == "meta")
    assert meta["mode"] == "agent" and meta["rows_used"] == 1 and "top_trades" in meta["evidence"]
    # second request carries the assistant tool_calls message and a matching tool message
    msgs = llm.requests[1]["messages"]
    assert msgs[-2]["role"] == "assistant" and msgs[-2]["tool_calls"][0]["id"] == "c_top_trades"
    assert msgs[-1]["role"] == "tool" and msgs[-1]["tool_call_id"] == "c_top_trades" and "RANK 2026-11-13 #1" in msgs[-1]["content"]
    assert msgs[0]["role"] == "system" and "DATA AS OF" in msgs[0]["content"]
    # logged
    assert memdb.conn.execute("SELECT intent, rows_used FROM query_log").fetchall() == [("chat", 1)]


def test_multi_round_multi_call(memdb, cfg):
    llm = FakeToolLLM([reply("", call("co_insiders", {"ticker": "GLBX"}, "a"), call("cluster_events", {"direction": "BUY"}, "b")),
                       reply("", call("insider_network", {"owner": "Natarajan"}, "c")),
                       reply("Priya Natarajan also bought QRTZ [GRAPH Natarajan Priya].")])
    evs = events(svc(memdb, llm, cfg))
    tools = [e for e in evs if e["type"] == "tool"]
    assert [t["name"] for t in tools] == ["co_insiders", "cluster_events", "insider_network"] and [t["round"] for t in tools] == [1, 1, 2]
    d = done(evs)
    assert d["rounds"] == 2 and d["tool_calls"] == 3 and d["citations"]["valid"] == ["GRAPH Natarajan Priya"]
    assert [r["tool_choice"] for r in llm.requests] == ["auto", "auto", "auto"]


def test_invalid_args_and_unknown_tool_are_returned_to_the_model(memdb, cfg):
    llm = FakeToolLLM([reply("", call("top_trades", {"n": 500}, "x"), call("hack", {}, "y")),
                       reply("", call("top_trades", {"n": 2}, "z")),
                       reply("I fixed it: NOVA [RANK 2026-11-13 #1].")])
    evs = events(svc(memdb, llm, cfg))
    tools = [e for e in evs if e["type"] == "tool"]
    assert [t["ok"] for t in tools] == [False, False, True]
    tool_msgs = [m for m in llm.requests[1]["messages"] if m["role"] == "tool"]
    assert "invalid arguments" in json.loads(tool_msgs[0]["content"])["error"]
    assert "unknown tool" in json.loads(tool_msgs[1]["content"])["error"]
    assert done(evs)["citations"]["valid"] == ["RANK 2026-11-13 #1"]


def test_max_rounds_then_forced_final_answer(memdb, cfg):
    script = [reply("", call("data_status", {}, f"r{i}")) for i in range(4)] + [reply("Data was refreshed [RUN run1].")]
    llm = FakeToolLLM(script)
    evs = events(svc(memdb, llm, cfg), "summarise the data situation and what else?")
    assert len(llm.requests) == 5 and llm.requests[4]["tool_choice"] == "none" and llm.requests[3]["tool_choice"] == "auto"
    d = done(evs)
    assert d["rounds"] == 4 and d["cut_off"] is False and d["citations"]["valid"] == ["RUN run1"]


def test_max_rounds_with_no_answer_gives_cut_off_message(memdb, cfg):
    llm = FakeToolLLM([reply("", call("data_status", {}, f"r{i}")) for i in range(5)])
    evs = events(svc(memdb, llm, cfg), "keep going")
    assert text(evs) == CUT_OFF_TEXT and done(evs)["cut_off"] is True


def test_per_round_call_cap(memdb):
    llm = FakeToolLLM([reply("", *[call("data_status", {}, f"c{i}") for i in range(6)]), reply("done")])
    agent = ChatAgent(llm, build_registry(), ToolContext(memdb))
    list(agent.run("q", None))
    assert [o.ok for o in agent.outcomes] == [True] * 4 + [False] * 2
    assert "too many" in agent.outcomes[-1].error


def test_unsupported_number_and_invalid_citation_are_flagged_and_downgrade_confidence(memdb, cfg):
    llm = FakeToolLLM([reply("", call("top_trades", {"n": 1})),
                       reply("NOVA sold $9.9M [RANK 2026-11-13 #1] [RANK 2026-01-01 #5].")])
    d = done(events(svc(memdb, llm, cfg)))
    assert d["grounding"]["unsupported"] == ["$9.9M"] and d["citations"]["invalid"] == ["RANK 2026-01-01 #5"]
    assert d["confidence"] == "moderate"


def test_abstention_when_tools_are_empty(memdb, cfg):
    llm = FakeToolLLM([reply("", call("filings_for", {"ticker": "ZZZZ"})), reply("I don't have data on that.")])
    d = done(events(svc(memdb, llm, cfg), "did ZZZZ insiders buy?"))
    assert d["confidence"] == "low" and d["grounding"]["abstained"] and d["citations"]["refused"]
    llm = FakeToolLLM([reply("", call("filings_for", {"ticker": "ZZZZ"})), reply("ZZZZ insiders bought $5M of stock.")])
    d = done(events(svc(memdb, llm, cfg), "did ZZZZ insiders buy?"))
    assert d["grounding"]["empty_but_asserted"] is True and d["confidence"] == "low"


def test_history_is_passed_to_the_model(memdb, cfg):
    llm = FakeToolLLM([reply("Sure.")])
    events(svc(memdb, llm, cfg), "and who?", [{"role": "user", "content": "q1"}, {"role": "assistant", "content": "a1"}])
    roles = [m["role"] for m in llm.requests[0]["messages"]]
    assert roles == ["system", "user", "assistant", "user"]


# ── degraded mode ────────────────────────────────────────────────────────────
class StreamOnly:
    last_label = "fake:model"

    def __init__(self, t="ok"):
        self.t = t

    def stream(self, messages, **kw):
        yield from (w + " " for w in self.t.split(" "))

    def chain(self):
        return []


def test_llm_without_tool_api_runs_degraded(memdb, cfg):
    evs = events(svc(memdb, StreamOnly("ACME sold a lot [RANK 2026-11-13 #3]"), cfg))
    d = done(evs)
    assert d["mode"] == "degraded" and next(e for e in evs if e["type"] == "meta")["mode"] == "degraded"
    assert d["citations"]["valid"] == ["RANK 2026-11-13 #3"] and "grounding" in d
    assert memdb.conn.execute("SELECT intent FROM query_log").fetchone()[0] == "chat_degraded"


def test_no_model_supports_tools_falls_back_to_degraded_without_tool_events(memdb, cfg):
    llm = FakeToolLLM(no_tools=True, stream_text="plain answer")
    evs = events(svc(memdb, llm, cfg))
    assert done(evs)["mode"] == "degraded" and not [e for e in evs if e["type"] == "tool"] and "plain answer" in text(evs)


def test_degraded_numeric_check_uses_the_evidence_text(memdb, cfg):
    d = done(events(svc(memdb, StreamOnly("NOVA sold $1.95M and also $42M [RANK 2026-11-13 #1]"), cfg)))
    assert d["grounding"]["unsupported"] == ["$42M"]


def test_agent_llm_outage_falls_through_to_degraded_then_raw_data(memdb, cfg):
    evs = events(svc(memdb, FakeToolLLM(fail=True), cfg))
    assert done(evs)["mode"] == "degraded" and "unavailable" in text(evs) and "NOVA" in text(evs)


def test_agent_disabled_by_setting(memdb):
    s = Settings(_env_file=None)
    object.__setattr__(s, "chat_agent_enabled", False)  # pydantic model: extra attr read via getattr in the service
    llm = FakeToolLLM([reply("should not be used")], stream_text="degraded text")
    assert done(events(svc(memdb, llm, s)))["mode"] == "degraded" and not llm.requests


def test_no_data_shortcut_makes_no_llm_call(cfg):
    db = MemDB().empty()
    llm = FakeToolLLM(fail=True)
    evs = events(svc(db, llm, cfg))
    assert "don't have any data" in text(evs) and done(evs)["confidence"] == "low" and not llm.requests


def test_status_and_chart_intents_never_touch_the_llm(memdb, cfg):
    llm = FakeToolLLM(fail=True)
    s = svc(memdb, llm, cfg)
    assert "pipeline run" in text(events(s, "when was the data last updated?")).lower()
    assert not llm.requests


def test_handle_query_collects_tools_and_grounding(memdb, cfg):
    llm = FakeToolLLM([reply("", call("sentiment", {"ticker": "GLBX"})), reply("Sentiment is bullish, 20 posts [SENT GLBX].")])
    out = svc(memdb, llm, cfg).handle_query("sentiment for GLBX?")
    assert out["mode"] == "agent" and out["tools"][0]["name"] == "sentiment" and out["grounding"]["unsupported"] == []
    assert out["citations"]["valid"] == ["SENT GLBX"]
