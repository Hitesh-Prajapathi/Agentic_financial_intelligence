"""Run the Streamlit script in-process (demo mode, no network, temp databases) and assert it renders without errors."""
from pathlib import Path

import pytest

pytest.importorskip("streamlit.testing.v1")

APP = Path(__file__).resolve().parent.parent / "app" / "streamlit_app.py"


def test_app_renders_in_demo_mode(tmp_path, monkeypatch):
    import streamlit as st
    from streamlit.testing.v1 import AppTest

    from insider_intel.config import settings

    monkeypatch.setattr(settings, "demo_db_path", str(tmp_path / "demo.duckdb"))
    monkeypatch.setattr(settings, "live_db_path", str(tmp_path / "live.duckdb"))
    st.cache_resource.clear()

    at = AppTest.from_file(str(APP), default_timeout=60)
    at.session_state["demo"] = True
    at.run()
    assert not at.exception, [e.value for e in at.exception]

    # dashboard shows the seeded demo ranking
    assert any("Top insider trades" in s.value for s in at.subheader)
    assert len(at.dataframe) >= 1
    # a chat question that needs no LLM (status intent) round-trips through the chat service
    at.chat_input[0].set_value("when was the data last updated?").run()
    assert not at.exception, [e.value for e in at.exception]
    assert any("pipeline run" in m.markdown[0].value.lower() for m in at.chat_message if m.markdown)

    st.cache_resource.clear()


def _scripted_llm_cls():
    from insider_intel.llm.client import LLMTool
    return type("_ScriptedToolLLM", (_ScriptedMixin, LLMTool), {})


class _ScriptedMixin:
    """Stands in for LLMTool (keeps its stats etc.): one top_trades call, then a canned answer. No network."""
    last_label = "fake:tool-model"

    def __init__(self, cfg, answer):
        super().__init__(cfg)
        self.answer, self.step = answer, 0

    def chain(self):
        return []

    def supports_tools(self):
        return True

    def chat_tools(self, messages, tools, tool_choice="auto", **kw):
        from insider_intel.llm.client import LLMResult, ToolCall
        self.step += 1
        if self.step == 1:
            return LLMResult("", "fake", "tool-model", 1, 1, 1, [ToolCall("c1", "top_trades", '{"n": 2}')])
        return LLMResult(self.answer, "fake", "tool-model", 1, 1, 1)

    def stream(self, messages, **kw):
        yield "unused"


def _run_agent_chat(tmp_path, monkeypatch, answer):
    import streamlit as st
    from streamlit.testing.v1 import AppTest

    from insider_intel.config import settings

    monkeypatch.setattr(settings, "demo_db_path", str(tmp_path / "demo.duckdb"))
    monkeypatch.setattr(settings, "live_db_path", str(tmp_path / "live.duckdb"))
    monkeypatch.setattr("insider_intel.system.LLMTool", lambda cfg: _scripted_llm_cls()(cfg, answer))
    st.cache_resource.clear()
    at = AppTest.from_file(str(APP), default_timeout=60)
    at.session_state["demo"] = True
    at.run()
    at.chat_input[0].set_value("what are the biggest insider trades?").run()
    assert not at.exception, [e.value for e in at.exception]
    return at, st


def test_chat_shows_how_i_got_this_and_warns_on_unverified_numbers(tmp_path, monkeypatch):
    at, st = _run_agent_chat(tmp_path, monkeypatch, "The top trade moved $987.65M in total.")
    assert any(e.label.startswith("How I got this (1 lookup)") for e in at.expander)
    assert any("Could not verify" in w.value for w in at.warning)
    st.cache_resource.clear()


def test_clean_agent_answer_has_no_warning(tmp_path, monkeypatch):
    at, st = _run_agent_chat(tmp_path, monkeypatch, "The first-ranked company is shown in the dashboard.")
    assert any(e.label.startswith("How I got this") for e in at.expander)
    assert not any("Could not verify" in w.value for w in at.warning)
    st.cache_resource.clear()


def test_app_renders_read_only_without_refresh_button(tmp_path, monkeypatch):
    import streamlit as st
    from streamlit.testing.v1 import AppTest

    from insider_intel.config import settings
    from insider_intel.demo import seed_demo
    from insider_intel.store.db import DBTool

    demo = tmp_path / "demo.duckdb"
    d = DBTool(str(demo))
    seed_demo(d)
    d.close()
    monkeypatch.setattr(settings, "demo_db_path", str(demo))
    monkeypatch.setattr(settings, "live_db_path", str(tmp_path / "live.duckdb"))
    monkeypatch.setattr(settings, "app_read_only", True)
    st.cache_resource.clear()
    at = AppTest.from_file(str(APP), default_timeout=60)
    at.session_state["demo"] = True
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    assert not any("Refresh data" in b.label for b in at.button)
    assert any("Read-only snapshot" in c.value for c in at.sidebar.caption)
    st.cache_resource.clear()
