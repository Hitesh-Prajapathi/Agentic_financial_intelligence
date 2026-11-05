import pytest
from pydantic import ValidationError

from config.settings import Settings
from tools.llm_tool import LLMError, LLMResult, LLMTool


def make_settings(**kw):
    return Settings(groq_api_key="g" * 20, openrouter_api_key="o" * 20, _env_file=None, **kw)


def test_paid_openrouter_models_are_rejected():
    with pytest.raises(ValidationError):
        make_settings(llm_chain="openrouter:openai/gpt-4o")
    with pytest.raises(ValidationError):
        make_settings(embedding_provider="openrouter", embedding_model="openai/text-embedding-3-small")
    make_settings(llm_chain="openrouter:foo/bar:free")           # free is fine
    make_settings(llm_chain="openrouter:openai/gpt-4o", allow_paid_models=True)  # explicit opt-in


def test_placeholder_keys_are_not_usable():
    s = Settings(sec_api_key="your_sec_api_key_here", groq_api_key="", _env_file=None)
    assert not s.has_key("sec_api_key") and not s.has_key("groq_api_key")


def test_chain_skips_providers_without_keys():
    s = Settings(groq_api_key="", openrouter_api_key="o" * 20, _env_file=None)
    assert all(p == "openrouter" for p, _ in LLMTool(s).chain())


def test_fallback_to_next_model_and_cooldown():
    llm = LLMTool(make_settings(llm_chain="groq:a,groq:b"))
    calls = []

    def fake(provider, model, *a, **k):
        calls.append(model)
        if model == "a":
            raise RuntimeError("429")
        return LLMResult("ok", provider, model, 0)

    llm._call_once = fake
    assert llm.chat([{"role": "user", "content": "x"}]).model == "b"
    llm._cool(("groq", "a"), 30)
    calls.clear()
    llm.chat([{"role": "user", "content": "x"}])
    assert calls == ["b"]  # 'a' is cooling down, skipped immediately


def test_all_models_failing_raises():
    llm = LLMTool(make_settings(llm_chain="groq:a"))
    llm._call_once = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    with pytest.raises(LLMError):
        llm.chat([{"role": "user", "content": "x"}])


def test_chat_json_falls_through_on_invalid_json():
    llm = LLMTool(make_settings(llm_chain="groq:a,groq:b"))
    llm._call_once = lambda p, m, *a, **k: LLMResult("not json" if m == "a" else '{"ok": 1}', p, m, 0)
    data, res = llm.chat_json([{"role": "user", "content": "x"}])
    assert data == {"ok": 1} and res.model == "b"
