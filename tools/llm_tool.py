"""LLM access with a provider/model fallback chain (free models only).

Chain order comes from settings.llm_chain, e.g. Groq first (fast, 1000 req/day free) and
OpenRouter ':free' models as fallback. A rate-limited / failing model is put on a short
cooldown so the next call skips straight to a healthy one.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Iterator

import httpx

from config.settings import settings as default_settings
from telemetry.otel_setup import tracer

logger = logging.getLogger(__name__)

PROVIDERS = {
    "groq": ("https://api.groq.com/openai/v1", "groq_api_key"),
    "openrouter": ("https://openrouter.ai/api/v1", "openrouter_api_key"),
}


class LLMError(Exception):
    """Every model in the chain failed."""


@dataclass
class LLMResult:
    text: str
    provider: str
    model: str
    latency_ms: int
    prompt_tokens: int = 0
    completion_tokens: int = 0

    @property
    def label(self) -> str:
        return f"{self.provider}:{self.model}"


@dataclass
class _Stats:
    calls: int = 0
    failures: int = 0
    by_model: dict = field(default_factory=dict)
    groq_remaining_requests: int | None = None


_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)
_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE)


def extract_json(text: str) -> dict:
    """Parse a JSON object from model output, tolerating code fences / chatter around it."""
    text = _THINK_RE.sub("", text or "").strip()
    text = _FENCE_RE.sub("", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end > start:
            return json.loads(text[start:end + 1])
        raise


class LLMTool:
    def __init__(self, settings=default_settings):
        self.settings = settings
        self._http = httpx.Client(timeout=settings.llm_timeout_s)
        self._cooldown: dict[tuple[str, str], float] = {}
        self._lock = threading.Lock()
        self.stats = _Stats()
        self.last_label: str | None = None
        self._quota_cache: tuple[float, dict] | None = None

    # ── chain handling ──────────────────────────────────────────────────────
    def chain(self) -> list[tuple[str, str]]:
        usable = [(p, m) for p, m in self.settings.chain
                  if p in PROVIDERS and self.settings.has_key(PROVIDERS[p][1])]
        now = time.time()
        ready = [c for c in usable if self._cooldown.get(c, 0) <= now]
        return ready or usable  # if everything is cooling down, try anyway

    def _cool(self, key: tuple[str, str], seconds: float) -> None:
        with self._lock:
            self._cooldown[key] = time.time() + min(max(seconds, 5), 90)

    def _headers(self, provider: str) -> dict:
        key = getattr(self.settings, PROVIDERS[provider][1])
        h = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        if provider == "openrouter":
            h["X-Title"] = "insider-trading-agent"
        return h

    def _payload(self, provider, model, messages, temperature, max_tokens, json_mode, stream=False) -> dict:
        p = {"model": model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens}
        if json_mode:
            p["response_format"] = {"type": "json_object"}
        if stream:
            p["stream"] = True
        if provider == "groq" and "gpt-oss" in model:
            p["reasoning_effort"] = "low"  # keep latency low; these tasks don't need deep reasoning
        return p

    def _record(self, label: str, ok: bool) -> None:
        self.stats.calls += 1
        if not ok:
            self.stats.failures += 1
        s = self.stats.by_model.setdefault(label, {"ok": 0, "fail": 0})
        s["ok" if ok else "fail"] += 1

    def _note_headers(self, provider: str, resp: httpx.Response) -> None:
        if provider == "groq":
            rem = resp.headers.get("x-ratelimit-remaining-requests")
            if rem and rem.isdigit():
                self.stats.groq_remaining_requests = int(rem)

    # ── non-streaming ───────────────────────────────────────────────────────
    def chat(self, messages: list[dict], temperature: float = 0.0, max_tokens: int = 1200,
             json_mode: bool = False, purpose: str = "chat",
             models: list[tuple[str, str]] | None = None) -> LLMResult:
        errors = []
        with tracer.start_as_current_span("llm.chat") as span:
            span.set_attribute("llm.purpose", purpose)
            for provider, model in (models or self.chain()):
                label = f"{provider}:{model}"
                t0 = time.time()
                try:
                    res = self._call_once(provider, model, messages, temperature, max_tokens, json_mode)
                    res.latency_ms = int((time.time() - t0) * 1000)
                    self._record(label, True)
                    self.last_label = label
                    span.set_attribute("llm.model", label)
                    span.set_attribute("llm.latency_ms", res.latency_ms)
                    span.set_attribute("llm.total_tokens", res.prompt_tokens + res.completion_tokens)
                    return res
                except Exception as e:  # noqa: BLE001 - we want to fall through to the next model
                    self._record(label, False)
                    errors.append(f"{label}: {str(e)[:120]}")
                    logger.warning("LLM %s failed: %s", label, str(e)[:160])
            raise LLMError("All models failed → " + " | ".join(errors) if errors else "No LLM keys configured")

    def _call_once(self, provider, model, messages, temperature, max_tokens, json_mode) -> LLMResult:
        base = PROVIDERS[provider][0]
        payload = self._payload(provider, model, messages, temperature, max_tokens, json_mode)
        resp = self._http.post(f"{base}/chat/completions", headers=self._headers(provider), json=payload)
        if resp.status_code == 400 and json_mode:  # some models reject response_format
            payload.pop("response_format", None)
            resp = self._http.post(f"{base}/chat/completions", headers=self._headers(provider), json=payload)
        self._note_headers(provider, resp)
        if resp.status_code in (429, 413, 503, 502, 500):
            ra = resp.headers.get("retry-after")
            self._cool((provider, model), float(ra) if ra and ra.replace(".", "").isdigit() else 30)
        if resp.status_code == 404:
            self._cool((provider, model), 600)
        resp.raise_for_status() if resp.status_code != 200 else None
        data = resp.json()
        if "error" in data:
            raise RuntimeError(str(data["error"])[:200])
        text = _THINK_RE.sub("", data["choices"][0]["message"].get("content") or "").strip()
        if not text:
            self._cool((provider, model), 60)
            raise RuntimeError("empty completion")
        usage = data.get("usage") or {}
        return LLMResult(text, provider, model, 0, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0))

    def chat_json(self, messages: list[dict], temperature: float = 0.0, max_tokens: int = 2000,
                  purpose: str = "json", models: list[tuple[str, str]] | None = None) -> tuple[dict, LLMResult]:
        """Like chat() but guarantees parsed JSON; a model that returns junk falls through to the next."""
        last_err = None
        tried: set[str] = set()
        for _ in range(len(self.settings.chain) or 1):
            res = self.chat(messages, temperature, max_tokens, json_mode=True, purpose=purpose, models=models)
            if res.label in tried:
                break
            try:
                return extract_json(res.text), res
            except json.JSONDecodeError as e:
                last_err = e
                tried.add(res.label)
                self._cool((res.provider, res.model), 30)
        raise LLMError(f"No model returned valid JSON ({last_err})")

    # ── streaming ───────────────────────────────────────────────────────────
    def stream(self, messages: list[dict], temperature: float = 0.1, max_tokens: int = 1200) -> Iterator[str]:
        errors = []
        for provider, model in self.chain():
            label = f"{provider}:{model}"
            got_any = False
            try:
                base = PROVIDERS[provider][0]
                payload = self._payload(provider, model, messages, temperature, max_tokens, False, stream=True)
                in_think = False
                with self._http.stream("POST", f"{base}/chat/completions",
                                       headers=self._headers(provider), json=payload) as resp:
                    self._note_headers(provider, resp)
                    if resp.status_code != 200:
                        if resp.status_code in (429, 413, 503, 500):
                            self._cool((provider, model), 30)
                        if resp.status_code == 404:
                            self._cool((provider, model), 600)
                        resp.read()
                        raise RuntimeError(f"HTTP {resp.status_code}")
                    for line in resp.iter_lines():
                        if not line.startswith("data:"):
                            continue
                        chunk = line[5:].strip()
                        if chunk == "[DONE]":
                            break
                        try:
                            delta = json.loads(chunk)["choices"][0].get("delta", {}).get("content")
                        except (json.JSONDecodeError, KeyError, IndexError):
                            continue
                        if not delta:
                            continue
                        # hide <think> blocks emitted by some reasoning models
                        if "<think>" in delta:
                            in_think = True
                        if in_think:
                            if "</think>" in delta:
                                in_think = False
                                delta = delta.split("</think>", 1)[1]
                            else:
                                continue
                        if delta:
                            got_any = True
                            yield delta
                if got_any:
                    self._record(label, True)
                    self.last_label = label
                    return
                raise RuntimeError("empty stream")
            except Exception as e:  # noqa: BLE001
                self._record(label, False)
                errors.append(f"{label}: {str(e)[:100]}")
                if got_any:  # cannot cleanly switch model mid-answer
                    raise
        raise LLMError("All models failed (stream) → " + " | ".join(errors))

    # ── embeddings (OpenRouter, free model) ─────────────────────────────────
    @property
    def embedding_id(self) -> str:
        return self.settings.local_embedding_model if self.settings.embedding_provider == "local" else self.settings.embedding_model

    def _embed_local(self, texts: list[str]) -> list[list[float]]:
        with self._lock:
            if getattr(self, "_local_model", None) is None:
                from fastembed import TextEmbedding
                self._local_model = TextEmbedding(self.settings.local_embedding_model, cache_dir=self.settings.models_dir)
        return [v.tolist() for v in self._local_model.embed(texts)]

    def embed(self, texts: list[str]) -> list[list[float]]:
        if self.settings.embedding_provider == "local":
            return self._embed_local(texts)
        if not self.settings.has_key("openrouter_api_key"):
            raise LLMError("OPENROUTER_API_KEY required for embeddings")
        out: list[list[float]] = []
        for i in range(0, len(texts), 32):
            batch = texts[i:i + 32]
            last = None
            for attempt in range(3):
                try:
                    r = self._http.post(f"{PROVIDERS['openrouter'][0]}/embeddings",
                                        headers=self._headers("openrouter"),
                                        json={"model": self.settings.embedding_model, "input": batch})
                    r.raise_for_status()
                    data = r.json()["data"]
                    out.extend(d["embedding"] for d in sorted(data, key=lambda d: d["index"]))
                    break
                except Exception as e:  # noqa: BLE001
                    last = e
                    time.sleep(1.5 * (attempt + 1))
            else:
                raise LLMError(f"Embedding failed: {last}")
        return out

    # ── quota / health ──────────────────────────────────────────────────────
    def openrouter_quota(self) -> dict:
        if self._quota_cache and time.time() - self._quota_cache[0] < 60:
            return self._quota_cache[1]
        try:
            r = self._http.get(f"{PROVIDERS['openrouter'][0]}/auth/key", headers=self._headers("openrouter"), timeout=10)
            d = r.json().get("data", {})
            q = d.get("free_model_daily_requests") or {}
            info = {"ok": r.status_code == 200, "free_remaining": q.get("remaining"), "free_limit": q.get("limit"),
                    "usage_usd": d.get("usage")}
        except Exception as e:  # noqa: BLE001
            info = {"ok": False, "error": str(e)[:100]}
        self._quota_cache = (time.time(), info)
        return info

    def ping(self) -> dict:
        """One tiny call per usable provider; used by the Health tab."""
        results = {}
        for provider in PROVIDERS:
            if not self.settings.has_key(PROVIDERS[provider][1]):
                results[provider] = {"ok": False, "detail": "no key"}
                continue
            models = [m for p, m in self.settings.chain if p == provider]
            for model in models:
                t0 = time.time()
                try:
                    self._call_once(provider, model, [{"role": "user", "content": "Reply with: ok"}], 0, 20, False)
                    results[provider] = {"ok": True, "detail": f"{model} · {int((time.time()-t0)*1000)} ms"}
                    break
                except Exception as e:  # noqa: BLE001
                    results[provider] = {"ok": False, "detail": f"{model}: {str(e)[:80]}"}
        return results


# Backwards-compatible name used by older imports
OpenRouterTool = LLMTool
