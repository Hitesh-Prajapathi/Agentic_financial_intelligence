"""LLM access with a provider/model fallback chain (free models only).

Chain order comes from settings.llm_chain, e.g. Groq first (fast, 1000 req/day free) and
OpenRouter ':free' models as fallback. A rate-limited / failing model is put on a short
cooldown so the next call skips straight to a healthy one.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

from insider_intel.config import settings as default_settings
from insider_intel.obs import timed

logger = logging.getLogger(__name__)

PROVIDERS = {
    "groq": ("https://api.groq.com/openai/v1", "groq_api_key"),
    "openrouter": ("https://openrouter.ai/api/v1", "openrouter_api_key"),
}


class LLMError(Exception):
    """Every model in the chain failed."""


class NoToolSupportError(LLMError):
    """No model in the chain accepts tool calling (all rejected `tools`, or none is configured)."""


class _ToolsRejected(RuntimeError):
    """The model/provider refused the `tools` parameter (HTTP 400/404/422): mark it no-tools."""


class _BadToolGeneration(RuntimeError):
    """The model tried to call a tool but produced a malformed call (Groq `tool_use_failed`): not a capability issue."""


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: str  # raw JSON string exactly as the model produced it


@dataclass
class LLMResult:
    text: str
    provider: str
    model: str
    latency_ms: int
    prompt_tokens: int = 0
    completion_tokens: int = 0
    tool_calls: list = field(default_factory=list)  # list[ToolCall]; empty for plain completions

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
            return json.loads(text[start : end + 1])
        raise


class LLMTool:
    def __init__(self, settings=default_settings, on_call: Callable[[dict], None] | None = None):
        self.settings = settings
        self._http = httpx.Client(timeout=settings.llm_timeout_s)
        self._cooldown: dict[tuple[str, str], float] = {}
        self._no_tools: set[tuple[str, str]] = set()  # models that rejected `tools`: skipped by chat_tools()
        self._lock = threading.Lock()
        self._tl = threading.local()  # per-thread hash of the prompt being sent (for telemetry)
        self.on_call = on_call  # hook feeding the llm_calls table (P3): called with one dict per call
        self.call_log: deque[dict] = deque(
            maxlen=500
        )  # in-memory record of recent calls (tokens, latency, model, purpose, status)
        self.stats = _Stats()
        self.last_label: str | None = None
        self._quota_cache: tuple[float, dict] | None = None

    # ── chain handling ──────────────────────────────────────────────────────
    def chain(self) -> list[tuple[str, str]]:
        usable = [(p, m) for p, m in self.settings.chain if p in PROVIDERS and self.settings.has_key(PROVIDERS[p][1])]
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

    def _note_prompt(self, messages: list[dict]) -> None:
        try:
            self._tl.prompt_hash = hashlib.sha256(
                json.dumps(messages, sort_keys=True, default=str).encode()
            ).hexdigest()[:16]
        except Exception:  # noqa: BLE001
            self._tl.prompt_hash = None

    def _emit(
        self,
        purpose: str,
        provider: str,
        model: str,
        status: str,
        latency_ms: int = 0,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        error: str | None = None,
        tools: bool = False,
        n_tool_calls: int = 0,
    ) -> None:
        rec = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "purpose": purpose,
            "provider": provider,
            "model": model,
            "status": status,
            "latency_ms": latency_ms,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "prompt_hash": getattr(self._tl, "prompt_hash", None),
            "tools": tools,
            "n_tool_calls": n_tool_calls,
            "error": error,
        }
        self.call_log.append(rec)
        if self.on_call:
            try:
                self.on_call(dict(rec))
            except Exception:  # noqa: BLE001 - telemetry must never break a call
                logger.warning("on_call hook failed", exc_info=True)

    # ── non-streaming ───────────────────────────────────────────────────────
    def chat(
        self,
        messages: list[dict],
        temperature: float = 0.0,
        max_tokens: int = 1200,
        json_mode: bool = False,
        purpose: str = "chat",
        models: list[tuple[str, str]] | None = None,
    ) -> LLMResult:
        errors = []
        self._note_prompt(messages)
        with timed("llm.chat") as span:
            span.set(llm_purpose=purpose)
            for provider, model in models or self.chain():
                label = f"{provider}:{model}"
                t0 = time.time()
                try:
                    res = self._call_once(provider, model, messages, temperature, max_tokens, json_mode)
                    res.latency_ms = int((time.time() - t0) * 1000)
                    self._record(label, True)
                    self.last_label = label
                    span.set(llm_model=label)
                    span.set(llm_latency_ms=res.latency_ms)
                    span.set(llm_total_tokens=res.prompt_tokens + res.completion_tokens)
                    self._emit(purpose, provider, model, "ok", res.latency_ms, res.prompt_tokens, res.completion_tokens)
                    return res
                except Exception as e:  # noqa: BLE001 - we want to fall through to the next model
                    self._record(label, False)
                    self._emit(purpose, provider, model, "error", int((time.time() - t0) * 1000), error=str(e)[:160])
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

    # ── tool calling (OpenAI-compatible `tools` / `tool_calls`) ─────────────
    def supports_tools(self) -> bool:
        """True if at least one usable model in the chain has not (yet) rejected tool calling."""
        return any(c not in self._no_tools for c in self.chain())

    def mark_no_tools(self, provider: str, model: str, why: str = "") -> None:
        with self._lock:
            self._no_tools.add((provider, model))
        logger.warning("LLM %s:%s marked as no-tools (%s)", provider, model, why[:200])

    def chat_tools(
        self,
        messages: list[dict],
        tools: list[dict],
        tool_choice: str = "auto",
        temperature: float = 0.0,
        max_tokens: int = 1200,
        purpose: str = "agent",
        models: list[tuple[str, str]] | None = None,
    ) -> LLMResult:
        """One non-streaming call that may return `tool_calls`. Models that reject tools are marked no-tools and skipped
        from then on; transient failures fall through to the next model exactly like chat()."""
        candidates = [c for c in (models or self.chain()) if c not in self._no_tools]
        if not candidates:
            raise NoToolSupportError("no model in the chain supports tool calling")
        errors = []
        self._note_prompt(messages)
        with timed("llm.chat_tools") as span:
            span.set(llm_purpose=purpose)
            for provider, model in candidates:
                label = f"{provider}:{model}"
                t0 = time.time()
                try:
                    res = self._call_tools_once(provider, model, messages, tools, tool_choice, temperature, max_tokens)
                    res.latency_ms = int((time.time() - t0) * 1000)
                    self._record(label, True)
                    self.last_label = label
                    span.set(llm_model=label, llm_latency_ms=res.latency_ms, n_tool_calls=len(res.tool_calls))
                    self._emit(
                        purpose,
                        provider,
                        model,
                        "ok",
                        res.latency_ms,
                        res.prompt_tokens,
                        res.completion_tokens,
                        tools=True,
                        n_tool_calls=len(res.tool_calls),
                    )
                    return res
                except _ToolsRejected as e:
                    self.mark_no_tools(provider, model, str(e))
                    self._record(label, False)
                    self._emit(
                        purpose,
                        provider,
                        model,
                        "no_tools",
                        int((time.time() - t0) * 1000),
                        error=str(e)[:160],
                        tools=True,
                    )
                    errors.append(f"{label}: no tool support")
                except Exception as e:  # noqa: BLE001
                    self._record(label, False)
                    status = "bad_tool_call" if isinstance(e, _BadToolGeneration) else "error"
                    self._emit(
                        purpose, provider, model, status, int((time.time() - t0) * 1000), error=str(e)[:160], tools=True
                    )
                    errors.append(f"{label}: {str(e)[:100]}")
                    logger.warning("LLM %s failed (tools): %s", label, str(e)[:160])
        if all("no tool support" in e for e in errors):
            raise NoToolSupportError("; ".join(errors))
        raise LLMError("All models failed (tools) → " + " | ".join(errors))

    def _call_tools_once(self, provider, model, messages, tools, tool_choice, temperature, max_tokens) -> LLMResult:
        base = PROVIDERS[provider][0]
        payload = self._payload(provider, model, messages, temperature, max_tokens, False)
        payload["tools"] = tools
        payload["tool_choice"] = tool_choice
        resp = self._http.post(f"{base}/chat/completions", headers=self._headers(provider), json=payload)
        self._note_headers(provider, resp)
        if resp.status_code != 200:
            body = resp.text[:400]
            low = body.lower()
            if resp.status_code in (429, 413, 503, 502, 500):
                ra = resp.headers.get("retry-after")
                self._cool((provider, model), float(ra) if ra and ra.replace(".", "").isdigit() else 30)
            elif resp.status_code in (400, 404, 422):
                if "tool_use_failed" in low or "failed_generation" in low:
                    raise _BadToolGeneration(
                        body[:160]
                    )  # malformed generation: try the next model, don't mark this one
                if resp.status_code == 404 and "tool" not in low:
                    self._cool((provider, model), 600)  # genuinely missing model
                else:
                    raise _ToolsRejected(f"HTTP {resp.status_code}: {body[:200]}")
            raise RuntimeError(f"HTTP {resp.status_code}")
        data = resp.json()
        if "error" in data:
            raise RuntimeError(str(data["error"])[:200])
        msg = data["choices"][0]["message"]
        text = _THINK_RE.sub("", msg.get("content") or "").strip()
        calls = []
        for i, tc in enumerate(msg.get("tool_calls") or []):
            fn = tc.get("function") or {}
            args = fn.get("arguments", "")
            calls.append(
                ToolCall(
                    tc.get("id") or f"call_{i}", fn.get("name", ""), args if isinstance(args, str) else json.dumps(args)
                )
            )
        if not text and not calls:
            self._cool((provider, model), 60)
            raise RuntimeError("empty completion")
        usage = data.get("usage") or {}
        return LLMResult(
            text, provider, model, 0, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0), calls
        )

    def chat_json(
        self,
        messages: list[dict],
        temperature: float = 0.0,
        max_tokens: int = 2000,
        purpose: str = "json",
        models: list[tuple[str, str]] | None = None,
    ) -> tuple[dict, LLMResult]:
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
    def stream(
        self, messages: list[dict], temperature: float = 0.1, max_tokens: int = 1200, purpose: str = "chat_stream"
    ) -> Iterator[str]:
        errors = []
        self._note_prompt(messages)
        for provider, model in self.chain():
            label = f"{provider}:{model}"
            got_any = False
            t0 = time.time()
            usage: dict = {}
            try:
                base = PROVIDERS[provider][0]
                payload = self._payload(provider, model, messages, temperature, max_tokens, False, stream=True)
                if provider == "groq":
                    payload["stream_options"] = {"include_usage": True}
                in_think = False
                with self._http.stream(
                    "POST", f"{base}/chat/completions", headers=self._headers(provider), json=payload
                ) as resp:
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
                            obj = json.loads(chunk)
                            usage = obj.get("usage") or (obj.get("x_groq") or {}).get("usage") or usage
                            delta = obj["choices"][0].get("delta", {}).get("content")
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
                    self._emit(
                        purpose,
                        provider,
                        model,
                        "ok",
                        int((time.time() - t0) * 1000),
                        usage.get("prompt_tokens", 0),
                        usage.get("completion_tokens", 0),
                    )
                    return
                raise RuntimeError("empty stream")
            except Exception as e:  # noqa: BLE001
                self._record(label, False)
                self._emit(purpose, provider, model, "error", int((time.time() - t0) * 1000), error=str(e)[:160])
                errors.append(f"{label}: {str(e)[:100]}")
                if got_any:  # cannot cleanly switch model mid-answer
                    raise
        raise LLMError("All models failed (stream) → " + " | ".join(errors))

    # ── quota / health ──────────────────────────────────────────────────────
    def openrouter_quota(self) -> dict:
        if self._quota_cache and time.time() - self._quota_cache[0] < 60:
            return self._quota_cache[1]
        try:
            r = self._http.get(
                f"{PROVIDERS['openrouter'][0]}/auth/key", headers=self._headers("openrouter"), timeout=10
            )
            d = r.json().get("data", {})
            q = d.get("free_model_daily_requests") or {}
            info = {
                "ok": r.status_code == 200,
                "free_remaining": q.get("remaining"),
                "free_limit": q.get("limit"),
                "usage_usd": d.get("usage"),
            }
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
                    self._call_once(
                        provider, model, [{"role": "user", "content": "Reply with: ok"}], 0, 200, False
                    )  # reasoning models spend tokens before answering
                    results[provider] = {"ok": True, "detail": f"{model} · {int((time.time() - t0) * 1000)} ms"}
                    break
                except Exception as e:  # noqa: BLE001
                    results[provider] = {"ok": False, "detail": f"{model}: {str(e)[:80]}"}
        return results
