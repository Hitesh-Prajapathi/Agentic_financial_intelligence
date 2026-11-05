"""LightRAG wrapper (switchable via ENABLE_LIGHTRAG).

LightRAG is async and keeps loop-bound locks, while Streamlit/background code is threaded. So LightRAG
lives on ONE dedicated event-loop thread; callers submit coroutines and wait with a hard timeout. That
removes the old nest_asyncio hack and means a slow index can never freeze the UI.

All LLM traffic goes through LLMTool (free-model chain) and embeddings use a free OpenRouter model.
"""
from __future__ import annotations

import asyncio
import logging
import re
import threading
import time
from concurrent.futures import TimeoutError as FutureTimeout
from pathlib import Path

from telemetry.otel_setup import tracer

logger = logging.getLogger(__name__)


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:40]


class LightRAGTool:
    def __init__(self, settings, llm, namespace: str = "live"):
        self.settings, self.llm = settings, llm
        self.enabled = bool(settings.enable_lightrag)
        self.workspace = Path(settings.lightrag_dir) / f"{namespace}_{_slug(llm.embedding_id)}"
        self._errors: list[str] = []
        self.rag = None
        self.error: str | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._init_lock = threading.Lock()

    # ── lifecycle ───────────────────────────────────────────────────────────
    @property
    def ready(self) -> bool:
        return self.rag is not None

    def _start_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, name="lightrag-loop", daemon=True).start()

    def _run(self, coro, timeout: float):
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=timeout)
        except FutureTimeout:
            fut.cancel()
            raise TimeoutError(f"LightRAG operation exceeded {timeout:.0f}s")

    def ensure_ready(self) -> bool:
        if not self.enabled:
            return False
        if self.ready:
            return True
        with self._init_lock:
            if self.ready:
                return True
            if self.error:  # failed before; don't hammer it on every request
                return False
            try:
                from lightrag import LightRAG
                from lightrag.utils import EmbeddingFunc

                if self._loop is None:
                    self._start_loop()
                self.workspace.mkdir(parents=True, exist_ok=True)
                llm = self.llm

                async def llm_func(prompt, system_prompt=None, history_messages=None, **kwargs):
                    messages = []
                    if system_prompt:
                        messages.append({"role": "system", "content": system_prompt})
                    messages.extend(history_messages or [])
                    messages.append({"role": "user", "content": prompt})
                    as_json = bool(kwargs.get("response_format")) or bool(kwargs.get("keyword_extraction"))
                    try:
                        res = await asyncio.to_thread(llm.chat, messages, 0.0, 2500, as_json, "lightrag")
                    except Exception as e:  # noqa: BLE001
                        self._errors.append(f"llm: {str(e)[:120]}")
                        raise
                    return res.text

                dim = len(llm.embed(["dimension probe"])[0])

                async def embed_func(texts: list[str]):
                    import numpy as np
                    try:
                        vecs = await asyncio.to_thread(llm.embed, list(texts))
                    except Exception as e:  # noqa: BLE001
                        self._errors.append(f"embed: {str(e)[:120]}")
                        raise
                    return np.array(vecs, dtype="float32")

                async def _build():
                    rag = LightRAG(
                        working_dir=str(self.workspace),
                        llm_model_func=llm_func,
                        embedding_func=EmbeddingFunc(embedding_dim=dim, max_token_size=8192, func=embed_func),
                        entity_extract_max_gleaning=0,   # one extraction pass: halves LLM calls per chunk
                        llm_model_max_async=2,           # stay inside free-tier rate limits
                        embedding_func_max_async=2,
                        chunk_token_size=1200,
                        max_parallel_insert=1,
                    )
                    await rag.initialize_storages()
                    return rag

                self.rag = self._run(_build(), timeout=90)
                logger.info("LightRAG ready (dim=%d, workspace=%s)", dim, self.workspace)
            except Exception as e:  # noqa: BLE001
                self.error = f"{type(e).__name__}: {str(e)[:160]}"
                logger.error("LightRAG unavailable: %s", self.error)
                return False
        return True

    # ── operations ──────────────────────────────────────────────────────────
    def insert(self, texts: list[str]) -> int:
        if not texts or not self.ensure_ready():
            return 0
        with tracer.start_as_current_span("lightrag.insert") as span:
            span.set_attribute("lightrag.texts", len(texts))
            self._errors.clear()
            self._run(self.rag.ainsert(texts), timeout=self.settings.lightrag_index_timeout_s)
            if self._errors:  # LightRAG logs internal failures but still returns normally - don't report false success
                raise RuntimeError("LightRAG indexing had errors: " + "; ".join(self._errors[:3]))
            return len(texts)

    def query_context(self, question: str, mode: str = "hybrid") -> str:
        """Retrieval only (no answer generation): returns the graph/vector context, or '' on any problem."""
        if not self.ensure_ready():
            return ""
        from lightrag import QueryParam
        with tracer.start_as_current_span("lightrag.query") as span:
            span.set_attribute("lightrag.mode", mode)
            t0 = time.time()
            try:
                out = self._run(
                    self.rag.aquery(question, param=QueryParam(mode=mode, only_need_context=True, top_k=10,
                                                               chunk_top_k=6, max_total_tokens=4000, enable_rerank=False)),
                    timeout=self.settings.lightrag_query_timeout_s)
            except Exception as e:  # noqa: BLE001
                logger.warning("LightRAG query failed (%s) — falling back to SQL context", e)
                span.set_attribute("lightrag.error", str(e)[:100])
                return ""
            span.set_attribute("lightrag.latency_ms", int((time.time() - t0) * 1000))
            return out if isinstance(out, str) else ""
