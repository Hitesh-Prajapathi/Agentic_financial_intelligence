"""Composition root: builds the whole system for a mode ('live' or 'demo')."""

from __future__ import annotations

from dataclasses import dataclass

from insider_intel.chat.retrieval import Retriever
from insider_intel.chat.service import ChatService
from insider_intel.config import settings
from insider_intel.llm.client import LLMTool
from insider_intel.pipeline.ingest import EdgarSource
from insider_intel.pipeline.run import PipelineRunner
from insider_intel.store.db import DBTool, get_db, get_read_only_db
from insider_intel.telemetry import make_llm_call_writer


@dataclass
class System:
    mode: str
    db: DBTool
    llm: LLMTool
    sec: EdgarSource
    pipeline: PipelineRunner | None  # None in read-only mode
    chat: ChatService
    retriever: Retriever


def build_system(mode: str = "live", cfg=settings, *, read_only: bool = False) -> System:
    """`read_only=True` is the hosted request path: DuckDB opened read-only and no pipeline (`system.pipeline is None`)."""
    path = cfg.live_db_path if mode == "live" else cfg.demo_db_path
    db = get_read_only_db(path) if read_only else get_db(path)
    llm = LLMTool(cfg)
    if not read_only:
        llm.on_call = make_llm_call_writer(db, cfg)  # llm_calls telemetry (best effort)
    sec = EdgarSource(cfg)  # builds its HTTP client lazily, so a missing SEC_USER_AGENT only matters on `run`
    retriever = Retriever(db, cfg)
    pipeline = None if read_only else PipelineRunner(db, sec, cfg, on_published=retriever.invalidate)
    return System(mode, db, llm, sec, pipeline, ChatService(db, llm, retriever, cfg), retriever)
