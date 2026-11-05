"""Supervisor: orchestrates the ingestion pipeline and routes chat questions.

Pipeline:  sec → ranking → GOVERNANCE GATE → persist → social → sentiment → indexing → learning
  - The ranking is only written to the database after every integrity check passes (quarantine otherwise).
  - Stages after the gate are best-effort: one failing degrades the run to `partial` instead of failing it.
  - It runs on a background thread; the UI polls `progress.snapshot()`.
"""
from __future__ import annotations

import logging
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Iterator

from skills.integrity_checks import run_all_checks
from telemetry.otel_setup import tracer
from telemetry.quarantine import QuarantineError, quarantine_run
from tools.llm_tool import LLMError

logger = logging.getLogger(__name__)

STAGES = ["sec", "ranking", "gate", "social", "sentiment", "indexing", "learning"]
STAGE_LABELS = {"sec": "SEC filings", "ranking": "Ranking", "gate": "Governance gate", "social": "Tweets",
                "sentiment": "Sentiment", "indexing": "LightRAG index", "learning": "Learning loop"}
_CHART_RE = re.compile(r"\b(chart|graph|plot|visuali[sz]e|visuali[sz]ation|draw)\b", re.I)
_STATUS_RE = re.compile(r"\b(pipeline status|last (run|update|refresh)|when .*(updated|refreshed)|data freshness|how fresh)\b", re.I)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class PipelineProgress:
    """Thread-safe progress state the UI can poll while the pipeline runs in the background."""

    def __init__(self):
        self._lock = threading.Lock()
        self.reset()

    def reset(self):
        with self._lock:
            self.running = False
            self.run_id = None
            self.trigger = None
            self.started_at = None
            self.finished_at = None
            self.status = None
            self.error = None
            self.stages = {s: {"status": "pending", "detail": {}, "seconds": None} for s in STAGES}

    def begin(self, run_id: str, trigger: str):
        self.reset()
        with self._lock:
            self.running, self.run_id, self.trigger, self.started_at = True, run_id, trigger, _utcnow()

    def set_stage(self, name: str, status: str, detail: dict | None = None, seconds: float | None = None):
        with self._lock:
            self.stages[name] = {"status": status, "detail": detail or {}, "seconds": seconds}

    def finish(self, status: str, error: str | None = None):
        with self._lock:
            self.running, self.status, self.error, self.finished_at = False, status, error, _utcnow()

    def snapshot(self) -> dict:
        with self._lock:
            return {"running": self.running, "run_id": self.run_id, "trigger": self.trigger, "status": self.status,
                    "error": self.error, "started_at": self.started_at, "finished_at": self.finished_at,
                    "stages": {k: dict(v) for k, v in self.stages.items()}}


class SupervisorAgent:
    def __init__(self, *, sec_data_agent, ranking_agent, social_scraper_agent, sentiment_agent, indexing_agent,
                 retrieval_agent, chat_agent, visualization_agent, learning_loop, db_tool, llm_tool, settings):
        self.sec, self.ranking, self.social, self.sentiment = sec_data_agent, ranking_agent, social_scraper_agent, sentiment_agent
        self.indexing, self.retrieval, self.chat, self.viz = indexing_agent, retrieval_agent, chat_agent, visualization_agent
        self.learning, self.db, self.llm, self.settings = learning_loop, db_tool, llm_tool, settings
        self.progress = PipelineProgress()
        self._run_lock = threading.Lock()

    # ═══ ingestion pipeline ═════════════════════════════════════════════════
    def start_pipeline_async(self, trigger: str = "manual") -> bool:
        """Kick off a background run. Returns False if one is already running."""
        if not self._run_lock.acquire(blocking=False):
            return False

        def _target():
            try:
                self._run(trigger)
            finally:
                self._run_lock.release()

        threading.Thread(target=_target, name="pipeline", daemon=True).start()
        return True

    def run_ingestion_pipeline(self, trigger: str = "manual") -> dict:
        """Blocking variant (CLI / tests)."""
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("A pipeline run is already in progress")
        try:
            return self._run(trigger)
        finally:
            self._run_lock.release()

    def _stage(self, name: str, fn, *, tolerant: bool = True):
        """Run one stage, recording status/timing. Tolerant stages swallow errors into a `failed` status."""
        self.progress.set_stage(name, "running")
        t0 = time.time()
        try:
            res = fn()
        except Exception as e:  # noqa: BLE001
            self.progress.set_stage(name, "failed", {"error": str(e)[:200]}, time.time() - t0)
            logger.error("Stage %s failed: %s", name, e)
            if tolerant:
                return None
            raise
        status = "ok"
        if isinstance(res, dict):
            if res.get("skipped"):
                status = "skipped"
            elif res.get("budget_blocked") or res.get("tickers_failed") or res.get("batches_failed") or res.get("tweets_pending"):
                status = "partial"
        self.progress.set_stage(name, status, {k: v for k, v in (res or {}).items()
                                               if k not in ("accessions", "ranked_df", "top_trades", "window_start", "stats")}, time.time() - t0)
        return res

    def _run(self, trigger: str) -> dict:
        run_id = str(uuid.uuid4())
        self.progress.begin(run_id, trigger)
        self.db.start_run(run_id, _utcnow(), trigger)
        out: dict = {"run_id": run_id}
        with tracer.start_as_current_span("pipeline.ingestion") as span:
            span.set_attribute("pipeline.run_id", run_id)
            try:
                sec = self._stage("sec", self.sec.run, tolerant=False)
                rank = self._stage("ranking", self.ranking.run, tolerant=False)

                # ── governance gate: validate BEFORE publishing anything or spending on APIs ──
                self.progress.set_stage("gate", "running")
                t0 = time.time()
                ok, checks = run_all_checks(self.db, rank["window_start"], sec["accessions"], sec["expected_txns"],
                                            rank["ranked_df"], rank["checksum"], rank["stats"], self.settings.top_n_trades)
                gate_detail = {c.name: ("PASS" if c.passed else f"FAIL: {c.detail}") for c in checks}
                if not ok:
                    failed = [c.name for c in checks if not c.passed]
                    self.progress.set_stage("gate", "failed", gate_detail, time.time() - t0)
                    raise QuarantineError(f"Integrity checks failed: {failed}", failed)
                self.progress.set_stage("gate", "ok", gate_detail, time.time() - t0)
                n_ranked = self.ranking.persist(run_id, rank)

                social = self._stage("social", lambda: self.social.run(rank["top_trades"]))
                sentiment = self._stage("sentiment", self.sentiment.run)
                indexing = self._stage("indexing", self.indexing.run)
                learning = self._stage("learning", self.learning.run)

                snap = self.progress.snapshot()["stages"]
                degraded = [k for k, v in snap.items() if v["status"] in ("failed", "partial")]
                status = "partial" if degraded else "success"
                self.db.update_run(
                    run_id, status=status, completed_at=_utcnow(), stages=snap,
                    filings_ingested=sec["filings_ingested"], trades_ranked=n_ranked,
                    tweets_scraped=(social or {}).get("total_tweets", 0),
                    tweets_classified=(sentiment or {}).get("tweets_classified", 0),
                    documents_indexed=(indexing or {}).get("indexed_blocks", 0),
                    error_message=("degraded stages: " + ", ".join(degraded)) if degraded else None)
                self.retrieval._tickers_cache = None
                self.progress.finish(status)
                out.update(status=status, sec_result=sec, ranking_result={"top_trades": rank["top_trades"], "checksum": rank["checksum"]},
                           social_result=social or {}, sentiment_result=sentiment or {}, indexing_result=indexing or {},
                           learning_result=learning or {})
                span.set_attribute("pipeline.status", status)
                return out

            except QuarantineError as e:
                quarantine_run(self.db, run_id, e.reason)
                snap = self.progress.snapshot()["stages"]
                self.db.update_run(run_id, stages=snap)  # keep the gate detail; status stays 'quarantined'
                self.progress.finish("quarantined", e.reason)
                span.set_attribute("pipeline.status", "quarantined")
                out.update(status="quarantined", error=e.reason)
                return out

            except Exception as e:  # noqa: BLE001
                logger.exception("Pipeline failed")
                self.db.update_run(run_id, status="failed", completed_at=_utcnow(), error_message=str(e)[:500],
                                   stages=self.progress.snapshot()["stages"])
                self.progress.finish("failed", str(e)[:300])
                span.set_attribute("pipeline.status", "failed")
                out.update(status="failed", error=str(e))
                return out

    # ═══ chat ═══════════════════════════════════════════════════════════════
    @staticmethod
    def classify_intent(question: str) -> str:
        if _CHART_RE.search(question):
            return "chart"
        if _STATUS_RE.search(question):
            return "status"
        return "chat"

    def status_text(self) -> str:
        r = self.db.query("""SELECT run_id, status, completed_at, filings_ingested, trades_ranked, tweets_classified, error_message
                             FROM pipeline_runs WHERE status <> 'running' ORDER BY started_at DESC LIMIT 1""")
        if not r:
            return "No pipeline run yet. Use **Refresh data** in the sidebar (or load demo data)."
        run_id, status, done, nf, nt, nc, err = r[0]
        return (f"Last pipeline run **{status}** at {done:%Y-%m-%d %H:%M UTC}: {nf} new filings, {nt} trades ranked, "
                f"{nc} tweets classified." + (f" Note: {err}" if err else ""))

    def stream_query(self, question: str, history: list[dict] | None = None) -> Iterator[dict]:
        """Yields events: {'type': 'meta'|'delta'|'chart'|'done'|'error', ...}."""
        t0 = time.time()
        intent = self.classify_intent(question)
        with tracer.start_as_current_span("supervisor.query") as span:
            span.set_attribute("intent", intent)
            span.set_attribute("question.chars", len(question))
            try:
                if intent == "status":
                    txt = self.status_text()
                    yield {"type": "delta", "text": txt}
                    yield {"type": "done", "intent": intent, "confidence": "high", "latency_ms": int((time.time() - t0) * 1000),
                           "citations": {"grounded": True, "valid": [], "invalid": []}}
                    return

                if intent == "chart":
                    tickers = self.retrieval.extract_tickers(question)
                    res = self.viz.run(question, tickers)
                    if res.get("figure") is None:
                        yield {"type": "delta", "text": res.get("error", "I couldn't build that chart.")}
                    else:
                        yield {"type": "chart", "figure": res["figure"]}
                        yield {"type": "delta", "text": res.get("note") or f"Here is the chart ({res['kind']})."}
                    qid = self.db.log_query(question, intent, "high" if res.get("figure") is not None else "low",
                                            int((time.time() - t0) * 1000), None, False, 0)
                    yield {"type": "done", "intent": intent, "confidence": "high" if res.get("figure") is not None else "low",
                           "latency_ms": int((time.time() - t0) * 1000), "query_id": qid,
                           "citations": {"grounded": True, "valid": [], "invalid": []}}
                    return

                evidence = self.retrieval.run(question)
                yield {"type": "meta", "confidence": evidence["confidence"], "reason": evidence["confidence_reason"],
                       "tickers": evidence["tickers"], "mode": evidence["mode"], "used_lightrag": evidence["used_lightrag"],
                       "rows_used": evidence["rows_used"], "evidence": evidence["sql_context"], "as_of": evidence["as_of"]}

                if evidence["rows_used"] == 0:  # nothing to ground an answer in: don't spend an LLM call
                    text = ("I don't have any data loaded yet. Click **Refresh data** in the sidebar to pull the latest "
                            "Form 4 filings, or switch on demo mode.")
                    yield {"type": "delta", "text": text}
                    full, model = text, None
                else:
                    full = ""
                    try:
                        for d in self.chat.stream(evidence, question, history):
                            full += d
                            yield {"type": "delta", "text": d}
                        model = self.llm.last_label
                    except LLMError as e:
                        logger.error("Chat LLM unavailable: %s", e)
                        fallback = ("⚠️ The language models are unavailable right now, so here is the raw data instead:\n\n"
                                    + evidence["sql_context"])
                        yield {"type": "delta", "text": fallback}
                        full, model = fallback, None

                cites = self.chat.verify_citations(full, evidence)
                confidence = "low" if cites["refused"] else evidence["confidence"]  # a refusal is never "high confidence"
                ms = int((time.time() - t0) * 1000)
                qid = self.db.log_query(question, intent, confidence, ms, model, evidence["used_lightrag"], evidence["rows_used"])
                span.set_attribute("latency_ms", ms)
                yield {"type": "done", "intent": intent, "confidence": confidence, "latency_ms": ms, "model": model,
                       "citations": cites, "query_id": qid}
            except Exception as e:  # noqa: BLE001
                logger.exception("Query failed")
                yield {"type": "error", "message": f"Something went wrong: {str(e)[:200]}"}

    def handle_query(self, question: str, history: list[dict] | None = None) -> dict:
        """Non-streaming convenience wrapper (CLI / tests)."""
        out = {"response": "", "chart": None, "confidence": "low", "citations": None}
        for ev in self.stream_query(question, history):
            if ev["type"] == "delta":
                out["response"] += ev["text"]
            elif ev["type"] == "chart":
                out["chart"] = ev["figure"]
            elif ev["type"] == "done":
                out.update(confidence=ev["confidence"], citations=ev.get("citations"), latency_ms=ev["latency_ms"])
            elif ev["type"] == "error":
                out["response"] = ev["message"]
        return out
