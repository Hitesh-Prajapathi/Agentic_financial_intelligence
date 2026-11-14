"""Pipeline orchestration:  ingest -> rank -> GOVERNANCE GATE -> publish.

  - The ranking is only written to the database after every integrity check passes (quarantine otherwise).
  - It can run on a background thread; the UI polls `progress.snapshot()`.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import datetime, timezone

from insider_intel.obs import timed
from insider_intel.telemetry import current_run_id
from insider_intel.pipeline.gate import QuarantineError, gate, quarantine_run
from insider_intel.pipeline.ingest import ingest
from insider_intel.pipeline.rank import publish, rank

logger = logging.getLogger(__name__)

STAGES = ["sec", "ranking", "gate", "publish"]
STAGE_LABELS = {"sec": "SEC filings", "ranking": "Ranking", "gate": "Governance gate", "publish": "Publish"}


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


class PipelineRunner:
    """Runs the pipeline once at a time; `ingest_fn(source, db, settings, **ingest_kwargs)` / `rank_fn` are injectable for tests."""

    def __init__(self, db, sec_tool, settings, *, ingest_fn=ingest, rank_fn=rank, on_published=None, ingest_kwargs=None):
        self.db, self.sec_tool, self.settings = db, sec_tool, settings
        self.ingest_kwargs = dict(ingest_kwargs or {})  # e.g. since=, max_filings= (CLI backfill)
        self.ingest_fn, self.rank_fn, self.on_published = ingest_fn, rank_fn, on_published
        self.progress = PipelineProgress()
        self._run_lock = threading.Lock()

    def start_async(self, trigger: str = "manual") -> bool:
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

    def run(self, trigger: str = "manual") -> dict:
        """Blocking variant (CLI / tests)."""
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("A pipeline run is already in progress")
        try:
            return self._run(trigger)
        finally:
            current_run_id.set(None)
            self._run_lock.release()

    def _stage(self, name: str, fn):
        """Run one stage, recording status/timing; a failure is recorded and re-raised."""
        self.progress.set_stage(name, "running")
        t0 = time.time()
        try:
            res = fn()
        except Exception as e:  # noqa: BLE001
            self.progress.set_stage(name, "failed", {"error": str(e)[:200]}, time.time() - t0)
            logger.error("Stage %s failed: %s", name, e)
            raise
        status = "skipped" if isinstance(res, dict) and res.get("skipped") else "ok"
        detail = {k: v for k, v in (res or {}).items() if k not in ("accessions", "ranked_df", "top_trades", "window_start", "stats")}
        self.progress.set_stage(name, status, detail, time.time() - t0)
        return res

    def _run(self, trigger: str) -> dict:
        run_id = str(uuid.uuid4())
        self.progress.begin(run_id, trigger)
        current_run_id.set(run_id)   # llm_calls rows made during this run carry it
        self.db.start_run(run_id, _utcnow(), trigger)
        out: dict = {"run_id": run_id}
        with timed("pipeline.run", run_id=run_id) as span:
            try:
                sec = self._stage("sec", lambda: self.ingest_fn(self.sec_tool, self.db, self.settings, **self.ingest_kwargs))
                ranking = self._stage("ranking", lambda: self.rank_fn(self.db, self.settings))

                # ── governance gate: validate BEFORE publishing anything ──
                self.progress.set_stage("gate", "running")
                t0 = time.time()
                ok, checks = gate(self.db, sec, ranking, self.settings.top_n_trades)
                gate_detail = {c.name: ("PASS" if c.passed else f"FAIL: {c.detail}") for c in checks}
                if not ok:
                    failed = [c.name for c in checks if not c.passed]
                    self.progress.set_stage("gate", "failed", gate_detail, time.time() - t0)
                    raise QuarantineError(f"Integrity checks failed: {failed}", failed)
                self.progress.set_stage("gate", "ok", gate_detail, time.time() - t0)
                n_ranked = self._stage("publish", lambda: {"trades_published": publish(self.db, run_id, ranking)})["trades_published"]

                # incomplete catch-up (cap / time budget / fetch errors) is published but marked partial
                incomplete = sec.get("stopped_early") or sec.get("fetch_errors") or sec.get("index_errors")
                status = "partial" if incomplete else "success"
                self.db.update_run(run_id, status=status, completed_at=_utcnow(), stages=self.progress.snapshot()["stages"],
                                   filings_ingested=sec["filings_ingested"], trades_ranked=n_ranked)
                if self.on_published:
                    self.on_published()
                self.progress.finish(status)
                out.update(status=status, sec_result=sec, ranking_result={"top_trades": ranking["top_trades"], "checksum": ranking["checksum"]})
                span.set(status=status)
                return out

            except QuarantineError as e:
                quarantine_run(self.db, run_id, e.reason)
                snap = self.progress.snapshot()["stages"]
                self.db.update_run(run_id, stages=snap)  # keep the gate detail; status stays 'quarantined'
                self.progress.finish("quarantined", e.reason)
                span.set(status="quarantined")
                out.update(status="quarantined", error=e.reason)
                return out

            except Exception as e:  # noqa: BLE001
                logger.exception("Pipeline failed")
                self.db.update_run(run_id, status="failed", completed_at=_utcnow(), error_message=str(e)[:500],
                                   stages=self.progress.snapshot()["stages"])
                self.progress.finish("failed", str(e)[:300])
                span.set(status="failed")
                out.update(status="failed", error=str(e))
                return out
