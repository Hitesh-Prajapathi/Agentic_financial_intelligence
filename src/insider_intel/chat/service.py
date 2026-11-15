"""Chat service: cheap regex routing (status / chart), then the tool-calling agent, with the v2 context-stuffing path
as a labelled "degraded" fallback. Emits the event protocol the UI consumes:

    meta   {confidence, reason, tickers, rows_used, evidence, as_of, mode}
    tool   {name, args, rows, ok, error, round}                     one per tool call ("How I got this")
    delta  {text}                                                    answer chunks
    chart  {figure}
    done   {intent, confidence, latency_ms, model, citations, grounding, mode, query_id}
    error  {message}
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Iterator
from datetime import datetime, timezone

from insider_intel.chat.agent import ChatAgent, llm_supports_tools
from insider_intel.chat.degraded import stream_answer
from insider_intel.chat.grounding import check_grounding
from insider_intel.chat.tools import ToolContext, build_registry
from insider_intel.llm.client import LLMError
from insider_intel.obs import timed
from insider_intel.ui.charts import chart_for_question

logger = logging.getLogger(__name__)

_CHART_RE = re.compile(r"\b(chart|graph|plot|visuali[sz]e|visuali[sz]ation|draw)\b", re.I)
_STATUS_RE = re.compile(
    r"\b(pipeline status|last (run|update|refresh)|when .*(updated|refreshed)|data freshness|how fresh)\b", re.I
)


def classify_intent(question: str) -> str:
    if _CHART_RE.search(question):
        return "chart"
    if _STATUS_RE.search(question):
        return "status"
    return "chat"


NO_DATA_TEXT = (
    "I don't have any data loaded yet. Click **Refresh data** in the sidebar to pull the latest "
    "Form 4 filings, or switch on demo mode."
)


def legacy_citations(g: dict) -> dict:
    """Map grounding's keys onto the keys older UI/tests read (cited / valid / invalid / grounded / refused)."""
    cited = sorted(set(g["citations_valid"]) | set(g["citations_invalid"]))
    return {
        "cited": cited,
        "valid": g["citations_valid"],
        "invalid": g["citations_invalid"],
        "grounded": bool(g["citations_valid"]) or g["abstained"],
        "refused": g["abstained"],
    }


def _age_hours(as_of) -> float | None:
    if not as_of:
        return None
    if not isinstance(as_of, datetime):  # a date
        as_of = datetime.combine(as_of, datetime.min.time())
    return (datetime.now(timezone.utc).replace(tzinfo=None) - as_of).total_seconds() / 3600


class ChatService:
    def __init__(self, db, llm, retriever, settings):
        self.db, self.llm, self.retriever, self.settings = db, llm, retriever, settings
        self.registry = build_registry()
        self.tool_ctx = ToolContext(db, rank_window_days=settings.rank_window_days)

    def _agent_enabled(self) -> bool:
        return bool(self.settings.chat_agent_enabled) and llm_supports_tools(self.llm)

    def _last_prompt_hash(self) -> str | None:
        log = getattr(self.llm, "call_log", None)
        return log[-1].get("prompt_hash") if log else None

    def _has_data(self) -> bool:
        return bool(
            self.db.query("SELECT COUNT(*) FROM top_trades")[0][0]
            or self.db.query("SELECT COUNT(*) FROM filings")[0][0]
        )

    def status_text(self) -> str:
        r = self.db.query("""SELECT run_id, status, completed_at, filings_ingested, trades_ranked, error_message
                             FROM pipeline_runs WHERE status <> 'running' ORDER BY started_at DESC LIMIT 1""")
        if not r:
            return "No pipeline run yet. Use **Refresh data** in the sidebar (or load demo data)."
        run_id, status, done, nf, nt, err = r[0]
        return f"Last pipeline run **{status}** at {done:%Y-%m-%d %H:%M UTC}: {nf} new filings, {nt} trades ranked." + (
            f" Note: {err}" if err else ""
        )

    def stream_query(self, question: str, history: list[dict] | None = None) -> Iterator[dict]:
        """Yields events: {'type': 'meta'|'delta'|'chart'|'done'|'error', ...}."""
        t0 = time.time()
        intent = classify_intent(question)
        with timed("chat.query", intent=intent, question_chars=len(question)) as span:
            try:
                if intent == "status":
                    txt = self.status_text()
                    yield {"type": "delta", "text": txt}
                    yield {
                        "type": "done",
                        "intent": intent,
                        "confidence": "high",
                        "latency_ms": int((time.time() - t0) * 1000),
                        "citations": {"grounded": True, "valid": [], "invalid": []},
                    }
                    return

                if intent == "chart":
                    tickers = self.retriever.extract_tickers(question)
                    res = chart_for_question(self.db, self.settings, question, tickers)
                    if res.get("figure") is None:
                        yield {"type": "delta", "text": res.get("error", "I couldn't build that chart.")}
                    else:
                        yield {"type": "chart", "figure": res["figure"]}
                        yield {"type": "delta", "text": res.get("note") or f"Here is the chart ({res['kind']})."}
                    qid = self.db.log_query(
                        question,
                        intent,
                        "high" if res.get("figure") is not None else "low",
                        int((time.time() - t0) * 1000),
                        None,
                        0,
                        mode="chart",
                    )
                    yield {
                        "type": "done",
                        "intent": intent,
                        "confidence": "high" if res.get("figure") is not None else "low",
                        "latency_ms": int((time.time() - t0) * 1000),
                        "query_id": qid,
                        "citations": {"grounded": True, "valid": [], "invalid": []},
                    }
                    return

                if self._agent_enabled():
                    done = yield from self._agent_turn(question, history, intent, t0)
                    if done:
                        span.set(latency_ms=int((time.time() - t0) * 1000), mode="agent")
                        return
                yield from self._degraded_turn(question, history, intent, t0)
                span.set(latency_ms=int((time.time() - t0) * 1000), mode="degraded")
            except Exception as e:  # noqa: BLE001
                logger.exception("Query failed")
                yield {"type": "error", "message": f"Something went wrong: {str(e)[:200]}"}

    # ── agent path ──────────────────────────────────────────────────────────
    def _evidence_text(self, outcomes) -> str:
        return "\n".join(
            f"{o.name}({json.dumps(o.args, default=str)}) -> {o.message if o.ok else 'ERROR ' + str(o.error)}"
            for o in outcomes
        )

    def _agent_turn(self, question, history, intent, t0) -> Iterator[dict]:
        """Returns True when it produced a final `done`; False when the caller should fall back to degraded mode."""
        as_of = self.db.data_as_of()
        if not self._has_data():
            yield {
                "type": "meta",
                "mode": "agent",
                "confidence": "low",
                "reason": "no data loaded",
                "tickers": [],
                "rows_used": 0,
                "evidence": "",
                "as_of": as_of,
            }
            yield {"type": "delta", "text": NO_DATA_TEXT}
            ms = int((time.time() - t0) * 1000)
            qid = self.db.log_query(question, intent, "low", ms, None, 0, mode="agent")
            yield {
                "type": "done",
                "intent": intent,
                "confidence": "low",
                "latency_ms": ms,
                "model": None,
                "mode": "agent",
                "citations": {"cited": [], "valid": [], "invalid": [], "grounded": True, "refused": True},
                "query_id": qid,
            }
            return True

        agent = ChatAgent(self.llm, self.registry, self.tool_ctx, max_rounds=int(self.settings.chat_max_tool_rounds))
        started, run, buffered = False, None, []
        try:
            for ev in agent.run(question, history, as_of):
                if ev["type"] == "tool":
                    yield ev
                elif ev["type"] == "delta":
                    if not started:
                        started = True
                        rows = sum(o.n_rows for o in agent.outcomes)
                        conf, why = self._confidence(rows, as_of)
                        yield {
                            "type": "meta",
                            "mode": "agent",
                            "confidence": conf,
                            "reason": why,
                            "tickers": [],
                            "rows_used": rows,
                            "evidence": self._evidence_text(agent.outcomes),
                            "as_of": as_of,
                        }
                    buffered.append(ev["text"])
                    yield ev
                elif ev["type"] == "result":
                    run = ev["run"]
        except LLMError as e:  # includes NoToolSupportError: nothing was streamed yet, so fall back cleanly
            logger.warning("Agent unavailable (%s); falling back to degraded mode", str(e)[:160])
            return False

        rows = sum(o.n_rows for o in run.outcomes)
        conf, why = self._confidence(rows, as_of)
        g = check_grounding(run.answer, run.outcomes, question, history)
        if g["abstained"]:
            conf = "low"  # a refusal is never "high confidence"
        elif conf == "high" and (
            g["unsupported"] or g["citations_invalid"] or g["empty_but_asserted"] or not g["citations_valid"]
        ):
            conf = "moderate"
        ms = int((time.time() - t0) * 1000)
        qid = self.db.log_query(
            question,
            intent,
            conf,
            ms,
            run.model,
            rows,
            mode="agent",
            tool_calls=len(run.outcomes),
            unsupported_numbers=len(g.get("unsupported") or []),
            prompt_hash=self._last_prompt_hash(),
        )
        yield {
            "type": "done",
            "intent": intent,
            "confidence": conf,
            "reason": why,
            "latency_ms": ms,
            "model": run.model,
            "mode": "agent",
            "citations": legacy_citations(g),
            "grounding": g,
            "query_id": qid,
            "tool_calls": len(run.outcomes),
            "rounds": run.rounds,
            "cut_off": run.cut_off,
        }
        return True

    def _confidence(self, rows: int, as_of) -> tuple[str, str]:
        age_h = _age_hours(as_of)
        win_h = 24.0 * float(self.settings.rank_window_days)  # data is refreshed ~twice a month
        if rows == 0:
            return "low", "the lookups returned no matching rows"
        if age_h is not None and age_h > 2 * win_h:
            return "low", f"data is {age_h / 24:.0f} days old"
        if age_h is not None and age_h > win_h:
            return "moderate", f"data is {age_h / 24:.0f} days old"
        return "high", "data within the refresh window"

    # ── degraded path (v2 context stuffing) ─────────────────────────────────
    def _degraded_turn(self, question, history, intent, t0) -> Iterator[dict]:
        evidence = self.retriever.run(question)
        yield {
            "type": "meta",
            "mode": "degraded",
            "confidence": evidence["confidence"],
            "reason": evidence["confidence_reason"],
            "tickers": evidence["tickers"],
            "rows_used": evidence["rows_used"],
            "evidence": evidence["sql_context"],
            "as_of": evidence["as_of"],
        }

        if evidence["rows_used"] == 0:  # nothing to ground an answer in: don't spend an LLM call
            yield {"type": "delta", "text": NO_DATA_TEXT}
            full, model = NO_DATA_TEXT, None
        else:
            full = ""
            try:
                for d in stream_answer(self.llm, evidence, question, history):
                    full += d
                    yield {"type": "delta", "text": d}
                model = self.llm.last_label
            except LLMError as e:
                logger.error("Chat LLM unavailable: %s", e)
                fallback = (
                    "⚠️ The language models are unavailable right now, so here is the raw data instead:\n\n"
                    + evidence["sql_context"]
                )
                yield {"type": "delta", "text": fallback}
                full, model = fallback, None

        g = check_grounding(
            full, [], question, history, evidence_text=evidence["sql_context"], extra_sources=evidence["known_sources"]
        )
        confidence = "low" if g["abstained"] else evidence["confidence"]  # a refusal is never "high confidence"
        ms = int((time.time() - t0) * 1000)
        qid = self.db.log_query(
            question,
            "chat_degraded",
            confidence,
            ms,
            model,
            evidence["rows_used"],
            mode="degraded",
            tool_calls=0,
            unsupported_numbers=len(g.get("unsupported") or []),
            prompt_hash=self._last_prompt_hash(),
        )
        yield {
            "type": "done",
            "intent": intent,
            "confidence": confidence,
            "latency_ms": ms,
            "model": model,
            "mode": "degraded",
            "citations": legacy_citations(g),
            "grounding": g,
            "query_id": qid,
        }

    def handle_query(self, question: str, history: list[dict] | None = None) -> dict:
        """Non-streaming convenience wrapper (CLI / tests)."""
        out = {
            "response": "",
            "chart": None,
            "confidence": "low",
            "citations": None,
            "tools": [],
            "grounding": None,
            "mode": None,
        }
        for ev in self.stream_query(question, history):
            if ev["type"] == "delta":
                out["response"] += ev["text"]
            elif ev["type"] == "chart":
                out["chart"] = ev["figure"]
            elif ev["type"] == "tool":
                out["tools"].append(ev)
            elif ev["type"] == "done":
                out.update(
                    confidence=ev["confidence"],
                    citations=ev.get("citations"),
                    latency_ms=ev["latency_ms"],
                    grounding=ev.get("grounding"),
                    mode=ev.get("mode"),
                )
            elif ev["type"] == "error":
                out["response"] = ev["message"]
        return out
