"""Answer generation (streamed) with citation verification."""
import logging
import re
from pathlib import Path
from typing import Iterator

from agents.retrieval_agent import citation_tags
from telemetry.otel_setup import tracer

logger = logging.getLogger(__name__)
PROMPT_PATH = Path(__file__).resolve().parent.parent / "config" / "prompts" / "chat_agent.txt"
MAX_HISTORY = 6  # last 3 exchanges


class ChatAgent:
    def __init__(self, llm_tool, settings):
        self.llm, self.settings = llm_tool, settings
        self._template = PROMPT_PATH.read_text()

    def build_messages(self, evidence: dict, question: str, history: list[dict] | None = None) -> list[dict]:
        as_of = evidence["as_of"].strftime("%Y-%m-%d %H:%M UTC") if evidence.get("as_of") else "unknown (no data loaded)"
        system = self._template.format(
            as_of=as_of,
            sql_context=evidence["sql_context"] or "(no data loaded yet)",
            rag_context=evidence["rag_context"] or "(none)")
        if evidence["confidence"] == "low":
            system = (f"CAUTION: retrieval confidence is LOW ({evidence['confidence_reason']}). "
                      "Be explicit about what is missing and do not extrapolate.\n\n") + system
        msgs = [{"role": "system", "content": system}]
        for m in (history or [])[-MAX_HISTORY:]:
            if m.get("role") in ("user", "assistant") and m.get("content"):
                msgs.append({"role": m["role"], "content": m["content"][:1500]})
        msgs.append({"role": "user", "content": question})
        return msgs

    def stream(self, evidence: dict, question: str, history: list[dict] | None = None) -> Iterator[str]:
        with tracer.start_as_current_span("chat_agent.stream") as span:
            span.set_attribute("retrieval.confidence", evidence["confidence"])
            yield from self.llm.stream(self.build_messages(evidence, question, history), temperature=0.1, max_tokens=900)

    def run(self, evidence: dict, question: str, history: list[dict] | None = None) -> str:
        return "".join(self.stream(evidence, question, history))

    @staticmethod
    def verify_citations(response: str, evidence: dict) -> dict:
        """Which of the cited tags actually exist in the evidence we supplied?"""
        cited = citation_tags(response)
        known = evidence.get("known_sources", set())
        valid = cited & known
        refused = bool(re.search(r"don'?t have (any )?data|no data", response, re.I))
        return {"cited": sorted(cited), "valid": sorted(valid), "invalid": sorted(cited - valid),
                "grounded": bool(valid) or refused, "refused": refused}
