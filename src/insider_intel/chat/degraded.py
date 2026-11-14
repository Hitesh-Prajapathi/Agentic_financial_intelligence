"""Degraded mode: the v2 context-stuffing path (SQL evidence pasted into the prompt, no tool calling).

Used when no model in the chain supports tool calling (or the LLM object has no tool API). Answers are labelled
`mode="degraded"`; citations and numbers are still checked against the evidence text by chat.grounding.
"""
from __future__ import annotations

from typing import Iterator

from insider_intel.llm import load_prompt
from insider_intel.obs import timed

MAX_HISTORY = 6  # last 3 exchanges


def build_messages(evidence: dict, question: str, history: list[dict] | None = None) -> list[dict]:
    as_of = evidence["as_of"].strftime("%Y-%m-%d %H:%M UTC") if evidence.get("as_of") else "unknown (no data loaded)"
    system = load_prompt("chat_agent").format(
        as_of=as_of,
        sql_context=evidence["sql_context"] or "(no data loaded yet)")
    if evidence["confidence"] == "low":
        system = (f"CAUTION: retrieval confidence is LOW ({evidence['confidence_reason']}). "
                  "Be explicit about what is missing and do not extrapolate.\n\n") + system
    msgs = [{"role": "system", "content": system}]
    for m in (history or [])[-MAX_HISTORY:]:
        if m.get("role") in ("user", "assistant") and m.get("content"):
            msgs.append({"role": m["role"], "content": m["content"][:1500]})
    msgs.append({"role": "user", "content": question})
    return msgs


def stream_answer(llm, evidence: dict, question: str, history: list[dict] | None = None) -> Iterator[str]:
    with timed("chat.stream", retrieval_confidence=evidence["confidence"]):
        yield from llm.stream(build_messages(evidence, question, history), temperature=0.1, max_tokens=900)
