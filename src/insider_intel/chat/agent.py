"""The one real agent (plan B3): an LLM with typed read-only SQL tools, a bounded tool loop, then the final answer.

    system prompt -> [model decides tool calls -> validate args (pydantic) -> run -> tool messages back]  x <= 4 rounds
                  -> final answer (streamed to the UI in small chunks)

Errors (bad args, unknown tool, failing query) are returned to the model as tool messages so it can retry. After the
last allowed round one more call is made with tool_choice="none" so the model must answer from what it has.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass, field

from insider_intel.chat.tools import ToolContext, ToolOutcome, ToolRegistry
from insider_intel.llm import load_prompt
from insider_intel.obs import timed

MAX_HISTORY = 6
MAX_CALLS_PER_ROUND = 4
CUT_OFF_TEXT = (
    "I couldn't finish looking this up within the allowed number of steps, so I won't guess. Try a narrower question."
)


@dataclass
class AgentRun:
    answer: str = ""
    outcomes: list[ToolOutcome] = field(default_factory=list)
    model: str | None = None
    rounds: int = 0  # rounds in which the model called tools
    cut_off: bool = False  # hit the round limit and had to be forced to answer


def llm_supports_tools(llm) -> bool:
    """True if `llm` exposes the tool API and at least one chain model has not rejected tools."""
    if not callable(getattr(llm, "chat_tools", None)):
        return False
    probe = getattr(llm, "supports_tools", None)
    return bool(probe()) if callable(probe) else True


def chunk_text(text: str, words: int = 3) -> Iterator[str]:
    """Replay a finished answer as small deltas so the UI streams it (no extra LLM request needed)."""
    toks = re.findall(r"\S+\s*", text)
    for i in range(0, len(toks), words):
        yield "".join(toks[i : i + words])


class ChatAgent:
    def __init__(self, llm, registry: ToolRegistry, ctx: ToolContext, max_rounds: int = 4):
        self.llm, self.registry, self.ctx, self.max_rounds = llm, registry, ctx, max_rounds
        self.outcomes: list[ToolOutcome] = []  # updated live, so the caller can read evidence before the answer streams

    def system_prompt(self, as_of) -> str:
        stamp = as_of.strftime("%Y-%m-%d %H:%M UTC") if hasattr(as_of, "strftime") else "unknown (no data loaded)"
        return load_prompt("chat_agent_tools").format(as_of=stamp)

    def run(self, question: str, history: list[dict] | None, as_of=None) -> Iterator[dict]:
        """Yields {'type':'tool',...} per tool call, then {'type':'delta'} chunks, then {'type':'result','run':AgentRun}.
        Raises NoToolSupportError / LLMError from the LLM layer if it cannot get a tool-capable model to respond."""
        messages = [{"role": "system", "content": self.system_prompt(as_of)}]
        for m in (history or [])[-MAX_HISTORY:]:
            if m.get("role") in ("user", "assistant") and m.get("content"):
                messages.append({"role": m["role"], "content": m["content"][:1500]})
        messages.append({"role": "user", "content": question})
        tools = self.registry.openai_tools()
        run = AgentRun()

        with timed("chat.agent", question_chars=len(question)) as span:
            final_text = ""
            for rnd in range(self.max_rounds + 1):
                force = rnd == self.max_rounds
                res = self.llm.chat_tools(
                    messages,
                    tools,
                    tool_choice="none" if force else "auto",
                    temperature=0.0,
                    max_tokens=900,
                    purpose=f"agent_round_{rnd + 1}",
                )
                run.model = res.label if hasattr(res, "label") else None
                if force or not res.tool_calls:
                    final_text = res.text
                    run.cut_off = force and bool(res.tool_calls or not res.text)
                    break
                run.rounds += 1
                messages.append(
                    {
                        "role": "assistant",
                        "content": res.text or None,
                        "tool_calls": [
                            {"id": c.id, "type": "function", "function": {"name": c.name, "arguments": c.arguments}}
                            for c in res.tool_calls
                        ],
                    }
                )
                for i, call in enumerate(res.tool_calls):
                    if i >= MAX_CALLS_PER_ROUND:
                        out = ToolOutcome(
                            call.name, {}, False, error=f"too many tool calls in one round (max {MAX_CALLS_PER_ROUND})"
                        )
                    else:
                        out = self.registry.call(self.ctx, call.name, call.arguments)
                    self.outcomes.append(out)
                    messages.append({"role": "tool", "tool_call_id": call.id, "content": out.message})
                    yield {
                        "type": "tool",
                        "name": out.name,
                        "args": out.args,
                        "rows": out.n_rows,
                        "ok": out.ok,
                        "error": out.error,
                        "round": rnd + 1,
                    }
            if not final_text.strip():
                final_text, run.cut_off = CUT_OFF_TEXT, True
            span.set(rounds=run.rounds, tool_calls=len(self.outcomes), cut_off=run.cut_off)

        run.answer, run.outcomes = final_text, list(self.outcomes)
        for piece in chunk_text(final_text):
            yield {"type": "delta", "text": piece}
        yield {"type": "result", "run": run}
