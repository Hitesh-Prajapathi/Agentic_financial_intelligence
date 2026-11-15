"""Grounding checks for chat answers (plan B3, "Grounding v2").

Three independent, deterministic checks run on every agent answer:

(a) citations: every ``[RANK ...]`` / ``[SEC Form 4 ...]`` / ``[SENT ...]`` / ``[GRAPH ...]`` tag in the answer must be
    a source id that one of *this turn's* tool results actually returned;
(b) numeric grounding: every number the answer states (after normalising ``$``, ``%``, thousands separators and
    K/M/B/million/billion) must match a number present in the tool outputs (or in the question / tool arguments),
    within rounding. Years, dates, accession numbers, rank markers (``#1``), ordinals and form names are ignored;
(c) abstention: if every tool came back empty and the answer still states facts, that is flagged.

Known limits (documented, tested): a number that happens to coincide with an unrelated tool value is "supported"
(false negative); derived numbers (sums, differences) are reported as unsupported by design, since the model is told
not to do arithmetic (false positive when it does it correctly); plain integers below 1,000 must match exactly.
"""

from __future__ import annotations

import bisect
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

# ── citations ────────────────────────────────────────────────────────────────
CITATION_PREFIXES = ("SEC", "RANK", "SENT", "RUN", "GRAPH", "CLUSTER")
_DASHES = {ord(c): "-" for c in "‐‑‒–—−"}
_CITE_RE = re.compile(r"\[((?:" + "|".join(CITATION_PREFIXES) + r")[^\]]*)\]")


def _normalise(text: str) -> str:
    return (text or "").translate(_DASHES).replace("*", "").replace("`", "").replace("【", "[").replace("】", "]")


def citation_tags(text: str) -> set[str]:
    """Source tags like [RANK 2026-11-05 #1]. Tolerates markdown bold/code and unicode hyphens that models emit."""
    return {re.sub(r"\s+", " ", t.strip()) for t in _CITE_RE.findall(_normalise(text))}


def collect_sources(obj: Any) -> set[str]:
    """All stable source ids (``"source"`` keys) found anywhere inside a tool result."""
    out: set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "source" and isinstance(v, str):
                out.add(re.sub(r"\s+", " ", v.strip().strip("[]")))
            else:
                out |= collect_sources(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            out |= collect_sources(v)
    return out


# ── number extraction ────────────────────────────────────────────────────────
_MONTHS = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
_IGNORE = [
    re.compile(r"\b\d{10}-\d{2}-\d{6}\b"),  # accession numbers
    re.compile(r"\bDEMO-\d+-\d+\b"),  # demo accession numbers
    re.compile(r"\b\d{4}-\d{1,2}-\d{1,2}\b"),  # ISO dates
    re.compile(r"\b\d{4}-\d{2}\b"),  # year-month
    re.compile(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b"),  # 11/05, 11/05/2026 (and 4/A style is below)
    re.compile(rf"\b{_MONTHS}\.?\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,?\s+\d{{4}})?\b", re.I),  # Nov 5, November 5th, 2026
    re.compile(rf"\b\d{{1,2}}(?:st|nd|rd|th)?\s+{_MONTHS}\b(?:\s+\d{{4}})?", re.I),  # 5 Nov 2026
    re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?\b"),  # times
    re.compile(r"\b10b5-1\b|\b10b5\b", re.I),
    re.compile(
        r"\bForms?\s+\d(?:\s*/\s*[A-Z])?(?:\s*(?:,|and|or|&)\s*\d)*\b", re.I
    ),  # Form 4, Form 4/A, Forms 3, 4 and 5
    re.compile(
        r"\b(?:10-[KQ]|8-K|S-\d|13[DGF]|Schedule\s+13[DG]|Rule\s+\d+[a-z0-9\-]*|Section\s+16(?:\([a-z]\))?)\b", re.I
    ),
    re.compile(r"\b(?:Q[1-4]|H[12]|FY\s?\d{2,4})\b"),  # fiscal periods
    re.compile(r"\bS&P\s*\d+\b", re.I),  # S&P 500
    re.compile(r"#\s?\d+"),  # rank markers
    re.compile(r"\b(?:rank(?:ed)?|no\.|number)\s*#?\s*\d+\b", re.I),
    re.compile(r"\b\d+(?:st|nd|rd|th)\b", re.I),  # ordinals
    re.compile(r"^\s*\d+[.)]\s", re.M),  # numbered-list markers
]

_NUM_RE = re.compile(
    r"(?<![\w.])(?P<int>\d{1,3}(?:,\d{3})+|\d+)(?P<frac>\.\d+)?"
    r"(?:(?P<pct>\s?%|\s?percent\b)|(?P<scale>\s?(?:thousand|million|billion|trillion|mln|bn|mm)\b|[kKmMbB]\b))?",
    re.I,
)
_SCALES = {
    "k": 1e3,
    "thousand": 1e3,
    "m": 1e6,
    "million": 1e6,
    "mln": 1e6,
    "mm": 1e6,
    "b": 1e9,
    "billion": 1e9,
    "bn": 1e9,
    "trillion": 1e12,
}


@dataclass(frozen=True)
class Number:
    raw: str  # as written in the text, e.g. "$17.4M"
    value: float  # magnitude after applying K/M/B (sign is ignored on purpose)
    kind: str  # 'usd' | 'pct' | 'plain'
    half_unit: float  # half a unit of the displayed precision, in absolute terms
    loose: bool  # True when rounding tolerance applies (has $, %, comma, decimals or a K/M/B scale)


def _blank(text: str) -> str:
    """Remove everything that looks like a number but is a date / id / rank / form name / citation tag."""
    text = _CITE_RE.sub(" ", text)
    for rx in _IGNORE:
        text = rx.sub(lambda m: " " * len(m.group(0)), text)
    return text


def extract_numbers(text: str) -> list[Number]:
    clean = _blank(_normalise(text))
    out: list[Number] = []
    for m in _NUM_RE.finditer(clean):
        int_part, frac = m.group("int"), m.group("frac") or ""
        has_unit = bool(m.group("pct") or m.group("scale"))
        before = clean[max(0, m.start() - 4) : m.start()]
        pre = re.search(r"\$\s*[-+]?\s*$", before)
        usd = bool(pre)
        if not has_unit and not usd and not frac and "," not in int_part and 1900 <= int(int_part) <= 2100:
            continue  # a bare year
        mant = float(int_part.replace(",", "") + frac)
        decimals = len(frac) - 1 if frac else 0
        mult = 1.0
        if m.group("scale"):
            mult = _SCALES[m.group("scale").strip().lower()]
        kind = "pct" if m.group("pct") else ("usd" if usd else "plain")
        loose = has_unit or usd or bool(frac) or "," in int_part
        raw = (pre.group(0).replace(" ", "") if pre else "") + m.group(0).strip()
        out.append(Number(raw=raw, value=mant * mult, kind=kind, half_unit=0.5 * 10 ** (-decimals) * mult, loose=loose))
    return out


# ── numbers present in the evidence ──────────────────────────────────────────
def _leaves(obj: Any) -> Iterable[float]:
    if isinstance(obj, bool):
        return
    if isinstance(obj, (int, float)):
        yield abs(float(obj))
    elif isinstance(obj, str):
        s = obj.strip().replace(",", "").lstrip("$")
        try:
            yield abs(float(s))
        except ValueError:
            pass
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if k == "source":
                continue
            yield from _leaves(v)
    elif isinstance(obj, (list, tuple, set)):
        for v in obj:
            yield from _leaves(v)


class NumberPool:
    """Sorted bag of 'known' values; supports tolerance queries in O(log n)."""

    def __init__(self, values: Iterable[float] = ()):
        self._vals = sorted(v for v in values if v == v)

    def add(self, values: Iterable[float]) -> None:
        self._vals = sorted(self._vals + [v for v in values if v == v])

    def _hit(self, lo: float, hi: float) -> bool:
        i = bisect.bisect_left(self._vals, lo)
        return i < len(self._vals) and self._vals[i] <= hi

    def supports(self, n: Number) -> bool:
        v = n.value
        tol = max(0.005 * v, n.half_unit) if n.loose else 1e-6
        if self._hit(v - tol, v + tol):
            return True
        if n.kind == "pct":  # the DB stores fractions (0.43) where the answer says 43%
            return self._hit((v - tol) / 100, (v + tol) / 100)
        return False


def build_pool(
    outcomes: list, question: str = "", history: list[dict] | None = None, evidence_text: str = ""
) -> NumberPool:
    vals: list[float] = [n.value for n in extract_numbers(evidence_text)] if evidence_text else []
    for o in outcomes:
        result, args = _get(o, "result"), _get(o, "args")
        vals += list(_leaves(result)) + list(_leaves(args))
        if isinstance(result, dict) and isinstance(result.get("rows"), list):
            vals.append(float(len(result["rows"])))
    vals += [n.value for n in extract_numbers(question)]
    for m in history or []:
        if m.get("content"):
            vals += [n.value for n in extract_numbers(m["content"])]
    return NumberPool(vals)


def _get(o, key):
    return o.get(key) if isinstance(o, dict) else getattr(o, key, None)


# ── abstention ───────────────────────────────────────────────────────────────
_ABSTAIN_RE = re.compile(
    r"(don'?t|do not|doesn'?t|does not) (currently )?have (any )?(data|information|records)|no data|not available|"
    r"(couldn'?t|could not|can'?t|cannot|unable to|did not|didn'?t) (find|locate|answer|retrieve)|"
    r"no (matching |relevant )?(results|filings|records|trades|transactions|rows|clusters|insiders?)|nothing (matches|found)|"
    r"not (covered|in the data)|outside (the scope|what)",
    re.I,
)


def is_abstention(answer: str) -> bool:
    return bool(_ABSTAIN_RE.search(answer or ""))


def _is_empty(result: Any) -> bool:
    if not isinstance(result, dict):
        return True
    if result.get("available") is False or result.get("error"):
        return True
    rows = result.get("rows")
    return not rows


# ── main entry ───────────────────────────────────────────────────────────────
def check_grounding(
    answer: str,
    outcomes: list,
    question: str = "",
    history: list[dict] | None = None,
    evidence_text: str = "",
    extra_sources: Iterable[str] = (),
) -> dict:
    """``outcomes``: this turn's tool results (ToolOutcome objects or dicts with name/args/result/ok).
    Degraded mode has no tool results: it passes the SQL context as ``evidence_text`` and its tags as ``extra_sources``."""
    known: set[str] = set(extra_sources)
    for o in outcomes:
        known |= collect_sources(_get(o, "result"))
    cited = citation_tags(answer)
    valid = sorted(cited & known)
    invalid = sorted(cited - known)

    pool = build_pool(outcomes, question, history, evidence_text)
    nums = extract_numbers(answer)
    supported, unsupported = [], []
    for n in nums:
        (supported if pool.supports(n) else unsupported).append(n.raw)
    supported = list(dict.fromkeys(supported))
    unsupported = list(dict.fromkeys(unsupported))

    abstained = is_abstention(answer)
    all_empty = (not evidence_text.strip()) if evidence_text else all(_is_empty(_get(o, "result")) for o in outcomes)
    states_facts = bool(nums or cited)
    return {
        "supported": supported,
        "unsupported": unsupported,
        "citations_valid": valid,
        "citations_invalid": invalid,
        "abstained": abstained,
        "empty_but_asserted": bool(all_empty and states_facts and not abstained),
        "n_numbers": len(nums),
        "n_tool_calls": len(outcomes),
    }
