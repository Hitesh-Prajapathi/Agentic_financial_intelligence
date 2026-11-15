"""Post sentiment classifier: pure functions, no database. Not a pipeline stage (re-wired to a new source in E3).

Output is validated strictly: only well-formed `bullish|bearish|neutral` entries survive; anything the model
skipped or mangled stays unclassified (no fabricated defaults). Tweets are untrusted data.
"""

import json
import logging
import re

from insider_intel.llm import load_prompt
from insider_intel.llm.client import LLMError

logger = logging.getLogger(__name__)
LABELS = {"bullish", "bearish", "neutral"}
_URL_RE = re.compile(r"https?://\S+")
DEFAULT_BATCH_SIZE = 30


def clean_tweet(text: str) -> str:
    return re.sub(r"\s+", " ", _URL_RE.sub("", text or "")).strip()[:280]


def parse_batch(response: dict, n: int) -> dict[int, tuple[str, float]]:
    """Validate a model response: only well-formed entries survive; anything else stays unclassified."""
    out = {}
    for k, v in (response or {}).items():
        try:
            idx = int(k)
            label = str(v["label"]).strip().lower()
            conf = max(0.0, min(1.0, float(v.get("confidence", 0.5))))
        except (ValueError, KeyError, TypeError, AttributeError):
            continue
        if 1 <= idx <= n and label in LABELS:
            out[idx] = (label, conf)
    return out


def classify(
    llm,
    tweets: list[dict],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    models: list[tuple[str, str]] | None = None,
    prompt: str | None = None,
) -> tuple[list[dict], dict]:
    """tweets: [{tweet_id, ticker, text}] -> (labelled rows, meta). Missing/invalid answers are left unlabelled."""
    prompt = prompt if prompt is not None else load_prompt("sentiment_agent")
    rows, meta = [], {"batches": 0, "batches_failed": 0, "models": {}, "errors": []}
    for i in range(0, len(tweets), batch_size):
        batch = tweets[i : i + batch_size]
        payload = json.dumps([{"i": j + 1, "t": clean_tweet(t["text"])} for j, t in enumerate(batch)])
        meta["batches"] += 1
        try:
            resp, r = llm.chat_json(
                [{"role": "system", "content": prompt}, {"role": "user", "content": payload}],
                max_tokens=2500,
                purpose="sentiment",
                models=models,
            )
        except LLMError as e:
            meta["batches_failed"] += 1
            meta["errors"].append(str(e)[:160])
            logger.warning("Sentiment batch failed: %s", e)
            continue
        meta["models"][r.label] = meta["models"].get(r.label, 0) + 1
        for idx, (label, conf) in parse_batch(resp, len(batch)).items():
            t = batch[idx - 1]
            rows.append(
                {
                    "tweet_id": t["tweet_id"],
                    "ticker": t["ticker"],
                    "sentiment_label": label,
                    "confidence_score": conf,
                    "model": r.label,
                }
            )
    return rows, meta
