"""Stage 4: classify unlabelled tweets with the free-model chain, with strict output validation."""
import json
import logging
import re
from pathlib import Path

from telemetry.otel_setup import tracer
from tools.llm_tool import LLMError

logger = logging.getLogger(__name__)
PROMPT_PATH = Path(__file__).resolve().parent.parent / "config" / "prompts" / "sentiment_agent.txt"
LABELS = {"bullish", "bearish", "neutral"}
_URL_RE = re.compile(r"https?://\S+")
MAX_PENDING = 400  # cap work (and LLM requests) per run


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


class SentimentAgent:
    def __init__(self, llm_tool, db_tool, settings):
        self.llm, self.db, self.settings = llm_tool, db_tool, settings
        self._prompt = PROMPT_PATH.read_text()

    def _system_prompt(self) -> str:
        """Base prompt + few-shot examples distilled by the learning loop (labels two models agreed on)."""
        try:
            ex = self.db.query("SELECT text, label FROM learned_examples ORDER BY label, confidence DESC LIMIT 6")
        except Exception:  # noqa: BLE001
            ex = []
        if not ex:
            return self._prompt
        shots = "\n".join(f'- "{clean_tweet(t)[:200]}" -> {l}' for t, l in ex)
        return f"{self._prompt}\nExamples of correctly labelled posts:\n{shots}\n"

    def classify(self, tweets: list[dict], models: list[tuple[str, str]] | None = None) -> tuple[list[dict], dict]:
        """tweets: [{tweet_id, ticker, text}] → (labelled rows, meta). Missing/invalid answers are left unlabelled."""
        rows, meta = [], {"batches": 0, "batches_failed": 0, "models": {}, "errors": []}
        bs = self.settings.sentiment_batch_size
        prompt = self._system_prompt()
        for i in range(0, len(tweets), bs):
            batch = tweets[i:i + bs]
            payload = json.dumps([{"i": j + 1, "t": clean_tweet(t["text"])} for j, t in enumerate(batch)])
            meta["batches"] += 1
            try:
                resp, r = self.llm.chat_json(
                    [{"role": "system", "content": prompt}, {"role": "user", "content": payload}],
                    max_tokens=2500, purpose="sentiment", models=models)
            except LLMError as e:
                meta["batches_failed"] += 1
                meta["errors"].append(str(e)[:160])
                logger.warning("Sentiment batch failed: %s", e)
                continue
            meta["models"][r.label] = meta["models"].get(r.label, 0) + 1
            for idx, (label, conf) in parse_batch(resp, len(batch)).items():
                t = batch[idx - 1]
                rows.append({"tweet_id": t["tweet_id"], "ticker": t["ticker"], "sentiment_label": label,
                             "confidence_score": conf, "model": r.label})
        return rows, meta

    def run(self, ticker: str | None = None) -> dict:
        with tracer.start_as_current_span("sentiment_agent.run") as span:
            sql = """SELECT tweet_id, ticker, text FROM tweets
                     WHERE tweet_id NOT IN (SELECT tweet_id FROM tweet_sentiments)"""
            params = []
            if ticker:
                sql += " AND ticker = ?"
                params.append(ticker)
            sql += f" ORDER BY created_at DESC LIMIT {MAX_PENDING}"
            pending = [{"tweet_id": r[0], "ticker": r[1], "text": r[2]} for r in self.db.query(sql, params)]
            if not pending:
                return {"tweets_classified": 0, "tweets_pending": 0, "batches_failed": 0, "low_confidence": 0,
                        "models": {}, "errors": []}

            rows, meta = self.classify(pending)
            if rows:
                self.db.insert_sentiments(rows)
                self.db.refresh_sentiment_summary(sorted({r["ticker"] for r in rows}))
            low = sum(1 for r in rows if r["confidence_score"] < self.settings.low_confidence_threshold)
            span.set_attribute("tweets.classified", len(rows))
            return {"tweets_classified": len(rows), "tweets_pending": len(pending) - len(rows),
                    "batches_failed": meta["batches_failed"], "low_confidence": low, "models": meta["models"],
                    "errors": meta["errors"]}
