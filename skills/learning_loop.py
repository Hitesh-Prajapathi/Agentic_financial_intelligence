"""Learning loop: the system measures itself and improves its sentiment labels. Safety-gated.

SAFETY CONTRACT
  Runs only after a pipeline run PASSED the governance gate, and every read of run history is filtered in
  SQL to status IN ('success','partial'). Quarantined / failed runs never reach this module.

What it actually does (all deterministic apart from the re-check LLM call):
  1. review_low_confidence : tweets labelled with confidence < threshold get an independent second opinion from a
     *different* model. Agreement → confirmed (confidence raised); disagreement → label set to neutral
     (we don't act on contested signals) and flagged reviewed.
  2. distill_examples      : labels two models agreed on with high confidence become few-shot examples that are
     prepended to the sentiment prompt on later runs (capped, label-balanced, to limit drift).
  3. metrics               : classification confidence, review agreement, answer feedback and retrieval-confidence
     mix, surfaced on the Health tab.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

MAX_EXAMPLES_PER_LABEL = 2
EXAMPLE_MIN_CONF = 0.85


class LearningLoop:
    def __init__(self, db_tool, llm_tool, sentiment_agent, settings):
        self.db, self.llm, self.sentiment, self.settings = db_tool, llm_tool, sentiment_agent, settings

    # ── gate ────────────────────────────────────────────────────────────────
    def approved_runs(self, limit: int = 10) -> list[dict]:
        rows = self.db.query(
            """SELECT run_id, started_at, completed_at, status, filings_ingested, trades_ranked, tweets_scraped,
                      tweets_classified, documents_indexed
               FROM pipeline_runs WHERE status IN ('success', 'partial')
               ORDER BY completed_at DESC LIMIT ?""", [limit])
        keys = ["run_id", "started_at", "completed_at", "status", "filings_ingested", "trades_ranked",
                "tweets_scraped", "tweets_classified", "documents_indexed"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 1. second opinions ──────────────────────────────────────────────────
    def _second_opinion_models(self) -> list[tuple[str, str]]:
        chain = self.llm.chain()
        return chain[1:2] or chain[:1]  # a different model than the primary when one exists

    def review_low_confidence(self, max_tweets: int = 30) -> dict:
        rows = self.db.query(
            """SELECT s.tweet_id, s.ticker, t.text, s.sentiment_label, s.confidence_score
               FROM tweet_sentiments s JOIN tweets t USING (tweet_id)
               WHERE s.confidence_score < ? AND NOT s.reviewed
               ORDER BY s.confidence_score ASC LIMIT ?""", [self.settings.low_confidence_threshold, max_tweets])
        if not rows:
            return {"reviewed": 0, "confirmed": 0, "changed": 0}
        tweets = [{"tweet_id": r[0], "ticker": r[1], "text": r[2]} for r in rows]
        old = {r[0]: (r[3], r[4]) for r in rows}
        new_rows, _ = self.sentiment.classify(tweets, models=self._second_opinion_models())

        updates, confirmed, changed = [], 0, 0
        for n in new_rows:
            prev_label, prev_conf = old[n["tweet_id"]]
            if n["sentiment_label"] == prev_label:
                confirmed += 1
                updates.append({**n, "confidence_score": max(prev_conf, n["confidence_score"]), "reviewed": True,
                                "model": f"{n['model']} (confirmed)"})
            else:  # contested → don't act on it
                changed += 1
                updates.append({**n, "sentiment_label": "neutral", "confidence_score": 0.5, "reviewed": True,
                                "model": f"{n['model']} (contested→neutral)"})
        if updates:
            self.db.insert_sentiments(updates)
            self.db.refresh_sentiment_summary(sorted({u["ticker"] for u in updates}))
        return {"reviewed": len(updates), "confirmed": confirmed, "changed": changed}

    # ── 2. few-shot distillation ────────────────────────────────────────────
    def distill_examples(self) -> dict:
        added = 0
        for label in ("bullish", "bearish", "neutral"):
            have = self.db.query("SELECT COUNT(*) FROM learned_examples WHERE label = ?", [label])[0][0]
            if have >= MAX_EXAMPLES_PER_LABEL:
                continue
            rows = self.db.query(
                """SELECT s.tweet_id, t.text, s.confidence_score FROM tweet_sentiments s JOIN tweets t USING (tweet_id)
                   WHERE s.reviewed AND s.sentiment_label = ? AND s.confidence_score >= ? AND s.model LIKE '%(confirmed)'
                     AND s.tweet_id NOT IN (SELECT tweet_id FROM learned_examples) AND length(t.text) BETWEEN 30 AND 220
                   ORDER BY s.confidence_score DESC LIMIT ?""", [label, EXAMPLE_MIN_CONF, MAX_EXAMPLES_PER_LABEL - have])
            for tid, text, conf in rows:
                self.db.execute("INSERT INTO learned_examples (tweet_id, text, label, confidence) VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
                                [tid, text, label, conf])
                added += 1
        return {"examples_added": added}

    # ── 3. metrics ──────────────────────────────────────────────────────────
    def metrics(self) -> dict:
        q = self.db.query
        n, avg_conf, low = q("""SELECT COUNT(*), AVG(confidence_score), SUM(CASE WHEN confidence_score < ? THEN 1 ELSE 0 END)
                                FROM tweet_sentiments""", [self.settings.low_confidence_threshold])[0]
        rev, conf_rev = q("SELECT SUM(CASE WHEN reviewed THEN 1 ELSE 0 END), SUM(CASE WHEN model LIKE '%(confirmed)' THEN 1 ELSE 0 END) FROM tweet_sentiments")[0]
        fb = q("SELECT SUM(CASE WHEN feedback > 0 THEN 1 ELSE 0 END), SUM(CASE WHEN feedback < 0 THEN 1 ELSE 0 END) FROM query_log")[0]
        lat = q("SELECT COUNT(*), AVG(latency_ms), quantile_cont(latency_ms, 0.9) FROM query_log")[0]
        mix = dict(q("SELECT confidence, COUNT(*) FROM query_log GROUP BY confidence"))
        return {
            "tweets_classified": int(n or 0), "avg_confidence": float(avg_conf or 0), "low_confidence": int(low or 0),
            "reviewed": int(rev or 0), "review_agreement": (float(conf_rev) / float(rev)) if rev else None,
            "thumbs_up": int(fb[0] or 0), "thumbs_down": int(fb[1] or 0),
            "questions": int(lat[0] or 0), "avg_latency_ms": float(lat[1] or 0), "p90_latency_ms": float(lat[2] or 0),
            "retrieval_confidence_mix": mix, "examples": q("SELECT COUNT(*) FROM learned_examples")[0][0],
            "approved_runs": len(self.approved_runs(50)),
        }

    def run(self) -> dict:
        """Called by the pipeline only after the governance gate passed."""
        out = {}
        try:
            out.update(self.review_low_confidence())
        except Exception as e:  # noqa: BLE001  never let learning break the run
            logger.warning("Learning loop review failed: %s", e)
            out["review_error"] = str(e)[:120]
        out.update(self.distill_examples())
        return out
