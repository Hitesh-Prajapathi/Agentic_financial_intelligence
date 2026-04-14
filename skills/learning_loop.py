"""
Hermes Learning Loop — Safety-Gated Knowledge Distillation

SAFETY CONTRACT:
  This module ONLY reads pipeline_runs WHERE status = 'success'.
  Quarantined or partial-failure runs are NEVER processed here.
  This is enforced at the SQL query level — never by application logic alone.
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class LearningLoop:
    def __init__(self, db_tool, lightrag_tool):
        self.db_tool = db_tool
        self.lightrag_tool = lightrag_tool

    def get_approved_runs(self, limit: int = 10) -> list[dict]:
        """
        SAFETY-GATED query: only returns runs with status = 'success'.
        Quarantined runs are filtered at SQL level — not application level.
        """
        rows = self.db_tool.query(
            """
            SELECT run_id, started_at, completed_at,
                   filings_ingested, trades_ranked,
                   tweets_scraped, tweets_classified, documents_indexed
            FROM pipeline_runs
            WHERE status = 'success'
            ORDER BY completed_at DESC
            LIMIT ?
            """,
            [limit]
        )
        runs = [
            {
                "run_id": r[0],
                "started_at": r[1],
                "completed_at": r[2],
                "filings_ingested": r[3],
                "trades_ranked": r[4],
                "tweets_scraped": r[5],
                "tweets_classified": r[6],
                "documents_indexed": r[7],
            }
            for r in rows
        ]
        logger.info(f"[LearningLoop] Found {len(runs)} approved (success) pipeline runs.")
        return runs

    def quarantine_run(self, run_id: str, reason: str, db_tool=None) -> None:
        """
        Mark a pipeline run as quarantined. It will never be surfaced by
        get_approved_runs() because that query filters for status='success'.
        """
        target_db = db_tool or self.db_tool
        completed_at = datetime.now(timezone.utc)
        target_db.query(
            """
            UPDATE pipeline_runs
            SET status = 'quarantined',
                completed_at = ?,
                error_message = ?
            WHERE run_id = ?
            """,
            [completed_at, reason, run_id]
        )
        logger.warning(f"[LearningLoop] Run {run_id} QUARANTINED. Reason: {reason}")

    def distill(self, limit: int = 5) -> dict:
        """
        Process approved runs: summarise what was learned and (optionally)
        push distilled text blocks back into LightRAG for future retrieval.
        """
        approved = self.get_approved_runs(limit=limit)
        if not approved:
            logger.info("[LearningLoop] No approved runs to distill.")
            return {"distilled": 0}

        blocks = []
        for run in approved:
            block = (
                f"PIPELINE RUN {run['run_id']} (completed {run['completed_at']}):\n"
                f"  - Filings ingested:    {run['filings_ingested']}\n"
                f"  - Trades ranked:       {run['trades_ranked']}\n"
                f"  - Tweets scraped:      {run['tweets_scraped']}\n"
                f"  - Tweets classified:   {run['tweets_classified']}\n"
                f"  - Documents indexed:   {run['documents_indexed']}\n"
            )
            blocks.append(block)

        if blocks and self.lightrag_tool:
            self.lightrag_tool.insert_texts(blocks)
            logger.info(f"[LearningLoop] Distilled {len(blocks)} run summaries into LightRAG.")

        return {"distilled": len(blocks)}
