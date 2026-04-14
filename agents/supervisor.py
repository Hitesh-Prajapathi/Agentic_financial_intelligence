import logging
import uuid
from datetime import datetime, timezone
from telemetry.otel_setup import tracer
from skills.integrity_checks import run_all_checks
from skills.learning_loop import LearningLoop

class SupervisorAgent:
    def __init__(self, sec_data_agent, ranking_agent, social_scraper_agent, 
                 sentiment_agent, indexing_agent, retrieval_agent, 
                 chat_agent, visualization_agent, db_tool):
        self.sec_data_agent = sec_data_agent
        self.ranking_agent = ranking_agent
        self.social_scraper_agent = social_scraper_agent
        self.sentiment_agent = sentiment_agent
        self.indexing_agent = indexing_agent
        self.retrieval_agent = retrieval_agent
        self.chat_agent = chat_agent
        self.visualization_agent = visualization_agent
        self.db_tool = db_tool
        
        self.logger = logging.getLogger(__name__)

    def run_ingestion_pipeline(self) -> dict:
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        
        with tracer.start_as_current_span("pipeline.ingestion") as pipeline_span:
            pipeline_span.set_attribute("pipeline.run_id", run_id)
            pipeline_span.set_attribute("pipeline.started_at", started_at.isoformat())
            
            try:
                # Store initial run status
                self.db_tool.query(
                    "INSERT INTO pipeline_runs (run_id, started_at, status) VALUES (?, ?, ?)",
                    [run_id, started_at, "running"]
                )
                
                # Step 1: SEC
                sec_result = self.sec_data_agent.run()
                
                # Step 2: Ranking
                rank_result = self.ranking_agent.run()
                
                # Step 3: Social Scraper
                # Get tickers from ranked trades
                top_trades = rank_result.get("top_trades", [])
                social_result = self.social_scraper_agent.run(top_trades)
                
                # Step 4: Sentiment
                sentiment_result = self.sentiment_agent.run()
                
                # Step 5: Indexing
                index_result = self.indexing_agent.run()
                
                completed_at = datetime.now(timezone.utc)

                # ── Governance Gate ──────────────────────────────────────────
                # Run ALL integrity checks before marking the run as success.
                # If ANY check fails, quarantine the run so the Learning Loop
                # never consumes corrupted data.
                raw_filings   = sec_result.get("raw_filings", [])
                db_filings    = sec_result.get("db_filings", [])
                raw_txns      = sec_result.get("raw_transactions", [])
                ranked_df     = rank_result.get("ranked_df")
                checksum      = rank_result.get("checksum", "empty")

                passed, failed_checks = run_all_checks(
                    raw_filings, db_filings, raw_txns, ranked_df, checksum
                )

                if not passed:
                    reason = f"Integrity checks failed: {failed_checks}"
                    self.logger.error(f"[Governance] {reason} — quarantining run {run_id}")
                    pipeline_span.set_attribute("pipeline.status", "quarantined")
                    pipeline_span.set_attribute("pipeline.failed_checks", str(failed_checks))
                    LearningLoop(self.db_tool, None).quarantine_run(run_id, reason)
                    raise RuntimeError(reason)
                # ─────────────────────────────────────────────────────────────

                # Update pipeline status to success
                self.db_tool.query(
                    """
                    UPDATE pipeline_runs 
                    SET status = ?, completed_at = ?, filings_ingested = ?, 
                        trades_ranked = ?, tweets_scraped = ?, tweets_classified = ?, 
                        documents_indexed = ?
                    WHERE run_id = ?
                    """,
                    [
                        "success", completed_at, sec_result.get("filings_ingested", 0),
                        len(top_trades), social_result.get("total_tweets", 0),
                        sentiment_result.get("tweets_classified", 0),
                        index_result.get("indexed_blocks", 0), run_id
                    ]
                )
                
                pipeline_span.set_attribute("pipeline.status", "success")
                pipeline_span.set_attribute("pipeline.total_filings", sec_result.get("filings_ingested", 0))
                pipeline_span.set_attribute("pipeline.total_tweets", social_result.get("total_tweets", 0))
                
                return {
                    "status": "success",
                    "run_id": run_id,
                    "sec_result": sec_result,
                    "ranking_result": rank_result,
                    "social_result": social_result,
                    "sentiment_result": sentiment_result,
                    "indexing_result": index_result
                }
                
            except Exception as e:
                self.logger.error(f"Pipeline failed: {e}")
                pipeline_span.set_attribute("pipeline.status", "failed")
                pipeline_span.set_attribute("error", str(e))
                
                completed_at = datetime.now(timezone.utc)
                # Ensure the failure is recorded
                try:
                    self.db_tool.query(
                        "UPDATE pipeline_runs SET status = ?, completed_at = ? WHERE run_id = ?",
                        ["partial_failure", completed_at, run_id]
                    )
                except Exception as db_err:
                    self.logger.error(f"Failed to record pipeline failure to DB: {db_err}")
                    
                raise

    def handle_query(self, user_message: str) -> dict:
        with tracer.start_as_current_span("supervisor.handle_query") as span:
            span.set_attribute("query", user_message)
            
            try:
                # Fast path: chart requests bypass everything
                is_chart_request = any(word in user_message.lower() for word in ["chart", "graph", "plot", "visualize", "draw"])
                
                if is_chart_request:
                    viz_intent = {"query_type": "summary", "tickers": []}
                    viz_result = self.visualization_agent.run(viz_intent)
                    chart_path = viz_result.get("chart_path")
                    
                    if chart_path:
                        return {
                            "response": "Here is the chart of the top insider trades:",
                            "has_citations": True,
                            "confidence": "high",
                            "chart_path": chart_path
                        }
                    else:
                        return {
                            "response": "No trade data available to generate a chart. Try loading demo data first.",
                            "has_citations": False,
                            "confidence": "low",
                            "chart_path": None
                        }
                
                # Normal path: pull context from DB directly (no LightRAG query)
                db_context = self._build_db_context()
                
                context_bundle = {
                    "rag_context": "",
                    "db_context": db_context,
                    "confidence": "high" if len(db_context) > 50 else "low"
                }
                
                chat_result = self.chat_agent.run(context_bundle, user_message)
                    
                return {
                    "response": chat_result.get("response", ""),
                    "has_citations": chat_result.get("has_citations", False),
                    "confidence": chat_result.get("confidence", "low"),
                    "chart_path": None
                }
                
            except Exception as e:
                self.logger.error(f"Query handling failed: {e}")
                span.set_attribute("error", str(e))
                return {
                    "response": f"I'm sorry, an error occurred while processing your query: {e}",
                    "has_citations": False,
                    "confidence": "low",
                    "chart_path": None
                }

    def _build_db_context(self) -> str:
        """Pull all relevant context directly from DuckDB — fast and reliable."""
        parts = []
        
        try:
            trades_df = self.db_tool.query_df(
                "SELECT rank, run_date, issuer_ticker, insider_name, net_dollar_value, transaction_count, dominant_direction FROM top_trades ORDER BY run_date DESC, rank ASC LIMIT 10"
            )
            if not trades_df.empty:
                parts.append("=== TOP INSIDER TRADES ===\n" + trades_df.to_string(index=False))
        except Exception:
            pass
        
        try:
            sent_df = self.db_tool.query_df(
                "SELECT ticker, summary_date, total_tweets, bullish_count, bearish_count, neutral_count, sentiment_index FROM sentiment_summary ORDER BY summary_date DESC LIMIT 10"
            )
            if not sent_df.empty:
                parts.append("\n=== SENTIMENT SUMMARY ===\n" + sent_df.to_string(index=False))
        except Exception:
            pass
        
        try:
            filings_df = self.db_tool.query_df(
                "SELECT issuer_ticker, issuer_name, insider_name, insider_title, filing_date FROM filings ORDER BY filing_date DESC LIMIT 10"
            )
            if not filings_df.empty:
                parts.append("\n=== RECENT SEC FILINGS ===\n" + filings_df.to_string(index=False))
        except Exception:
            pass
            
        return "\n".join(parts) if parts else "No data available. Please run the pipeline or load demo data first."
