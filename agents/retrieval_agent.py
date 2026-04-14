import logging
from config.settings import settings
from telemetry.otel_setup import tracer
import json

class RetrievalAgent:
    def __init__(self, llm_tool, lightrag_tool, db_tool=None, settings=settings):
        self.llm_tool = llm_tool
        self.lightrag_tool = lightrag_tool
        self.db_tool = db_tool
        self.settings = settings
        self.run_id = None
        self.logger = logging.getLogger(__name__)

    def determine_mode(self, user_query: str) -> str:
        with tracer.start_as_current_span("retrieval.determine_mode") as span:
            span.set_attribute("query", user_query)
            
            try:
                with open("config/prompts/retrieval_agent.txt", "r") as f:
                    system_prompt = f.read()
                    
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_query}
                ]
                
                response_json = self.llm_tool.call_llm_structured(
                    messages=messages, 
                    model=self.settings.openrouter_model_classify,
                    response_format={"type": "json_object"}
                )
                
                mode = response_json.get('mode', 'hybrid').lower()
                if mode not in ['local', 'global', 'hybrid']:
                    mode = 'hybrid'
                    
                span.set_attribute("selected_mode", mode)
                return mode
                
            except Exception as e:
                self.logger.warning(f"Failed to determine retrieval mode, defaulting to hybrid: {e}")
                span.set_attribute("error", str(e))
                return "hybrid"

    def run(self, user_query: str, force_mode: str = None) -> dict:
        with tracer.start_as_current_span("retrieval_agent.run") as span:
            span.set_attribute("agent.name", "RetrievalAgent")
            if self.run_id:
                span.set_attribute("agent.run_id", self.run_id)
            
            mode = force_mode if force_mode else self.determine_mode(user_query)
            span.set_attribute("query.mode", mode)
            
            try:
                self.logger.info(f"Querying LightRAG for '{user_query}' using mode: {mode}")
                context = self.lightrag_tool.query(user_query, mode=mode)
                
                # Basic DB Context addition
                db_context = "No specific DB matches found."
                if self.db_tool:
                    try:
                        top_trades = self.db_tool.query_df("SELECT * FROM top_trades ORDER BY run_date DESC LIMIT 5")
                        if not top_trades.empty:
                            db_context = "Top Recent Trades:\n" + top_trades.to_csv(index=False)
                    except Exception as e:
                        self.logger.warning(f"Could not load DB context: {e}")
                
                return {
                    "mode_used": mode,
                    "rag_context": context,
                    "db_context": db_context,
                    "confidence": "high" if len(context) > 50 else "low",
                    "intent": {"needs_chart": "chart" in user_query.lower()}
                }
            except Exception as e:
                self.logger.error(f"Retrieval failed: {e}")
                span.set_attribute("error", str(e))
                return {
                    "mode_used": mode,
                    "rag_context": f"Failed to retrieve context: {str(e)}",
                    "db_context": "",
                    "confidence": "low",
                    "intent": {}
                }
