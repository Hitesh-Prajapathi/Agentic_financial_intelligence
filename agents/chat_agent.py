import logging
import re
from config.settings import settings
from telemetry.otel_setup import tracer

class ChatAgent:
    def __init__(self, llm_tool, settings=settings):
        self.llm_tool = llm_tool
        self.settings = settings
        self.run_id = None
        self.logger = logging.getLogger(__name__)

    def run(self, context_bundle: dict, user_question: str) -> dict:
        with tracer.start_as_current_span("chat_agent.run") as span:
            span.set_attribute("agent.name", "ChatAgent")
            if self.run_id:
                span.set_attribute("agent.run_id", self.run_id)
                
            confidence = context_bundle.get("confidence", "low")
            rag_context = context_bundle.get("rag_context", "")
            db_context = context_bundle.get("db_context", "")
            
            span.set_attribute("retrieval.confidence", confidence)
            
            try:
                with open("config/prompts/chat_agent.txt", "r") as f:
                    system_template = f.read()
                    
                system_prompt = system_template.format(
                    rag_context=rag_context,
                    db_context=db_context,
                    confidence=confidence
                )
                
                if confidence.lower() == "low":
                    warning = "WARNING: Retrieval confidence is LOW. Be especially careful to indicate uncertainty and avoid making claims beyond what the context supports.\n\n"
                    system_prompt = warning + system_prompt
                    
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_question}
                ]
                
                response_text = self.llm_tool.call_llm(
                    messages=messages,
                    model=self.settings.openrouter_model_chat,
                    temperature=0.1 # Keep it deterministic
                )
                
                # Verify basic citations (naive check for dates or tickers generally works, or just check 'per Form 4' or 'tweets')
                has_citations = "202" in response_text or "Form 4" in response_text or "tweet" in response_text
                
                if "sufficient data" in response_text.lower() or "don't have" in response_text.lower() or "doesn't have" in response_text.lower():
                    has_citations = False
                    
                if not has_citations and "sufficient data" not in response_text.lower() and "don't have" not in response_text.lower() and "doesn't have" not in response_text.lower():
                    response_text += "\n\n[Note: This response could not be verified against source data]"
                    has_citations = False
                    
                span.set_attribute("response.length", len(response_text))
                span.set_attribute("response.has_citations", has_citations)
                
                return {
                    "response": response_text,
                    "has_citations": has_citations,
                    "confidence": confidence
                }
                
            except Exception as e:
                self.logger.error(f"Chat agent failed: {e}")
                span.set_attribute("error", str(e))
                raise
