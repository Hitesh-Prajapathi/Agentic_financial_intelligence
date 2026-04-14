import httpx
import json
from config.settings import settings
from telemetry.otel_setup import tracer
import logging

class OpenRouterTool:
    def __init__(self):
        self.api_key = settings.openrouter_api_key
        self.default_model = settings.openrouter_model_classify
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "X-Title": "insider-trading-agent",
            "Content-Type": "application/json"
        }
        self.logger = logging.getLogger(__name__)

    def call_llm(self, messages: list[dict], model: str = None, temperature: float = 0.0) -> str:
        model = model or self.default_model
        
        with tracer.start_as_current_span("openrouter.llm_call") as span:
            span.set_attribute("llm.model", model)
            span.set_attribute("llm.temperature", temperature)
            span.set_attribute("llm.purpose", "chat_response")
            
            payload = {
                "model": model,
                "messages": messages,
                "temperature": temperature
            }
            
            try:
                response = httpx.post(self.base_url, headers=self.headers, json=payload, timeout=60.0)
                response.raise_for_status()
                data = response.json()
                
                content = data['choices'][0]['message']['content']
                usage = data.get('usage', {})
                span.set_attribute("llm.prompt_tokens", usage.get("prompt_tokens", 0))
                span.set_attribute("llm.completion_tokens", usage.get("completion_tokens", 0))
                
                return content
            except Exception as e:
                self.logger.error(f"OpenRouter API Error: {e}")
                span.set_attribute("error", str(e))
                raise

    def call_llm_structured(self, messages: list[dict], model: str = None, response_format: dict = None) -> dict:
        model = model or self.default_model
        temperature = 0.0
        
        with tracer.start_as_current_span("openrouter.llm_call_structured") as span:
            span.set_attribute("llm.model", model)
            span.set_attribute("llm.purpose", "sentiment_classify")
            
            payload = {
                "model": model,
                "messages": messages,
                "temperature": temperature,
                "response_format": response_format or {"type": "json_object"}
            }
            
            try:
                response = httpx.post(self.base_url, headers=self.headers, json=payload, timeout=60.0)
                response.raise_for_status()
                data = response.json()
                
                content = data['choices'][0]['message']['content']
                usage = data.get('usage', {})
                span.set_attribute("llm.prompt_tokens", usage.get("prompt_tokens", 0))
                span.set_attribute("llm.completion_tokens", usage.get("completion_tokens", 0))
                
                return json.loads(content)
            except Exception as e:
                self.logger.error(f"OpenRouter API Error: {e}")
                span.set_attribute("error", str(e))
                raise
