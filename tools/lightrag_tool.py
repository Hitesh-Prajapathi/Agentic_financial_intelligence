import os
from lightrag import LightRAG, QueryParam
from lightrag.llm.openai import openai_complete_if_cache, openai_embed as openai_embedding
from lightrag.utils import EmbeddingFunc
from config.settings import settings
from telemetry.otel_setup import tracer
import logging

class LightRAGTool:
    def __init__(self, workspace_dir="./data/lightrag_workspace"):
        self.workspace_dir = workspace_dir
        if not os.path.exists(workspace_dir):
            os.makedirs(workspace_dir)
            
        self.logger = logging.getLogger(__name__)

        async def llm_model_func(prompt, **kwargs):
            # LightRAG may pass 'model' in kwargs — remove it so we control which model is used
            kwargs.pop("model", None)
            return await openai_complete_if_cache(
                settings.lightrag_llm_model,  # first positional arg = model
                prompt,
                api_key=settings.openrouter_api_key,
                base_url="https://openrouter.ai/api/v1",
                **kwargs
            )
            
        async def embedding_func(texts: list[str]):
            return await openai_embedding(
                texts,
                model=settings.lightrag_embedding_model,
                api_key=settings.openrouter_api_key,
                base_url="https://openrouter.ai/api/v1"
            )

        # LightRAG requires an async event loop for initialization of embeddings if graph requires it.
        # It's safer to pass callables that LightRAG internally handles.
        self.rag = LightRAG(
            working_dir=self.workspace_dir,
            llm_model_func=llm_model_func,
            llm_model_name=settings.lightrag_llm_model,
            embedding_func=EmbeddingFunc(
                embedding_dim=1536,
                max_token_size=8192,
                func=embedding_func
            )
        )
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            # Already inside an event loop (e.g. Streamlit) — use nest_asyncio
            import nest_asyncio
            nest_asyncio.apply()
            loop.run_until_complete(self.rag.initialize_storages())
        except RuntimeError:
            # No event loop running — safe to use asyncio.run
            asyncio.run(self.rag.initialize_storages())

    def insert(self, texts: list[str] | str):
        with tracer.start_as_current_span("lightrag.insert") as span:
            span.set_attribute("lightrag.texts_count", len(texts) if isinstance(texts, list) else 1)
            try:
                self.rag.insert(texts)
            except Exception as e:
                self.logger.error(f"LightRAG insert failed: {e}")
                span.set_attribute("error", str(e))
                raise

    def query(self, query_text: str, mode: str = "hybrid") -> str:
        with tracer.start_as_current_span("lightrag.query") as span:
            span.set_attribute("lightrag.query", query_text)
            span.set_attribute("lightrag.mode", mode)
            try:
                param = QueryParam(mode=mode)
                result = self.rag.query(query_text, param=param)
                return result
            except Exception as e:
                self.logger.error(f"LightRAG query failed: {e}")
                span.set_attribute("error", str(e))
                raise
