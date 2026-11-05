"""Composition root: builds the whole agent system for a mode ('live' or 'demo')."""
from __future__ import annotations

from dataclasses import dataclass

from agents.chat_agent import ChatAgent
from agents.indexing_agent import IndexingAgent
from agents.ranking_agent import RankingAgent
from agents.retrieval_agent import RetrievalAgent
from agents.sec_data_agent import SecDataAgent
from agents.sentiment_agent import SentimentAgent
from agents.social_scraper_agent import SocialScraperAgent
from agents.supervisor import SupervisorAgent
from agents.visualization_agent import VisualizationAgent
from config.settings import settings
from skills.learning_loop import LearningLoop
from tools.apify_tool import ApifyTool
from tools.chart_tool import ChartTool
from tools.db_tool import DBTool, get_db
from tools.lightrag_tool import LightRAGTool
from tools.llm_tool import LLMTool
from tools.sec_api_tool import SecApiTool


@dataclass
class System:
    mode: str
    db: DBTool
    llm: LLMTool
    rag: LightRAGTool
    apify: ApifyTool
    sec: SecApiTool
    charts: ChartTool
    learning: LearningLoop
    supervisor: SupervisorAgent


def build_system(mode: str = "live", cfg=settings) -> System:
    path = cfg.live_db_path if mode == "live" else cfg.demo_db_path
    db = get_db(path)
    llm = LLMTool(cfg)
    rag = LightRAGTool(cfg, llm, namespace=mode)
    apify, sec, charts = ApifyTool(cfg), SecApiTool(cfg), ChartTool()

    sentiment = SentimentAgent(llm, db, cfg)
    learning = LearningLoop(db, llm, sentiment, cfg)
    supervisor = SupervisorAgent(
        sec_data_agent=SecDataAgent(sec, db, cfg), ranking_agent=RankingAgent(db, cfg),
        social_scraper_agent=SocialScraperAgent(apify, db, cfg), sentiment_agent=sentiment,
        indexing_agent=IndexingAgent(rag, db, cfg), retrieval_agent=RetrievalAgent(rag, db, cfg),
        chat_agent=ChatAgent(llm, cfg), visualization_agent=VisualizationAgent(charts, db, cfg),
        learning_loop=learning, db_tool=db, llm_tool=llm, settings=cfg)
    return System(mode, db, llm, rag, apify, sec, charts, learning, supervisor)
