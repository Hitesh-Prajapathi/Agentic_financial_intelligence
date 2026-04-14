import sys
import os

# Add root folder to python path so imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
from config.settings import settings
from tools.db_tool import db_tool
from tools.sec_api_tool import SecApiTool
from tools.apify_tool import ApifyTool
from tools.lightrag_tool import LightRAGTool
from tools.openrouter_tool import OpenRouterTool
from tools.chart_tool import ChartTool

from agents.sec_data_agent import SecDataAgent
from agents.ranking_agent import RankingAgent
from agents.social_scraper_agent import SocialScraperAgent
from agents.sentiment_agent import SentimentAgent
from agents.indexing_agent import IndexingAgent
from agents.retrieval_agent import RetrievalAgent
from agents.chat_agent import ChatAgent
from agents.visualization_agent import VisualizationAgent
from agents.supervisor import SupervisorAgent

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def main():
    setup_logging()
    logger = logging.getLogger("run_pipeline")
    
    logger.info("Initializing database...")
    db_tool.init_db()

    logger.info("Initializing tools...")
    sec_tool = SecApiTool()
    apify_tool = ApifyTool()
    llm_tool = OpenRouterTool()
    lightrag_tool = LightRAGTool()
    chart_tool = ChartTool()

    logger.info("Initializing agents...")
    sec_data_agent = SecDataAgent(sec_tool, db_tool, settings)
    ranking_agent = RankingAgent(db_tool, settings)
    social_scraper_agent = SocialScraperAgent(apify_tool, db_tool, settings)
    sentiment_agent = SentimentAgent(llm_tool, db_tool, settings)
    indexing_agent = IndexingAgent(lightrag_tool, db_tool, settings)
    
    retrieval_agent = RetrievalAgent(llm_tool, lightrag_tool, db_tool=db_tool, settings=settings)
    chat_agent = ChatAgent(llm_tool, settings)
    visualization_agent = VisualizationAgent(chart_tool, db_tool, settings)

    supervisor = SupervisorAgent(
        sec_data_agent=sec_data_agent,
        ranking_agent=ranking_agent,
        social_scraper_agent=social_scraper_agent,
        sentiment_agent=sentiment_agent,
        indexing_agent=indexing_agent,
        retrieval_agent=retrieval_agent,
        chat_agent=chat_agent,
        visualization_agent=visualization_agent,
        db_tool=db_tool
    )

    logger.info("[Pipeline] Starting ingestion pipeline...")
    try:
        results = supervisor.run_ingestion_pipeline()
        logger.info("[Pipeline] ✅ Pipeline completed successfully.")
        
        logger.info(f"  → Ingested {results['sec_result'].get('filings_ingested')} filings")
        logger.info(f"  → Ranked {len(results['ranking_result'].get('top_trades', []))} top trades. Checksum: {results['ranking_result'].get('checksum')}")
        logger.info(f"  → Scraped {results['social_result'].get('total_tweets')} tweets")
        logger.info(f"  → Classified {results['sentiment_result'].get('tweets_classified')} tweets")
        logger.info(f"  → Indexed {results['indexing_result'].get('indexed_blocks')} context blocks")
        
    except Exception as e:
        logger.error(f"[Pipeline] ❌ Pipeline failed: {e}")

if __name__ == "__main__":
    main()
