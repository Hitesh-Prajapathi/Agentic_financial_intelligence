import pytest
import os
import shutil
from unittest.mock import MagicMock
from config.settings import settings
from tools.db_tool import DBTool
from agents.sec_data_agent import SecDataAgent
from agents.ranking_agent import RankingAgent
from agents.social_scraper_agent import SocialScraperAgent
from agents.sentiment_agent import SentimentAgent
from agents.indexing_agent import IndexingAgent
from agents.retrieval_agent import RetrievalAgent
from agents.chat_agent import ChatAgent
from agents.visualization_agent import VisualizationAgent
from agents.supervisor import SupervisorAgent

@pytest.fixture
def clean_db():
    # Use a temporary DB for integration testing
    test_db_path = "./data/test_integration.duckdb"
    settings.db_path = test_db_path
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
        
    db = DBTool()
    db.init_db()
    
    yield db
    
    # Cleanup DB connection and file
    del db 
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

@pytest.fixture
def supervisor(clean_db):
    # Mock External Tools
    sec_tool = MagicMock()
    # Simulate a single AAPL filing with 1 transaction
    sec_tool.fetch_recent_filings.return_value = [{"accessionNo": "000123-24-001"}]
    sec_tool.parse_filing.return_value = (
        {
            "accession_number": "000123-24-001",
            "filing_date": "2026-04-14T00:00:00Z",
            "issuer_name": "Apple Inc",
            "issuer_ticker": "AAPL",
            "insider_name": "Tim Cook",
            "form_type": "4",
            "raw_json": {}
        },
        [
            {
                "accession_number": "000123-24-001",
                "transaction_type": "D",
                "shares": 1000,
                "price_per_share": 150.0
            }
        ]
    )
    
    apify_tool = MagicMock()
    apify_tool.scrape_tweets.return_value = [{
        "tweet_id": "tweet_1",
        "ticker": "AAPL",
        "text": "AAPL going down today after Tim Cook sells",
        "author_username": "trader_joe",
        "created_at": "2026-04-14T12:00:00Z",
        "retweet_count": 0, "like_count": 10, "reply_count": 1,
        "raw_json": {}
    }]
    
    llm_tool = MagicMock()
    # sentiment analysis fallback
    llm_tool.call_llm_structured.return_value = {"label": "bearish", "confidence": 0.9}
    
    lightrag_tool = MagicMock()
    # Simulate inserting nothing or success
    lightrag_tool.insert_texts.return_value = None
    
    chart_tool = MagicMock()

    settings.top_n_trades = 5 # Force to 5

    # Agents
    sec_data_agent = SecDataAgent(sec_tool, clean_db, settings)
    ranking_agent = RankingAgent(clean_db, settings)
    social_scraper_agent = SocialScraperAgent(apify_tool, clean_db, settings)
    sentiment_agent = SentimentAgent(llm_tool, clean_db, settings)
    indexing_agent = IndexingAgent(lightrag_tool, clean_db, settings)
    retrieval_agent = RetrievalAgent(llm_tool, lightrag_tool, settings)
    chat_agent = ChatAgent(llm_tool, settings)
    visualization_agent = VisualizationAgent(chart_tool, clean_db, settings)

    return SupervisorAgent(
        sec_data_agent=sec_data_agent,
        ranking_agent=ranking_agent,
        social_scraper_agent=social_scraper_agent,
        sentiment_agent=sentiment_agent,
        indexing_agent=indexing_agent,
        retrieval_agent=retrieval_agent,
        chat_agent=chat_agent,
        visualization_agent=visualization_agent,
        db_tool=clean_db
    )

def test_seed_data(supervisor):
    assert settings.db_path == "./data/test_integration.duckdb"

def test_e2e_ingestion(supervisor):
    result = supervisor.run_ingestion_pipeline()
    assert result["status"] == "success"
    
    db = supervisor.db_tool
    filings = db.query("SELECT * FROM filings")
    top_trades = db.query("SELECT * FROM top_trades")
    tweets = db.query("SELECT * FROM tweets")
    sentiments = db.query("SELECT * FROM tweet_sentiments")
    
    # Assert cascading flows through 7 tables
    assert len(filings) == 1
    assert len(top_trades) == 1
    assert len(tweets) == 1
    assert len(sentiments) == 1
    
    # Assert Checksum exists
    assert top_trades[0][4] is not None # checksum validation

def test_e2e_query(supervisor):
    # Prepare LLM responses first
    supervisor.retrieval_agent.llm_tool = MagicMock()
    supervisor.retrieval_agent.lightrag_tool = MagicMock()
    # Set proper signature for the RAG intent
    supervisor.retrieval_agent.llm_tool.call_llm_structured.return_value = {
        "intent": "local", "tickers": ["AAPL"], "needs_chart": False
    }
    supervisor.retrieval_agent.lightrag_tool.query.return_value = "Tim Cook sold 1000 shares of AAPL for $150.0"
    
    # 2. Chat Agent generation
    supervisor.chat_agent.llm_tool = MagicMock()
    supervisor.chat_agent.llm_tool.call_llm.return_value = "[HIGH CONFIDENCE] Tim Cook sold 1000 shares on 2026-04-14 (per Form 4)"
    
    response = supervisor.handle_query("What did Tim Cook do?")
    
    assert response["has_citations"] is True
    assert "Tim Cook" in response["response"]
    assert "[HIGH CONFIDENCE]" in response["response"]

def test_hallucination_boundary(supervisor):
    # Prepare Chat logic to respond correctly when context is empty
    supervisor.retrieval_agent.llm_tool = MagicMock()
    supervisor.retrieval_agent.lightrag_tool = MagicMock()
    
    supervisor.retrieval_agent.llm_tool.call_llm_structured.return_value = {
        "intent": "hybrid", "tickers": ["UNKNOWN"], "needs_chart": False
    }
    # Empty retrieval
    supervisor.retrieval_agent.lightrag_tool.query.return_value = ""
    
    supervisor.chat_agent.llm_tool = MagicMock()
    supervisor.chat_agent.llm_tool.call_llm.return_value = "I don't have sufficient data to answer that question."
    
    response = supervisor.handle_query("Tell me about UNKNOWN stock.")
    
    assert response["has_citations"] is False
    assert "sufficient data" in response["response"].lower()
