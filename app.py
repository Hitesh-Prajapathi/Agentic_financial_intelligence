import nest_asyncio
nest_asyncio.apply()

import streamlit as st
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

import os

# Configure UI
st.set_page_config(page_title="Insider Trading AI", page_icon="📈", layout="wide")

# Suppress noisy third-party logs
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("opentelemetry").setLevel(logging.WARNING)
logging.getLogger("lightrag").setLevel(logging.WARNING)
logging.getLogger("apify_client").setLevel(logging.WARNING)

st.title("SEC Insider Trading Intelligence Agent")

# Sidebar setup and Supervisor caching
@st.cache_resource
def get_supervisor():
    """Initializes all agents once and caches the Supervisor"""
    logging.info("Initializing Agent System...")
    db_tool.init_db()

    sec_tool = SecApiTool()
    apify_tool = ApifyTool()
    llm_tool = OpenRouterTool()
    lightrag_tool = LightRAGTool()
    chart_tool = ChartTool()

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
    return supervisor

supervisor = get_supervisor()

# Sidebar Control Panel
with st.sidebar:
    st.header("Pipeline Controls")
    st.markdown("Manually trigger data ingestion across all external endpoints (SEC, Twitter).")
    if st.button("Run Live Data Pipeline"):
        with st.spinner("Running 5-stage ingestion pipeline..."):
            try:
                results = supervisor.run_ingestion_pipeline()
                st.success("Pipeline Completed!")
                st.write(f"- Filings: {results['sec_result'].get('filings_ingested', 0)}")
                st.write(f"- Ranked Trades: {len(results['ranking_result'].get('top_trades', []))}")
                st.write(f"- Tweets Scraped: {results['social_result'].get('total_tweets', 0)}")
                st.write(f"- Tweets Classified: {results['sentiment_result'].get('tweets_classified', 0)}")
                st.write(f"- Blocks Indexed: {results['indexing_result'].get('indexed_blocks', 0)}")
            except Exception as e:
                st.error(f"Pipeline failed (Quarantined): {e}")

    st.markdown("---")
    st.header("Setup & Debugging")
    st.markdown("If the SEC lacks Form 4 filings from the last 24H, use this to inject static mock trades and sentiments.")
    if st.button("Load Demo Data (Overrides Blank Run)"):
        with st.spinner("Injecting TSLA, AAPL, MSFT mock data..."):
            try:
                import sys
                import os
                sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
                from scripts.seed_test_data import seed_db
                seed_db()
                st.success("Test tables seeded! Try asking a question.")
            except Exception as e:
                st.error(f"Failed to load demo data: {e}")

# Chat History state
if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.messages.append({
        "role": "assistant",
        "content": "Hello! I am your Insider Trading AI. You can ask me about recent trades, or ask me to draw a chart of the top trades.",
        "chart_path": None
    })

# Render Chat History
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])
        if msg.get("chart_path") and os.path.exists(msg["chart_path"]):
            st.image(msg["chart_path"], width="stretch")

# Chat Input
if prompt := st.chat_input("Ask about recent insider trades or say 'show me a chart of the top trades'..."):
    # Add user message to UI
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    # Process Query
    with st.chat_message("assistant"):
        with st.spinner("Analyzing data and generating response..."):
            result = supervisor.handle_query(prompt)
            
            response_text = result.get("response", "No response generated.")
            chart_path = result.get("chart_path")
            
            # Display text
            st.markdown(response_text)
            
            # Display chart if generated
            if chart_path and os.path.exists(chart_path):
                st.image(chart_path, width="stretch")
                
            # Keep in state
            st.session_state.messages.append({
                "role": "assistant", 
                "content": response_text,
                "chart_path": chart_path
            })
