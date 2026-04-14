import pytest
from unittest.mock import MagicMock
from agents.visualization_agent import VisualizationAgent
from config.settings import settings
import pandas as pd

def test_visualization_agent_summary():
    chart_tool = MagicMock()
    chart_tool.plot_top_trades_summary.return_value = "path/to/summary.png"
    
    db_tool = MagicMock()
    # Mock return
    df_mock = pd.DataFrame([
        {"run_date": "2026-04-14", "rank": 1, "issuer_ticker": "AAPL", "insider_name": "Cook", "net_dollar_value": 500}
    ])
    db_tool.query_df.return_value = df_mock
    
    agent = VisualizationAgent(chart_tool, db_tool, settings)
    intent = {"query_type": "summary"}
    result = agent.run(intent)
    
    assert result["chart_path"] == "path/to/summary.png"
    assert result["chart_type"] == "top_trades_summary"
    chart_tool.plot_top_trades_summary.assert_called_once()

def test_visualization_agent_comparison():
    chart_tool = MagicMock()
    chart_tool.plot_sentiment_vs_trades.return_value = "path/to/trades_sent.png"
    
    db_tool = MagicMock()
    trades_df = pd.DataFrame([{"run_date": "2026-04-14", "net_dollar_value": 500}])
    sent_df = pd.DataFrame([{"summary_date": "2026-04-14", "sentiment_index": 0.5}])
    db_tool.query_df.side_effect = [trades_df, sent_df]
    
    agent = VisualizationAgent(chart_tool, db_tool, settings)
    intent = {"query_type": "comparison", "tickers": ["AAPL"]}
    result = agent.run(intent)
    
    assert result["chart_path"] == "path/to/trades_sent.png"
    assert result["chart_type"] == "sentiment_vs_trades"
    chart_tool.plot_sentiment_vs_trades.assert_called_once()
    
def test_visualization_agent_entity():
    chart_tool = MagicMock()
    chart_tool.plot_sentiment_distribution.return_value = "path/to/dist.png"
    
    db_tool = MagicMock()
    sent_df = pd.DataFrame([{"bullish_count": 10, "bearish_count": 5, "neutral_count": 2}])
    db_tool.query_df.return_value = sent_df
    
    agent = VisualizationAgent(chart_tool, db_tool, settings)
    intent = {"query_type": "entity", "tickers": ["AAPL"]}
    result = agent.run(intent)
    
    assert result["chart_path"] == "path/to/dist.png"
    assert result["chart_type"] == "sentiment_distribution"
    chart_tool.plot_sentiment_distribution.assert_called_once()
