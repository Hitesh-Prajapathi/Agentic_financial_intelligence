import pytest
from unittest.mock import MagicMock
from agents.indexing_agent import IndexingAgent

def test_context_block_construction():
    db_tool = MagicMock()
    # Mock trades: rank, insider_name, net_dollar_value, dominant_direction
    trade_mock = [(1, "Tim Cook", 500000.0, "SELL")]
    # Mock sentiment: total_tweets, bullish_count, bearish_count, neutral_count, avg_confidence, sentiment_index
    sent_mock = [(100, 60, 20, 20, 0.95, 0.40)]
    # Mock tweets: text
    tweet_mock = [("Just bought an iPhone!",), ("AAPL is overrated",)]
    
    db_tool.query.side_effect = [trade_mock, sent_mock, tweet_mock]
    
    lightrag_tool = MagicMock()
    agent = IndexingAgent(lightrag_tool, db_tool)
    
    block = agent.build_context_block("AAPL", "2024-04-14")
    
    assert "DATA REPORT FOR TICKER: AAPL ON DATE: 2024-04-14" in block
    assert "Tim Cook executing dominant SELL with net dollar flow of $500000.0" in block
    assert "Total related tweets: 100" in block
    assert "Sentiment split: 60 Bullish, 20 Bearish, 20 Neutral" in block
    assert "Overall Net Sentiment Index: 0.40 (Bullish)" in block
    assert "Just bought an iPhone!" in block

def test_indexing_agent_run():
    db_tool = MagicMock()
    # Mock to return exactly one ticker, then one date
    db_tool.query.side_effect = [
        [("AAPL",)], # SELECT DISTINCT issuer_ticker
        [("2024-04-14",)], # SELECT DISTINCT run_date
        # Followed by build_context queries
        [(1, "Tim Cook", 500000.0, "BUY")],
        [(100, 50, 50, 0, 0.9, 0.0)],
        [("Great phone",)]
    ]
    
    lightrag_tool = MagicMock()
    agent = IndexingAgent(lightrag_tool, db_tool)
    
    res = agent.run()
    assert res['indexed_blocks'] == 1
    assert res['tickers_updated'] == 1
    lightrag_tool.insert.assert_called_once()

def test_indexing_agent_run_empty():
    db_tool = MagicMock()
    # Returns empty array for tickers
    db_tool.query.return_value = []
    
    lightrag_tool = MagicMock()
    agent = IndexingAgent(lightrag_tool, db_tool)
    
    res = agent.run()
    assert res['indexed_blocks'] == 0
    assert res['tickers_updated'] == 0
    lightrag_tool.insert.assert_not_called()
