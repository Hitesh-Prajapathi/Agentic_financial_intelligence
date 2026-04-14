import pytest
from unittest.mock import MagicMock, mock_open, patch
from agents.sentiment_agent import SentimentAgent
from config.settings import settings

def test_sentiment_agent_bullish():
    # "TSLA to the moon 🚀🚀🚀" → bullish
    llm_tool = MagicMock()
    llm_tool.call_llm_structured.return_value = {"1": {"label": "bullish", "confidence": 0.95}}
    
    db_tool = MagicMock()
    db_tool.query.side_effect = [
        [("1", "TSLA", "TSLA to the moon 🚀🚀🚀", "2024-04-14")],
        [("bullish", 1, 0.95)]
    ]
    
    with patch("builtins.open", mock_open(read_data="mock prompt")):
        agent = SentimentAgent(llm_tool, db_tool, settings)
        result = agent.run()
        
    assert result["tweets_classified"] == 1
    llm_tool.call_llm_structured.assert_called_once()
    db_tool.insert_sentiments.assert_called_once()
    inserted = db_tool.insert_sentiments.call_args[0][0]
    assert inserted[0]["sentiment_label"] == "bullish"

def test_sentiment_agent_bearish():
    # "SEC just opened an investigation into $AAPL insider trading" → bearish
    llm_tool = MagicMock()
    llm_tool.call_llm_structured.return_value = {"2": {"label": "bearish", "confidence": 0.99}}
    db_tool = MagicMock()
    db_tool.query.side_effect = [
        [("2", "AAPL", "SEC just opened an investigation into $AAPL insider trading", "2024-04-14")],
        [("bearish", 1, 0.99)]
    ]
    with patch("builtins.open", mock_open(read_data="mock prompt")):
        agent = SentimentAgent(llm_tool, db_tool, settings)
        agent.run()
        inserted = db_tool.insert_sentiments.call_args[0][0]
        assert inserted[0]["sentiment_label"] == "bearish"

def test_sentiment_agent_neutral():
    # "Apple reported Q3 earnings today" → neutral
    llm_tool = MagicMock()
    llm_tool.call_llm_structured.return_value = {"3": {"label": "neutral", "confidence": 0.99}}
    db_tool = MagicMock()
    db_tool.query.side_effect = [
        [("3", "AAPL", "Apple reported Q3 earnings today", "2024-04-14")],
        [("neutral", 1, 0.99)]
    ]
    with patch("builtins.open", mock_open(read_data="mock prompt")):
        agent = SentimentAgent(llm_tool, db_tool, settings)
        agent.run()
        inserted = db_tool.insert_sentiments.call_args[0][0]
        assert inserted[0]["sentiment_label"] == "neutral"

def test_sentiment_agent_short_squeeze_bullish():
    # "This stock is a short squeeze waiting to happen" → bullish
    llm_tool = MagicMock()
    llm_tool.call_llm_structured.return_value = {"4": {"label": "bullish", "confidence": 0.99}}
    db_tool = MagicMock()
    db_tool.query.side_effect = [
        [("4", "GME", "This stock is a short squeeze waiting to happen", "2024-04-14")],
        [("bullish", 1, 0.99)]
    ]
    with patch("builtins.open", mock_open(read_data="mock prompt")):
        agent = SentimentAgent(llm_tool, db_tool, settings)
        agent.run()
        inserted = db_tool.insert_sentiments.call_args[0][0]
        assert inserted[0]["sentiment_label"] == "bullish"

def test_sentiment_agent_batch_of_10():
    # batch of 10 tweets returns 10 labels
    llm_tool = MagicMock()
    mock_response = {str(i): {"label": "bullish", "confidence": 0.8} for i in range(10)}
    llm_tool.call_llm_structured.return_value = mock_response
    
    db_tool = MagicMock()
    db_query_response = [(str(i), "AAPL", f"Tweet {i}", "2024-04-14") for i in range(10)]
    db_tool.query.side_effect = [
        db_query_response,
        [("bullish", 10, 0.8)]
    ]
    with patch("builtins.open", mock_open(read_data="mock prompt")):
        agent = SentimentAgent(llm_tool, db_tool, settings)
        res = agent.run()
        assert res["tweets_classified"] == 10
        llm_tool.call_llm_structured.assert_called_once()
