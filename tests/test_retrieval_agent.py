import pytest
from unittest.mock import MagicMock, mock_open, patch
from agents.retrieval_agent import RetrievalAgent
from config.settings import settings

def test_determine_mode_local():
    llm_tool = MagicMock()
    llm_tool.call_llm_structured.return_value = {"mode": "local"}
    
    lightrag_tool = MagicMock()
    
    with patch("builtins.open", mock_open(read_data="mock prompt")):
        agent = RetrievalAgent(llm_tool, lightrag_tool, settings)
        mode = agent.determine_mode("What did Tim Cook buy?")
        
    assert mode == "local"

def test_determine_mode_global():
    llm_tool = MagicMock()
    llm_tool.call_llm_structured.return_value = {"mode": "global"}
    
    lightrag_tool = MagicMock()
    
    with patch("builtins.open", mock_open(read_data="mock prompt")):
        agent = RetrievalAgent(llm_tool, lightrag_tool, settings)
        mode = agent.determine_mode("What are the broader themes in the market?")
        
    assert mode == "global"

def test_determine_mode_fallback():
    # If the LLM throws an error, default to hybrid
    llm_tool = MagicMock()
    llm_tool.call_llm_structured.side_effect = Exception("API failed")
    
    lightrag_tool = MagicMock()
    
    with patch("builtins.open", mock_open(read_data="mock prompt")):
        agent = RetrievalAgent(llm_tool, lightrag_tool, settings)
        mode = agent.determine_mode("Any question")
        
    assert mode == "hybrid"

def test_run_retrieval():
    llm_tool = MagicMock()
    llm_tool.call_llm_structured.return_value = {"mode": "local"}
    
    lightrag_tool = MagicMock()
    lightrag_tool.query.return_value = "Tim Cook sold 500k shares of AAPL context."
    
    with patch("builtins.open", mock_open(read_data="mock prompt")):
        agent = RetrievalAgent(llm_tool, lightrag_tool, settings)
        result = agent.run("Why did Tim Cook sell?")
        
    assert result["mode_used"] == "local"
    assert "Tim Cook sold 500k shares" in result["context"]
    lightrag_tool.query.assert_called_once_with("Why did Tim Cook sell?", mode="local")
