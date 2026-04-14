import pytest
from unittest.mock import MagicMock, mock_open, patch
from agents.chat_agent import ChatAgent
from config.settings import settings

def test_chat_agent_empty_context():
    llm_tool = MagicMock()
    # If the LLM faithfully follows the prompt, it should say I don't know
    llm_tool.call_llm.return_value = "I don't have sufficient data to answer that question. The available data covers nothing."
    
    context_bundle = {
        "rag_context": "",
        "db_context": "",
        "confidence": "low"
    }
    
    with patch("builtins.open", mock_open(read_data="Rules:\n{rag_context}\n{db_context}\n{confidence}")):
        agent = ChatAgent(llm_tool, settings)
        result = agent.run(context_bundle, "What about AAPL?")
        
    assert "sufficient data" in result["response"].lower()
    assert result["confidence"] == "low"
    
def test_chat_agent_citations():
    llm_tool = MagicMock()
    llm_tool.call_llm.return_value = "Tim Cook sold 500k shares per Form 4 filed 2026-04-14."
    
    context_bundle = {
        "rag_context": "Tim Cook sold 500k shares on 2026-04-14",
        "db_context": "",
        "confidence": "high"
    }
    
    with patch("builtins.open", mock_open(read_data="Rules:\n{rag_context}\n{db_context}\n{confidence}")):
        agent = ChatAgent(llm_tool, settings)
        result = agent.run(context_bundle, "What did Tim Cook do?")
        
    assert result["has_citations"] is True
    assert "2026-04-14" in result["response"]
    
def test_chat_agent_no_citations_flags_warning():
    llm_tool = MagicMock()
    llm_tool.call_llm.return_value = "Tim Cook sold a bunch of shares."
    
    context_bundle = {
        "rag_context": "Tim Cook sold 500k shares",
        "db_context": "",
        "confidence": "medium"
    }
    
    with patch("builtins.open", mock_open(read_data="Rules:\n{rag_context}\n{db_context}\n{confidence}")):
        agent = ChatAgent(llm_tool, settings)
        result = agent.run(context_bundle, "What did Tim Cook do?")
        
    assert result["has_citations"] is False
    assert "[Note: This response could not be verified against source data]" in result["response"]
