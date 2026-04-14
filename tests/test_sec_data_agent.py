import pytest
import json
import time
from unittest.mock import MagicMock
from tools.sec_api_tool import SecApiTool, ParsingError, UpstreamAPIError
from agents.sec_data_agent import SecDataAgent
from config.settings import settings

@pytest.fixture
def sample_form4_data():
    with open('tests/fixtures/sample_form4.json', 'r') as f:
        return json.load(f)

def test_parse_filing_basic(sample_form4_data):
    tool = SecApiTool()
    raw = sample_form4_data[0]
    filing, txns = tool.parse_filing(raw)
    
    assert filing['accession_number'] == "0001234567-24-000001"
    assert filing['issuer_ticker'] == "AAPL"
    assert len(txns) == 1
    assert txns[0]['shares'] == "1000"
    assert txns[0]['transaction_type'] == "D"

def test_parse_filing_multiple_transactions(sample_form4_data):
    tool = SecApiTool()
    raw = sample_form4_data[1]  # TSLA filing with 2 txns
    filing, txns = tool.parse_filing(raw)
    
    assert len(txns) == 2
    assert txns[0]['shares'] == "5000"
    assert txns[0]['transaction_type'] == "A"
    assert txns[1]['shares'] == "100"
    assert txns[1]['transaction_type'] == "D"

def test_retry_on_api_failure():
    tool = SecApiTool()
    tool.api = MagicMock()
    import requests
    # Mock to fail twice then succeed
    tool.api.get_data.side_effect = [
        requests.exceptions.HTTPError("502 Bad Gateway"),
        requests.exceptions.HTTPError("503 Service Unavailable"),
        {'filings': [{'accessionNo': '123'}]}
    ]
    tool._rate_limit = MagicMock()
    # Speed up tests by overriding delays
    import tools.sec_api_tool
    original_sleep = tools.sec_api_tool.time.sleep
    tools.sec_api_tool.time.sleep = MagicMock()
    
    res = tool.fetch_recent_filings()
    assert len(res) == 1
    assert res[0]['accessionNo'] == '123'
    assert tool.api.get_data.call_count == 3
    
    tools.sec_api_tool.time.sleep = original_sleep

def test_rate_limiting():
    tool = SecApiTool()
    start = time.time()
    tool._rate_limit()
    tool._rate_limit()
    end = time.time()
    
    assert end - start >= 0.1

def test_deduplication_by_accession_number(sample_form4_data):
    # Mock SecApiTool
    sec_tool = SecApiTool()
    sec_tool.fetch_recent_filings = MagicMock(return_value=sample_form4_data)
    
    # Mock DBTool
    db_tool = MagicMock()
    db_tool.query.return_value = [("0001234567-24-000001",)] # AAPL filing exists
    
    agent = SecDataAgent(sec_tool, db_tool, settings)
    result = agent.run()
    
    assert result['duplicates_skipped'] == 1
    assert result['filings_ingested'] == 2 # 3 total, 1 skipped
    assert result['transactions_found'] == 3 # 2 + 1 txns from remaining 2 filings
    db_tool.insert_filings.assert_called_once()
    db_tool.insert_transactions.assert_called_once()
