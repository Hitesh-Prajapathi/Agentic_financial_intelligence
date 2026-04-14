import pytest
from unittest.mock import MagicMock
import pandas as pd
from agents.ranking_agent import RankingAgent
from config.settings import settings

def test_ranking_agent_no_transactions():
    db_tool = MagicMock()
    # Mock empty dataframe from db query
    db_tool.query_df.return_value = pd.DataFrame()
    
    agent = RankingAgent(db_tool, settings)
    result = agent.run()
    
    assert len(result['top_trades']) == 0
    assert result['checksum'] == "empty"
    db_tool.insert_top_trades.assert_not_called()

def test_ranking_agent_with_transactions():
    db_tool = MagicMock()
    # Mock data directly matching what DB would join
    mock_data = pd.DataFrame([
        {
            'accession_number': '123',
            'issuer_ticker': 'AAPL',
            'shares': 1000,
            'price_per_share': 150.0,
            'transaction_type': 'A',
            'insider_name': 'Tim Cook',
            'transaction_date': '2024-04-14',
            'filing_date': '2024-04-14 10:00:00'
        },
        {
            'accession_number': '124',
            'issuer_ticker': 'TSLA',
            'shares': 500,
            'price_per_share': 200.0,
            'transaction_type': 'D',
            'insider_name': 'Elon Musk',
            'transaction_date': '2024-04-14',
            'filing_date': '2024-04-14 11:00:00'
        }
    ])
    db_tool.query_df.return_value = mock_data
    
    agent = RankingAgent(db_tool, settings)
    result = agent.run()
    
    assert len(result['top_trades']) == 2
    assert result['top_trades'][0]['issuer_ticker'] == 'AAPL'
    assert result['top_trades'][0]['net_dollar_value'] == 150000.0
    assert result['top_trades'][0]['dominant_direction'] == 'BUY'
    
    assert result['top_trades'][1]['issuer_ticker'] == 'TSLA'
    assert result['top_trades'][1]['net_dollar_value'] == -100000.0
    assert result['top_trades'][1]['dominant_direction'] == 'SELL'
    
    db_tool.insert_top_trades.assert_called_once()
    
    payload = db_tool.insert_top_trades.call_args[0][0]
    assert len(payload) == 2
