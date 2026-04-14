import pytest
from unittest.mock import MagicMock
from agents.social_scraper_agent import SocialScraperAgent
from tools.apify_tool import ApifyTool
from config.settings import settings
import json

@pytest.fixture
def sample_tweets():
    with open('tests/fixtures/sample_tweets.json', 'r') as f:
        return json.load(f)

def test_apify_tool_normalization(sample_tweets):
    tool = ApifyTool()
    tool.client = MagicMock()
    
    # Mocking ApifyClient actor return
    mock_run = {'status': 'SUCCEEDED', 'defaultDatasetId': 'abc'}
    tool.client.actor().call.return_value = mock_run
    tool.client.dataset().iterate_items.return_value = sample_tweets
    
    tweets = tool.scrape_tweets("$AAPL")
    assert len(tweets) == 2
    assert tweets[0]['tweet_id'] == "1234567890"
    assert tweets[0]['author_username'] == "trader_bob"
    assert tweets[0]['like_count'] == 150

def test_social_scraper_agent():
    apify_tool = MagicMock()
    apify_tool.scrape_tweets.return_value = [
        {
            "tweet_id": "123",
            "text": "test tweet",
            "author_username": "test_user",
            "created_at": "2024-04-14T12:00:00Z",
            "retweet_count": 0,
            "like_count": 0,
            "reply_count": 0,
            "raw_json": {}
        }
    ]
    
    db_tool = MagicMock()
    db_tool.query.return_value = [("Apple Inc.",)]
    
    agent = SocialScraperAgent(apify_tool, db_tool, settings)
    
    top_trades = [{'issuer_ticker': 'AAPL'}]
    result = agent.run(top_trades)
    
    assert result['tickers_scraped'] == 1
    assert result['total_tweets'] == 1
    assert result['per_ticker']['AAPL'] == 1
    
    # Assert query formulation: $TICKER OR "Company Name"
    apify_tool.scrape_tweets.assert_called_once()
    query_used = apify_tool.scrape_tweets.call_args[1]['query']
    assert query_used == '$AAPL OR "Apple Inc."'
    
    db_tool.insert_tweets.assert_called_once()
    tweets_inserted = db_tool.insert_tweets.call_args[0][0]
    assert tweets_inserted[0]['ticker'] == 'AAPL'

def test_social_scraper_agent_empty_top_trades():
    apify_tool = MagicMock()
    db_tool = MagicMock()
    agent = SocialScraperAgent(apify_tool, db_tool, settings)
    
    result = agent.run([])
    assert result['tickers_scraped'] == 0
    assert result['total_tweets'] == 0
    apify_tool.scrape_tweets.assert_not_called()
