from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    # API Keys
    sec_api_key: str = Field(default="your_sec_api_key_here")
    apify_api_token: str = Field(default="your_apify_token_here")
    openrouter_api_key: str = Field(default="your_openrouter_key_here")
    
    # Models
    openrouter_model_chat: str = Field(default="openrouter/auto")
    openrouter_model_classify: str = Field(default="mistralai/mixtral-8x7b-instruct")
    lightrag_storage_backend: str = Field(default="networkx")
    lightrag_llm_model: str = Field(default="openrouter/auto")
    lightrag_embedding_model: str = Field(default="text-embedding-3-small")
    
    # Database Configuration
    db_type: str = Field(default="duckdb")
    db_path: str = Field(default="./data/insider_trading.duckdb")
    
    # Pipeline Settings
    polling_interval_minutes: int = Field(default=30)
    top_n_trades: int = Field(default=5)
    tweet_lookback_days: int = Field(default=7)
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
    
    @property
    def filing_lookback_hours(self) -> int:
        return 24
        
    @property
    def chunk_token_size_filings(self) -> int:
        return 800
        
    @property
    def chunk_token_size_tweets(self) -> int:
        return 200

settings = Settings()
