"""Central configuration. Every value can be overridden through environment variables / .env."""
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PLACEHOLDER_MARKERS = ("your_", "_here", "changeme")


def _is_placeholder(value: str) -> bool:
    v = (value or "").strip().lower()
    return (not v) or any(m in v for m in PLACEHOLDER_MARKERS)


class Settings(BaseSettings):
    # ── API keys ────────────────────────────────────────────────────────────
    sec_api_key: str = ""
    apify_api_token: str = ""
    openrouter_api_key: str = ""
    groq_api_key: str = ""

    # ── LLM chain (free models only) ────────────────────────────────────────
    # Tried in order; the first that answers wins. Format: provider:model
    llm_chain: str = (
        "groq:openai/gpt-oss-120b,"
        "groq:qwen/qwen3.8-27b,"
        "openrouter:nvidia/nemotron-3-ultra-550b-a55b:free,"
        "openrouter:recursive/flint-v1.0:free,"
        "openrouter:google/gemma-4-31b-it:free"
    )
    allow_paid_models: bool = False  # hard guard: OpenRouter models must end in ":free"
    embedding_provider: str = "local"                        # local (fastembed/ONNX, no limits) | openrouter
    local_embedding_model: str = "BAAI/bge-small-en-v1.5"   # 384-dim, ~67 MB, downloaded once to ./data/models
    embedding_model: str = "nvidia/nemotron-3.5-embed-1b:free"  # used only when embedding_provider=openrouter
    models_dir: str = "./data/models"
    llm_timeout_s: float = 60.0

    # ── LightRAG (switchable) ───────────────────────────────────────────────
    enable_lightrag: bool = True
    lightrag_query_timeout_s: float = 20.0
    lightrag_index_timeout_s: float = 300.0

    # ── Storage ─────────────────────────────────────────────────────────────
    live_db_path: str = "./data/live.duckdb"
    demo_db_path: str = "./data/demo.duckdb"
    charts_dir: str = "./data/charts"
    lightrag_dir: str = "./data/lightrag"

    # ── Pipeline parameters ─────────────────────────────────────────────────
    filing_lookback_hours: int = 24
    sec_max_pages: int = 8              # 50 filings per page
    top_n_trades: int = 5
    min_trade_notional: float = 10_000  # ignore tickers with less gross open-market $ than this
    tweet_lookback_days: int = 7
    tweets_per_ticker: int = 40
    tweet_cache_hours: float = 6.0      # skip re-scraping a ticker scraped more recently than this
    empty_scrape_cooldown_hours: float = 2.0  # Apify bills empty runs too, so do not retry an empty ticker immediately
    apify_actor_id: str = "apidojo/tweet-scraper"
    apify_monthly_budget_usd: float = 2.0
    apify_price_per_tweet_usd: float = 0.0004
    apify_run_timeout_s: int = 120
    sentiment_batch_size: int = 30
    low_confidence_threshold: float = 0.6

    # ── Telemetry ───────────────────────────────────────────────────────────
    otel_console: bool = False          # print every span to the terminal

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── Derived helpers ─────────────────────────────────────────────────────
    @property
    def chain(self) -> list[tuple[str, str]]:
        out = []
        for item in self.llm_chain.split(","):
            item = item.strip()
            if not item or ":" not in item:
                continue
            provider, model = item.split(":", 1)
            out.append((provider.strip().lower(), model.strip()))
        return out

    def has_key(self, name: str) -> bool:
        return not _is_placeholder(getattr(self, name, ""))

    @model_validator(mode="after")
    def _enforce_free_models(self):
        if not self.allow_paid_models:
            for provider, model in self.chain:
                if provider == "openrouter" and not model.endswith(":free"):
                    raise ValueError(
                        f"Refusing paid OpenRouter model '{model}'. Use a ':free' model "
                        "or set ALLOW_PAID_MODELS=true."
                    )
            if self.embedding_provider == "openrouter" and not self.embedding_model.endswith(":free"):
                raise ValueError(
                    f"Refusing paid embedding model '{self.embedding_model}'. Use a ':free' model "
                    "or set ALLOW_PAID_MODELS=true."
                )
        return self


settings = Settings()
