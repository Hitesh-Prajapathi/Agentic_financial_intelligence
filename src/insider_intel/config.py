"""Central configuration. Every value can be overridden through environment variables / .env."""

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PLACEHOLDER_MARKERS = ("your_", "_here", "changeme")


def _is_placeholder(value: str) -> bool:
    v = (value or "").strip().lower()
    return (not v) or any(m in v for m in PLACEHOLDER_MARKERS)


class Settings(BaseSettings):
    # ── SEC EDGAR (no API key; a descriptive User-Agent with contact info is mandatory) ──────────────
    sec_user_agent: str = ""  # env SEC_USER_AGENT, e.g. "your-app-name your-email@example.com"

    # ── API keys ────────────────────────────────────────────────────────────
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
    llm_timeout_s: float = 60.0

    # ── Chat agent ──────────────────────────────────────────────────────────
    chat_agent_enabled: bool = True  # False -> always use the degraded context-stuffing path
    chat_max_tool_rounds: int = 4

    # ── Telemetry: NOTIONAL cost ────────────────────────────────────────────
    # ESTIMATE ONLY: USD per 1M tokens as (input, output) at approximate public list prices, keyed by model name.
    # Only free-tier models are actually used, so nothing is billed; the llm_calls.est_cost_usd column answers
    # "what would this usage cost at paid rates". Models not listed cost 0. Override with LLM_LIST_PRICES='{"model":[in,out]}'.
    llm_list_prices: dict[str, tuple[float, float]] = {
        "openai/gpt-oss-120b": (0.15, 0.60),  # ESTIMATE
    }

    # ── Storage ─────────────────────────────────────────────────────────────
    app_read_only: bool = False  # hosted UI: open DuckDB read-only, no pipeline / Refresh button
    live_db_path: str = "./data/live.duckdb"
    demo_db_path: str = "./data/demo.duckdb"

    # ── Pipeline parameters ─────────────────────────────────────────────────
    rank_window_days: int = 14  # ranking/gate window, measured on EDGAR acceptance time
    edgar_cache_dir: str = "./data/raw/edgar"
    edgar_initial_lookback_days: int = 14  # first run (no watermark) starts this many days back
    edgar_max_filings_per_run: int = 0  # 0 = unlimited (scheduled catch-up of ~2 weeks is ~10-15k filings)
    edgar_first_run_max_filings: int = 20_000  # safety cap used only when there is no watermark yet
    edgar_max_runtime_minutes: int = 150  # stop cleanly before the CI job timeout; 0 = no limit
    top_n_trades: int = 5
    min_trade_notional: float = 10_000  # ignore tickers with less gross open-market $ than this

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

    @property
    def filing_lookback_hours(self) -> int:
        """Back-compat alias (hours) for callers written against the old 24h window."""
        return self.rank_window_days * 24

    @property
    def has_sec_user_agent(self) -> bool:
        ua = (self.sec_user_agent or "").strip().lower()
        return bool(ua) and not any(
            m in ua for m in ("your-app-name", "your-email", "example.com", *PLACEHOLDER_MARKERS)
        )

    def has_key(self, name: str) -> bool:
        return not _is_placeholder(getattr(self, name, ""))

    @model_validator(mode="after")
    def _enforce_free_models(self):
        if not self.allow_paid_models:
            for provider, model in self.chain:
                if provider == "openrouter" and not model.endswith(":free"):
                    raise ValueError(
                        f"Refusing paid OpenRouter model '{model}'. Use a ':free' model or set ALLOW_PAID_MODELS=true."
                    )
        return self


settings = Settings()
