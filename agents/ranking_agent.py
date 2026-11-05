"""Stage 2: deterministic ranking. Computes only; persisting happens after the governance gate."""
import logging
from datetime import datetime, timedelta, timezone

from agents.symbolic_validation import validate_and_rank
from telemetry.otel_setup import tracer

logger = logging.getLogger(__name__)

QUERY = """
SELECT t.accession_number, t.row_idx, t.transaction_date, t.code, t.acquired_disposed, t.shares,
       t.price_per_share, t.post_transaction_shares,
       f.issuer_ticker, f.issuer_name, f.insider_name, f.insider_title,
       f.is_officer, f.is_director, f.is_10b5_1, f.filing_date
FROM transactions t JOIN filings f USING (accession_number)
WHERE f.filing_date >= ? AND t.code IN ('P', 'S')
"""


class RankingAgent:
    def __init__(self, db_tool, settings):
        self.db, self.settings = db_tool, settings

    def run(self) -> dict:
        with tracer.start_as_current_span("ranking_agent.run") as span:
            window_start = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=self.settings.filing_lookback_hours)
            df = self.db.query_df(QUERY, [window_start])
            ranked, checksum, stats = validate_and_rank(df, self.settings.top_n_trades, self.settings.min_trade_notional)
            trades = []
            if not ranked.empty:
                trades = ranked.drop(columns=["abs_net"]).to_dict("records")
                for t in trades:
                    t["validation_checksum"] = checksum
            span.set_attribute("trades.ranked_count", len(trades))
            return {"ranked_df": ranked, "checksum": checksum, "stats": stats,
                    "window_start": window_start, "top_trades": trades}

    def persist(self, run_id: str, result: dict) -> int:
        trades = result["top_trades"]
        if trades:
            self.db.replace_top_trades(run_id, datetime.now(timezone.utc).strftime("%Y-%m-%d"), trades)
        return len(trades)
