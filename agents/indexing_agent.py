"""Stage 5: turn the day's ranked trades + sentiment into readable context documents for LightRAG."""
import logging
import re

from telemetry.otel_setup import tracer

logger = logging.getLogger(__name__)
_URL_RE = re.compile(r"https?://\S+")


def _usd(x) -> str:
    return f"${abs(float(x)):,.0f}"


class IndexingAgent:
    def __init__(self, lightrag_tool, db_tool, settings=None):
        self.rag, self.db, self.settings = lightrag_tool, db_tool, settings

    def build_blocks(self) -> list[str]:
        trades = self.db.query_df(
            """SELECT * FROM top_trades WHERE run_date = (SELECT MAX(run_date) FROM top_trades) ORDER BY rank""")
        blocks = []
        for t in trades.itertuples():
            lines = [
                f"Insider trading report for {t.issuer_name or t.issuer_ticker} (ticker {t.issuer_ticker}), {t.run_date}.",
                f"Rank {t.rank} by absolute net open-market dollar flow. Dominant direction: {t.dominant_direction}. "
                f"Net {_usd(t.net_dollar_value)} ({_usd(t.buy_value)} bought, {_usd(t.sell_value)} sold) across "
                f"{t.transaction_count} transactions by {t.insider_count} insider(s).",
                f"Largest trader: {t.insider_name} ({t.insider_title}).",
            ]
            if t.signal_tags:
                lines.append("Signals: " + t.signal_tags.replace(",", ", ") + ".")
            s = self.db.query(
                """SELECT SUM(total_tweets), SUM(bullish_count), SUM(bearish_count), SUM(neutral_count),
                          SUM(sentiment_index * total_tweets) / NULLIF(SUM(total_tweets), 0)
                   FROM sentiment_summary WHERE ticker = ?""", [t.issuer_ticker])[0]
            if s[0]:
                lines.append(f"Social sentiment on X for {t.issuer_ticker}: {int(s[0])} tweets analysed — {int(s[1])} bullish, "
                             f"{int(s[2])} bearish, {int(s[3])} neutral; net sentiment index {float(s[4]):+.2f} (range -1 to +1).")
            tweets = self.db.query(
                """SELECT x.text FROM tweets x JOIN tweet_sentiments s USING (tweet_id)
                   WHERE x.ticker = ? ORDER BY x.like_count DESC, x.retweet_count DESC LIMIT 3""", [t.issuer_ticker])
            for (txt,) in tweets:
                lines.append(f'Notable post about {t.issuer_ticker}: "{_URL_RE.sub("", txt).strip()[:200]}"')
            blocks.append("\n".join(lines))
        return blocks

    def run(self) -> dict:
        with tracer.start_as_current_span("indexing_agent.run") as span:
            if not self.rag.enabled:
                return {"indexed_blocks": 0, "skipped": "LightRAG disabled"}
            blocks = self.build_blocks()
            if not blocks:
                return {"indexed_blocks": 0, "skipped": "no ranked trades"}
            if not self.rag.ensure_ready():
                return {"indexed_blocks": 0, "skipped": f"LightRAG unavailable: {self.rag.error}"}
            n = self.rag.insert(blocks)
            span.set_attribute("lightrag.indexed_blocks", n)
            return {"indexed_blocks": n}
