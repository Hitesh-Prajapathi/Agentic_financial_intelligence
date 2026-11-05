"""Chart requests bypass the LLM entirely: pick a chart type from the question and build it from SQL."""
import logging

from telemetry.otel_setup import tracer

logger = logging.getLogger(__name__)


class VisualizationAgent:
    def __init__(self, chart_tool, db_tool, settings):
        self.charts, self.db, self.settings = chart_tool, db_tool, settings

    def run(self, question: str, tickers: list[str]) -> dict:
        with tracer.start_as_current_span("visualization_agent.run") as span:
            low = question.lower()
            try:
                if tickers:
                    tk = tickers[0]
                    if any(w in low for w in ("vs", "versus", "compare", "correlat", "against", "relationship")):
                        trades = self.db.query_df("SELECT run_date, net_dollar_value FROM top_trades WHERE issuer_ticker = ? ORDER BY run_date", [tk])
                        sent = self.db.query_df("SELECT summary_date, sentiment_index FROM sentiment_summary WHERE ticker = ? ORDER BY summary_date", [tk])
                        if not trades.empty or not sent.empty:
                            span.set_attribute("chart.type", "sentiment_vs_trades")
                            return {"figure": self.charts.sentiment_vs_trades(tk, trades, sent), "kind": f"{tk} flow vs sentiment"}
                    else:
                        c = self.db.query_df(
                            """SELECT SUM(bullish_count) bullish_count, SUM(bearish_count) bearish_count, SUM(neutral_count) neutral_count
                               FROM sentiment_summary WHERE ticker = ?""", [tk])
                        if not c.empty and c.iloc[0].notna().all() and c.iloc[0].sum() > 0:
                            span.set_attribute("chart.type", "sentiment_distribution")
                            return {"figure": self.charts.sentiment_distribution(tk, c.iloc[0].to_dict()), "kind": f"{tk} sentiment"}
                trades = self.db.latest_top_trades(self.settings.top_n_trades)
                if trades.empty:
                    return {"figure": None, "error": "No ranked trades yet — run the pipeline or load demo data."}
                span.set_attribute("chart.type", "top_trades_summary")
                note = f"No chart data for {tickers[0]} — showing the market-wide top trades instead." if tickers else None
                return {"figure": self.charts.top_trades_summary(trades), "kind": "top trades", "note": note}
            except Exception as e:  # noqa: BLE001
                logger.error("Visualization failed: %s", e)
                return {"figure": None, "error": f"Could not build chart: {e}"}
