import logging
from telemetry.otel_setup import tracer
from config.settings import settings

class VisualizationAgent:
    def __init__(self, chart_tool, db_tool, settings=settings):
        self.chart_tool = chart_tool
        self.db_tool = db_tool
        self.settings = settings
        self.run_id = None
        self.logger = logging.getLogger(__name__)

    def run(self, intent: dict) -> dict:
        with tracer.start_as_current_span("visualization_agent.run") as span:
            span.set_attribute("agent.name", "VisualizationAgent")
            if self.run_id:
                span.set_attribute("agent.run_id", self.run_id)
                
            query_type = intent.get("query_type", "summary")
            tickers = intent.get("tickers", [])
            
            span.set_attribute("query_type", query_type)
            span.set_attribute("tickers_count", len(tickers))
            
            try:
                filepath = None
                chart_type = None
                
                if query_type == "summary" or (not tickers and query_type in ["entity", "relationship", "comparison"]):
                    # Top trades summary across market
                    trades_df = self.db_tool.query_df("SELECT * FROM top_trades ORDER BY run_date DESC, rank ASC LIMIT ?", [self.settings.top_n_trades])
                    trades = trades_df.to_dict('records')
                    
                    if trades:
                        filepath = self.chart_tool.plot_top_trades_summary(trades)
                        chart_type = "top_trades_summary"
                        
                elif query_type == "comparison" or query_type == "relationship":
                    # Sentiment vs Trades for the first ticker
                    if tickers:
                        ticker = tickers[0]
                        trades_df = self.db_tool.query_df("SELECT run_date, net_dollar_value FROM top_trades WHERE issuer_ticker=?", [ticker])
                        sent_df = self.db_tool.query_df("SELECT summary_date, sentiment_index FROM sentiment_summary WHERE ticker=?", [ticker])
                        
                        trades = trades_df.to_dict('records')
                        sentiment = sent_df.to_dict('records')
                        
                        if trades and sentiment:
                            filepath = self.chart_tool.plot_sentiment_vs_trades(ticker, trades, sentiment)
                            chart_type = "sentiment_vs_trades"
                            
                else: # entity or explicitly sentiment focused
                    if tickers:
                        ticker = tickers[0]
                        sent_df = self.db_tool.query_df(
                            "SELECT SUM(bullish_count) as bullish_count, SUM(bearish_count) as bearish_count, SUM(neutral_count) as neutral_count FROM sentiment_summary WHERE ticker=?", 
                            [ticker]
                        )
                        counts = sent_df.to_dict('records')[0] if not sent_df.empty else {}
                        
                        if counts and not all(v == 0 or v is None for v in counts.values()):
                            counts = {k: v for k, v in counts.items() if v is not None}
                            filepath = self.chart_tool.plot_sentiment_distribution(ticker, counts)
                            chart_type = "sentiment_distribution"
                
                if filepath:
                    return {
                        "chart_path": filepath,
                        "chart_type": chart_type
                    }
                else:
                    return {
                        "chart_path": None,
                        "chart_type": None,
                        "error": "Insufficient data to generate chart"
                    }
                    
            except Exception as e:
                self.logger.error(f"Visualization agent failed: {e}")
                span.set_attribute("error", str(e))
                return {
                    "chart_path": None,
                    "chart_type": None,
                    "error": str(e)
                }
