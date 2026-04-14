import os
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from telemetry.otel_setup import tracer
import matplotlib
matplotlib.use('Agg') # Headless backend

class ChartTool:
    def __init__(self, charts_dir="./data/charts"):
        self.charts_dir = charts_dir
        if not os.path.exists(charts_dir):
            os.makedirs(charts_dir)
        self.logger = logging.getLogger(__name__)

    def _get_filepath(self, prefix: str, ticker: str) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{ticker}_{timestamp}_{prefix}.png" if ticker else f"{timestamp}_{prefix}.png"
        return os.path.join(self.charts_dir, filename)

    def plot_sentiment_vs_trades(self, ticker: str, trade_data: list[dict], sentiment_data: list[dict]) -> str:
        with tracer.start_as_current_span("chart.sentiment_vs_trades") as span:
            span.set_attribute("ticker", ticker)
            try:
                # Prepare data
                dates = sorted(list(set([t.get('run_date', t.get('transaction_date')) for t in trade_data] + 
                                      [s.get('summary_date') for s in sentiment_data])))
                if not dates:
                    raise ValueError("No dates found in data")

                trade_dict = {t.get('run_date', t.get('transaction_date')): float(t.get('net_dollar_value', 0)) for t in trade_data}
                sent_dict = {s.get('summary_date'): float(s.get('sentiment_index', 0)) for s in sentiment_data}

                x = np.arange(len(dates))
                y_trades = [trade_dict.get(d, 0) for d in dates]
                y_sent = [sent_dict.get(d, 0) for d in dates]

                fig, ax1 = plt.subplots(figsize=(10, 6))

                color = 'tab:blue'
                ax1.set_xlabel('Date')
                ax1.set_ylabel('Net Trade Value ($)', color=color)
                
                # Colors based on value (green for buy, red for sell)
                bar_colors = ['tab:green' if val >= 0 else 'tab:red' for val in y_trades]
                ax1.bar(x, y_trades, color=bar_colors, alpha=0.6)
                ax1.tick_params(axis='y', labelcolor=color)
                ax1.set_xticks(x)
                ax1.set_xticklabels(dates, rotation=45)

                ax2 = ax1.twinx()
                color = 'tab:orange'
                ax2.set_ylabel('Sentiment Index (-1 to +1)', color=color)
                ax2.plot(x, y_sent, color=color, marker='o', linewidth=2)
                ax2.tick_params(axis='y', labelcolor=color)
                ax2.set_ylim(-1.1, 1.1)
                
                # Add a zero line for sentiment
                ax2.axhline(0, color='gray', linestyle='--', alpha=0.5)

                plt.title(f'Insider Trades vs Sentiment: {ticker}')
                fig.tight_layout()

                filepath = self._get_filepath("sentiment_vs_trades", ticker)
                plt.savefig(filepath, dpi=300, bbox_inches='tight')
                plt.close()

                return filepath
            except Exception as e:
                self.logger.error(f"Plotting failed: {e}")
                span.set_attribute("error", str(e))
                raise

    def plot_sentiment_distribution(self, ticker: str, counts: dict) -> str:
        with tracer.start_as_current_span("chart.sentiment_distribution") as span:
            span.set_attribute("ticker", ticker)
            try:
                labels = ['Bullish', 'Bearish', 'Neutral']
                sizes = [counts.get('bullish_count', 0), counts.get('bearish_count', 0), counts.get('neutral_count', 0)]
                colors = ['#2ca02c', '#d62728', '#7f7f7f']

                if sum(sizes) == 0:
                    raise ValueError("No sentiment data to plot")

                fig, ax = plt.subplots(figsize=(8, 8))
                ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
                ax.axis('equal') 
                plt.title(f'Twitter Sentiment Distribution: {ticker}')

                filepath = self._get_filepath("sentiment_distribution", ticker)
                plt.savefig(filepath, dpi=300, bbox_inches='tight')
                plt.close()

                return filepath
            except Exception as e:
                self.logger.error(f"Plotting failed: {e}")
                span.set_attribute("error", str(e))
                raise

    def plot_top_trades_summary(self, trades: list[dict]) -> str:
        with tracer.start_as_current_span("chart.top_trades_summary") as span:
            try:
                if not trades:
                    raise ValueError("No trade data to plot")

                # Parse trades
                tickers_names = [f"{t.get('issuer_ticker')}\n({t.get('insider_name')})" for t in trades]
                values = [float(t.get('net_dollar_value', 0)) for t in trades]

                fig, ax = plt.subplots(figsize=(10, 6))

                # Horizontal bar chart
                y_pos = np.arange(len(tickers_names))
                colors = ['tab:green' if val >= 0 else 'tab:red' for val in values]
                
                ax.barh(y_pos, values, color=colors, align='center')
                ax.set_yticks(y_pos)
                ax.set_yticklabels(tickers_names)
                ax.invert_yaxis()  # top ranks at top
                ax.set_xlabel('Net Trade Value ($)')
                ax.set_title('Top Insider Trades Summary')
                
                # Format x-axis with commas
                ax.get_xaxis().set_major_formatter(
                    matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

                # Add a vertical zero line
                ax.axvline(0, color='gray', linewidth=0.8)

                fig.tight_layout()

                filepath = self._get_filepath("top_trades_summary", "market")
                plt.savefig(filepath, dpi=300, bbox_inches='tight')
                plt.close()

                return filepath
            except Exception as e:
                self.logger.error(f"Plotting failed: {e}")
                span.set_attribute("error", str(e))
                raise
