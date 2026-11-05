"""Interactive Plotly charts (replaces the old matplotlib PNG files)."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

GREEN, RED, GREY, BLUE = "#16a34a", "#dc2626", "#94a3b8", "#2563eb"
_LAYOUT = dict(template="plotly_white", margin=dict(l=10, r=10, t=50, b=10), height=420,
               font=dict(size=13), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")


def _money(v: float) -> str:
    a = abs(v)
    s = f"{a / 1e9:.2f}B" if a >= 1e9 else f"{a / 1e6:.2f}M" if a >= 1e6 else f"{a / 1e3:.1f}K"
    return ("-" if v < 0 else "+") + "$" + s


class ChartTool:
    def top_trades_summary(self, trades: pd.DataFrame) -> go.Figure:
        if trades.empty:
            raise ValueError("No trade data to plot")
        t = trades.sort_values("rank")
        labels = [f"{r.issuer_ticker} · {str(r.insider_name)[:28]}" for r in t.itertuples()]
        fig = go.Figure(go.Bar(
            y=labels, x=t["net_dollar_value"], orientation="h",
            marker_color=[GREEN if v > 0 else RED for v in t["net_dollar_value"]],
            text=[_money(v) for v in t["net_dollar_value"]], textposition="outside", cliponaxis=False,
            customdata=t[["buy_value", "sell_value", "insider_count", "signal_tags"]].values,
            hovertemplate="<b>%{y}</b><br>Net %{text}<br>Bought $%{customdata[0]:,.0f} · Sold $%{customdata[1]:,.0f}"
                          "<br>Insiders: %{customdata[2]}<br>%{customdata[3]}<extra></extra>"))
        fig.update_yaxes(autorange="reversed")
        fig.update_xaxes(title="Net open-market $ (green = buying, red = selling)", zeroline=True, zerolinecolor=GREY)
        fig.update_layout(title="Top insider trades (24h)", **_LAYOUT)
        return fig

    def buy_sell_breakdown(self, trades: pd.DataFrame) -> go.Figure:
        if trades.empty:
            raise ValueError("No trade data to plot")
        t = trades.sort_values("rank")
        fig = go.Figure([go.Bar(name="Bought", x=t["issuer_ticker"], y=t["buy_value"], marker_color=GREEN),
                         go.Bar(name="Sold", x=t["issuer_ticker"], y=t["sell_value"], marker_color=RED)])
        fig.update_layout(barmode="group", title="Gross bought vs sold", yaxis_title="$", **_LAYOUT)
        return fig

    def sentiment_distribution(self, ticker: str, counts: dict) -> go.Figure:
        vals = [counts.get("bullish_count", 0), counts.get("bearish_count", 0), counts.get("neutral_count", 0)]
        if sum(vals) == 0:
            raise ValueError("No sentiment data to plot")
        fig = go.Figure(go.Pie(labels=["Bullish", "Bearish", "Neutral"], values=vals, hole=0.55,
                               marker_colors=[GREEN, RED, GREY], sort=False))
        fig.update_layout(title=f"X/Twitter sentiment for {ticker} ({int(sum(vals))} tweets)", **_LAYOUT)
        return fig

    def sentiment_vs_trades(self, ticker: str, trades: pd.DataFrame, sentiment: pd.DataFrame) -> go.Figure:
        if trades.empty and sentiment.empty:
            raise ValueError("No data to plot")
        fig = go.Figure()
        if not trades.empty:
            fig.add_bar(x=trades["run_date"], y=trades["net_dollar_value"], name="Net insider $",
                        marker_color=[GREEN if v > 0 else RED for v in trades["net_dollar_value"]], opacity=0.6)
        if not sentiment.empty:
            fig.add_scatter(x=sentiment["summary_date"], y=sentiment["sentiment_index"], name="Sentiment index",
                            yaxis="y2", mode="lines+markers", line=dict(color=BLUE, width=3))
        fig.update_layout(title=f"{ticker}: insider flow vs social sentiment", yaxis=dict(title="Net $"),
                          yaxis2=dict(title="Sentiment (-1…+1)", overlaying="y", side="right", range=[-1.1, 1.1]), **_LAYOUT)
        return fig

    def sentiment_gauge(self, value: float) -> go.Figure:
        fig = go.Figure(go.Indicator(mode="gauge+number", value=value, number={"valueformat": "+.2f"},
                                     gauge={"axis": {"range": [-1, 1]}, "bar": {"color": BLUE},
                                            "steps": [{"range": [-1, -0.2], "color": "#fecaca"},
                                                      {"range": [-0.2, 0.2], "color": "#e2e8f0"},
                                                      {"range": [0.2, 1], "color": "#bbf7d0"}]}))
        fig.update_layout(height=170, margin=dict(l=10, r=10, t=10, b=0), paper_bgcolor="rgba(0,0,0,0)")
        return fig
