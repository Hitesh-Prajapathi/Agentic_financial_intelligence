"""Builds the evidence for a question: authoritative SQL facts, each with a citation tag.

Every number the chat model sees comes from SQL rows computed deterministically upstream.
No LLM call is made here (routing is rule-based) to save latency and quota.
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone

from insider_intel.chat.grounding import citation_tags  # noqa: F401  (re-exported: tests and service import it from here)
from insider_intel.obs import timed

logger = logging.getLogger(__name__)
_TOKEN = re.compile(r"\$?\b([A-Z]{1,5})\b")
_STOP = {"I", "A", "THE", "AND", "OR", "ON", "IT", "AT", "IS", "AI", "US", "CEO", "CFO", "SEC", "USD", "BUY", "SELL", "PLAN", "ALL"}


def _usd(x) -> str:
    return f"${abs(float(x)):,.0f}"


class Retriever:
    def __init__(self, db_tool, settings):
        self.db, self.settings = db_tool, settings
        self._tickers_cache: tuple[float, dict] | None = None

    def invalidate(self) -> None:
        """Drop cached ticker lookups (call after new data is published)."""
        self._tickers_cache = None

    # ── entity detection ────────────────────────────────────────────────────
    def known_tickers(self) -> dict[str, str]:
        """{TICKER: company name} for everything we hold data on (cached 60 s)."""
        if self._tickers_cache and time.time() - self._tickers_cache[0] < 60:
            return self._tickers_cache[1]
        rows = self.db.query("SELECT DISTINCT issuer_ticker, issuer_name FROM filings WHERE issuer_ticker IS NOT NULL")
        rows += self.db.query("SELECT DISTINCT issuer_ticker, issuer_name FROM top_trades")
        d = {r[0]: (r[1] or "") for r in rows if r[0]}
        self._tickers_cache = (time.time(), d)
        return d

    def extract_tickers(self, question: str) -> list[str]:
        known = self.known_tickers()
        found = []
        for m in _TOKEN.finditer(question):
            tk = m.group(1)
            explicit = m.group(0).startswith("$")
            if tk in known and (explicit or tk not in _STOP) and tk not in found:
                found.append(tk)
        low = question.lower()
        for tk, name in known.items():  # company names, e.g. "dutch bros"
            short = re.sub(r"[,.]?\s+(inc|corp|co|ltd|plc|holdings?|group)\b\.?", "", name, flags=re.I).strip().lower()
            if len(short) >= 4 and short in low and tk not in found:
                found.append(tk)
        return found[:3]

    # ── SQL evidence ────────────────────────────────────────────────────────
    def data_as_of(self) -> datetime | None:
        return self.db.data_as_of()

    def sql_context(self, tickers: list[str]) -> tuple[str, int]:
        parts, rows_used = [], 0
        trades = self.db.latest_top_trades(10)
        if not trades.empty:
            lines = []
            for t in trades.itertuples():
                tags = f" signals={t.signal_tags}" if t.signal_tags else ""
                pct = f", largest trade = {t.pct_holdings:.0%} of that insider's position" if t.pct_holdings == t.pct_holdings and t.pct_holdings is not None else ""
                lines.append(
                    f"#{t.rank} {t.issuer_ticker} ({t.issuer_name}): {t.dominant_direction}, net {_usd(t.net_dollar_value)} "
                    f"(bought {_usd(t.buy_value)}, sold {_usd(t.sell_value)}), {t.transaction_count} trades by {t.insider_count} insider(s); "
                    f"lead: {t.insider_name} ({t.insider_title}){pct}; {t.planned_pct:.0%} of $ under 10b5-1 plan;{tags} "
                    f"[RANK {str(t.run_date)[:10]} #{t.rank}]")
            parts.append("TOP INSIDER TRADES (open-market buys/sells, ranked by absolute net $):\n" + "\n".join(lines))
            rows_used += len(trades)

        has_flag = bool(self.db.query("SELECT 1 FROM information_schema.columns WHERE table_name='transactions' "
                                      "AND column_name='superseded' LIMIT 1"))  # always true after migrations; old DBs may lack it
        live = "AND COALESCE(t.superseded, FALSE) = FALSE" if has_flag else ""
        focus = tickers or ([] if trades.empty else list(trades.issuer_ticker[:5]))
        for tk in focus:
            df = self.db.query_df(
                f"""SELECT f.accession_number, f.filing_date, f.insider_name, f.insider_title, f.is_10b5_1,
                          t.transaction_date, t.code, t.shares, t.price_per_share
                   FROM transactions t JOIN filings f USING (accession_number)
                   WHERE f.issuer_ticker = ? AND f.filing_date >= ? AND t.code IN ('P','S')
                     {live}
                   ORDER BY t.shares * t.price_per_share DESC LIMIT 6""",
                [tk, datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=self.settings.rank_window_days)])
            if not df.empty:
                lines = [f"{'BUY' if r.code == 'P' else 'SELL'} {r.shares:,.0f} sh @ ${r.price_per_share:,.2f} = {_usd(r.shares * r.price_per_share)} "
                         f"by {r.insider_name} ({r.insider_title}) on {r.transaction_date}{' [10b5-1 plan]' if r.is_10b5_1 else ''} "
                         f"[SEC Form 4 {r.accession_number}]" for r in df.itertuples()]
                parts.append(f"LARGEST RECENT FILINGS FOR {tk}:\n" + "\n".join(lines))
                rows_used += len(df)
            s = self.db.query(
                """SELECT SUM(total_tweets), SUM(bullish_count), SUM(bearish_count), SUM(neutral_count),
                          SUM(sentiment_index * total_tweets) / NULLIF(SUM(total_tweets), 0), MIN(summary_date), MAX(summary_date)
                   FROM sentiment_summary WHERE ticker = ?""", [tk])[0]
            if s[0]:
                parts.append(f"SOCIAL SENTIMENT FOR {tk} (X/Twitter, AI-classified): {int(s[0])} tweets {s[5]}→{s[6]}: "
                             f"{int(s[1])} bullish / {int(s[2])} bearish / {int(s[3])} neutral; net index {float(s[4]):+.2f} "
                             f"(-1 bearish … +1 bullish) [SENT {tk}]")
                rows_used += 1
        return "\n\n".join(parts), rows_used

    # ── main entry ──────────────────────────────────────────────────────────
    def run(self, question: str) -> dict:
        with timed("retrieval_agent.run") as span:
            tickers = self.extract_tickers(question)
            sql_ctx, rows = self.sql_context(tickers)

            as_of = self.data_as_of()
            age_h = (datetime.now(timezone.utc).replace(tzinfo=None) - as_of).total_seconds() / 3600 if as_of else None
            win_h = 24.0 * float(self.settings.rank_window_days)  # data is refreshed ~twice a month
            unknown = [m for m in re.findall(r"\$([A-Za-z]{1,5})\b", question) if m.upper() not in self.known_tickers()]
            if unknown:
                sql_ctx += f"\n\nNOTE: no data is held for: {', '.join('$' + u.upper() for u in unknown)}."
            if rows == 0:
                conf, why = "low", "no data loaded"
            elif unknown and not tickers:
                conf, why = "low", f"no data for {', '.join(u.upper() for u in unknown)}"
            elif age_h is not None and age_h > 2 * win_h:
                conf, why = "low", f"data is {age_h / 24:.0f} days old"
            elif (age_h is not None and age_h > win_h) or rows < 3:
                conf, why = "moderate", "limited or ageing data"
            else:
                conf, why = "high", "fresh data with multiple matching rows"
            span.set(retrieval_rows=rows)
            span.set(retrieval_confidence=conf)
            return {"tickers": tickers, "sql_context": sql_ctx, "rows_used": rows, "confidence": conf,
                    "confidence_reason": why, "as_of": as_of, "known_sources": citation_tags(sql_ctx)}
