"""Typed, read-only, parameterized SQL tools for the chat agent (plan B3).

Design rules
  * No free-form text-to-SQL: each tool is a fixed query; user/model text only ever travels as a bound parameter.
  * Only SELECT statements are issued (the DB handle is used read-only).
  * Every result carries counts/totals (the model must not count or sum) and a hard row cap.
  * Every row carries a stable ``source`` id the answer cites, e.g. ``RANK 2026-11-05 #1``, ``SEC Form 4 <accession>``,
    ``SENT <TICKER>``, ``GRAPH <owner_cik>``, ``CLUSTER <TICKER> BUY 2026-11-03``, ``RUN <run_id>``.
  * Windows ("days") end at the newest filing date in the database, not at wall-clock time, so answers over a frozen
    snapshot are reproducible.
  * Tools for tables/columns that may not exist yet (``reporting_owners``, ``insider_edges``, ``superseded``) degrade to
    a clear ``available: false`` result or to a name-based fallback.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

MAX_RESULT_CHARS = 6000  # keep tool messages small: the free tier is ~8k tokens per minute
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


# ── context & helpers ────────────────────────────────────────────────────────
@dataclass
class ToolContext:
    db: Any  # needs .query(sql, params) -> list[tuple] and .query_df(sql, params) -> DataFrame
    rank_window_days: int | None = None  # settings.rank_window_days (ranking window), shown by data_status
    now: Callable[[], datetime] = field(
        default=lambda: datetime.utcnow()
    )  # injectable clock (naive UTC), used only for data age

    def has_table(self, name: str) -> bool:
        return bool(self.db.query("SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1", [name]))

    def columns(self, table: str) -> set[str]:
        return {
            r[0]
            for r in self.db.query("SELECT column_name FROM information_schema.columns WHERE table_name = ?", [table])
        }

    def anchor(self) -> date | None:
        """Newest filing date in the data: the right edge of every ``days`` window."""
        if not self.has_table("filings"):
            return None
        v = self.db.query("SELECT CAST(MAX(filing_date) AS DATE) FROM filings")[0][0]
        return _to_date(v)

    def rows(self, sql: str, params: list | None = None) -> list[dict]:
        df = self.db.query_df(sql, params or [])
        return [{k: _clean(v) for k, v in rec.items()} for rec in df.to_dict("records")]

    def scalar(self, sql: str, params: list | None = None):
        r = self.db.query(sql, params or [])
        return _clean(r[0][0]) if r else None


def _to_date(v) -> date | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return datetime.fromisoformat(str(v)[:10]).date()
    except ValueError:
        return None


def _clean(v):
    """Make a DB value JSON-friendly and compact: dates as YYYY-MM-DD, floats rounded, NaN -> None."""
    if v is None:
        return None
    if hasattr(v, "tolist") and not isinstance(v, (str, bytes)):  # numpy scalar / array
        v = v.tolist()
    if isinstance(v, float):
        return None if math.isnan(v) or math.isinf(v) else round(v, 4)
    if isinstance(v, (datetime, date)):
        return v.date().isoformat() if isinstance(v, datetime) else v.isoformat()
    if hasattr(v, "isoformat"):  # pandas Timestamp / NaT
        try:
            return v.date().isoformat()
        except Exception:  # noqa: BLE001 - NaT
            return None
    if isinstance(v, (list, tuple)):
        return [_clean(x) for x in v]
    return v


def _like_tokens(col: str, text: str) -> tuple[str, list[str]]:
    """Every word of the owner name must appear (so 'George Kurtz' finds 'Kurtz George'). Fully parameterized."""
    words = [w for w in re.split(r"[\s,.]+", text.strip()) if w][:4]
    if not words:
        return "FALSE", []
    esc = [w.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") for w in words]
    return " AND ".join(f"{col} ILIKE ? ESCAPE '\\'" for _ in esc), [f"%{w}%" for w in esc]


def _window(ctx: ToolContext, days: int) -> tuple[date | None, date | None]:
    end = ctx.anchor()
    return (end - timedelta(days=days - 1), end) if end else (None, None)


def _result(rows: list[dict], total: int | None = None, **extra) -> dict:
    out = {"rows": rows, "row_count": len(rows), "total_matched": len(rows) if total is None else int(total)}
    out["truncated"] = out["total_matched"] > len(rows)
    out.update({k: v for k, v in extra.items() if v is not None})
    if not rows:
        out.setdefault("note", "No matching rows in the database.")
    return out


def _unavailable(what: str) -> dict:
    return {
        "rows": [],
        "row_count": 0,
        "total_matched": 0,
        "truncated": False,
        "available": False,
        "note": f"{what} is not available in this database yet.",
    }


# ── argument models ──────────────────────────────────────────────────────────
class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


def _ticker(v):
    if v is None:
        return v
    v = str(v).strip().lstrip("$").upper()
    if not _TICKER_RE.match(v):
        raise ValueError(f"'{v}' is not a valid ticker symbol")
    return v


class TopTradesArgs(_Args):
    date: str | None = Field(None, description="Ranking date YYYY-MM-DD. Omit for the latest ranking.")
    days: int | None = Field(
        None, ge=1, le=90, description="Include rankings from the last N days of ranking history instead of one date."
    )
    direction: Literal["BUY", "SELL"] | None = Field(None, description="Only buys or only sells.")
    n: int = Field(5, ge=1, le=20, description="How many ranked trades to return.")

    @field_validator("date")
    @classmethod
    def _date(cls, v):
        if v is not None:
            datetime.strptime(v, "%Y-%m-%d")
        return v

    @field_validator("direction", mode="before")
    @classmethod
    def _dir(cls, v):
        return v.upper() if isinstance(v, str) else v

    @model_validator(mode="after")
    def _exclusive(self):
        if self.date and self.days:
            raise ValueError("give either 'date' or 'days', not both")
        return self


class FilingsForArgs(_Args):
    ticker: str | None = Field(None, description="Issuer ticker, e.g. BROS.")
    owner: str | None = Field(None, description="Insider name (any word order) or reporting-owner CIK.")
    days: int = Field(
        14,
        ge=1,
        le=365,
        description="Window length in days, ending at the newest filing in the data (default 14, the ranking window).",
    )
    codes: list[str] = Field(
        default_factory=lambda: ["P", "S"],
        description='Transaction codes, default ["P","S"]: P open-market buy, S open-market sale, others if stored (A, M, F, G).',
    )
    limit: int = Field(15, ge=1, le=30)

    @field_validator("ticker", mode="before")
    @classmethod
    def _tk(cls, v):
        return _ticker(v)

    @field_validator("codes", mode="before")
    @classmethod
    def _codes(cls, v):
        if isinstance(v, str):
            v = re.split(r"[,\s/]+", v)
        out = [str(c).strip().upper() for c in v if str(c).strip()]
        if not out or any(not re.fullmatch(r"[A-Z]", c) for c in out):
            raise ValueError("codes must be single letters such as ['P','S']")
        return out

    @model_validator(mode="after")
    def _need_one(self):
        if not (self.ticker or self.owner):
            raise ValueError("provide 'ticker' or 'owner'")
        return self


class InsiderHistoryArgs(_Args):
    owner: str = Field(..., min_length=2, description="Insider name (any word order) or reporting-owner CIK.")
    limit: int = Field(20, ge=1, le=30)


class ClusterEventsArgs(_Args):
    days: int = Field(30, ge=1, le=365)
    min_insiders: int = Field(
        2, ge=2, le=20, description="Minimum number of distinct insiders trading the same way at one company."
    )
    direction: Literal["BUY", "SELL"] | None = None
    limit: int = Field(10, ge=1, le=20)

    @field_validator("direction", mode="before")
    @classmethod
    def _dir(cls, v):
        return v.upper() if isinstance(v, str) else v


class InsiderNetworkArgs(_Args):
    owner: str = Field(..., min_length=2, description="Insider name (any word order) or reporting-owner CIK.")


class CoInsidersArgs(_Args):
    ticker: str
    days: int = Field(90, ge=1, le=365)
    limit: int = Field(15, ge=1, le=30)

    @field_validator("ticker", mode="before")
    @classmethod
    def _tk(cls, v):
        return _ticker(v)


class SentimentArgs(_Args):
    ticker: str

    @field_validator("ticker", mode="before")
    @classmethod
    def _tk(cls, v):
        return _ticker(v)


class DataStatusArgs(_Args):
    pass


# ── SQL fragments ────────────────────────────────────────────────────────────
def _value_expr() -> str:
    return "COALESCE(t.dollar_value, t.shares * t.price_per_share)"


def _live(ctx: ToolContext) -> str:
    return " AND NOT COALESCE(t.superseded, FALSE)" if "superseded" in ctx.columns("transactions") else ""


def _owner_filter(ctx: ToolContext, owner: str) -> tuple[str, list]:
    """SQL condition on alias `f` (filings) matching an insider by name, or by CIK when `owner` is numeric."""
    if re.fullmatch(r"\d{4,10}", owner.strip()):
        cik = owner.strip().zfill(10)
        parts: list[str] = []
        params: list = []
        if "owner_ciks" in ctx.columns("filings"):
            parts.append("f.owner_ciks LIKE ?")
            params.append(f"%{cik}%")
        if ctx.has_table("reporting_owners"):
            parts.append(
                "EXISTS (SELECT 1 FROM reporting_owners ro WHERE ro.accession_number = f.accession_number AND ro.owner_cik IN (?, ?))"
            )
            params += [cik, owner.strip()]
        return ("(" + " OR ".join(parts) + ")", params) if parts else ("FALSE", [])
    cond, params = _like_tokens("f.insider_name", owner)
    if ctx.has_table("reporting_owners"):  # joint filers: the name may belong to a co-owner
        c2, p2 = _like_tokens("ro.owner_name", owner)
        return (
            f"(({cond}) OR EXISTS (SELECT 1 FROM reporting_owners ro WHERE ro.accession_number = f.accession_number AND {c2}))",
            params + p2,
        )
    return f"({cond})", params


_TXN_COLS = (
    f"f.accession_number, CAST(f.filing_date AS DATE) AS filed, f.issuer_ticker AS ticker, f.issuer_name AS company, "
    f"f.insider_name AS insider, f.insider_title AS title, f.is_10b5_1 AS under_10b5_1, f.form_type, "
    f"t.transaction_date AS trade_date, t.code, t.shares, t.price_per_share AS price, {_value_expr()} AS dollar_value"
)


def _with_source(rows: list[dict]) -> list[dict]:
    for r in rows:
        r["source"] = f"SEC Form 4 {r.pop('accession_number')}"
    return rows


# ── tools ────────────────────────────────────────────────────────────────────
def top_trades(ctx: ToolContext, a: TopTradesArgs) -> dict:
    if not ctx.has_table("top_trades"):
        return _unavailable("The ranking")
    latest = _to_date(ctx.scalar("SELECT MAX(run_date) FROM top_trades"))
    if latest is None:
        return _result([], note="No ranking has been published yet.")
    where, params = [], []
    if a.date:
        where.append("run_date = ?")
        params.append(a.date)
    elif a.days:
        where.append("run_date BETWEEN ? AND ?")
        params += [latest - timedelta(days=a.days - 1), latest]
    else:
        where.append("run_date = ?")
        params.append(latest)
    if a.direction:
        where.append("dominant_direction = ?")
        params.append(a.direction)
    w = " AND ".join(where)
    total = ctx.scalar(f"SELECT COUNT(*) FROM top_trades WHERE {w}", params)
    rows = ctx.rows(
        f"""SELECT run_date, rank, issuer_ticker AS ticker, issuer_name AS company, dominant_direction AS direction,
                   net_dollar_value, buy_value, sell_value, transaction_count, insider_count,
                   insider_name AS lead_insider, insider_title AS lead_title,
                   ROUND(pct_holdings * 100, 1) AS largest_trade_pct_of_position,
                   ROUND(planned_pct * 100, 1) AS pct_of_dollars_under_10b5_1_plan, signal_tags
            FROM top_trades WHERE {w} ORDER BY run_date DESC, rank LIMIT ?""",
        params + [a.n],
    )
    for r in rows:
        r["source"] = f"RANK {r['run_date']} #{r['rank']}"
    note = None if rows else (f"No ranking for {a.date}." if a.date else "No ranked trades match.")
    return _result(
        rows,
        total,
        latest_ranking_date=str(latest),
        note=note,
        ranking_rule="open-market buys/sells (P/S), ranked by absolute net dollars per company",
    )


def filings_for(ctx: ToolContext, a: FilingsForArgs) -> dict:
    if not ctx.has_table("filings") or not ctx.has_table("transactions"):
        return _unavailable("Filing data")
    start, end = _window(ctx, a.days)
    if end is None:
        return _result([], note="No filings loaded.")
    where = ["CAST(f.filing_date AS DATE) BETWEEN ? AND ?", f"t.code IN ({','.join('?' * len(a.codes))})"]
    params: list = [start, end, *a.codes]
    if a.ticker:
        where.append("f.issuer_ticker = ?")
        params.append(a.ticker)
    if a.owner:
        c, p = _owner_filter(ctx, a.owner)
        where.append(c)
        params += p
    w = " AND ".join(where) + _live(ctx)
    base = "FROM transactions t JOIN filings f ON f.accession_number = t.accession_number"
    tot = ctx.rows(
        f"""SELECT COUNT(*) AS n_transactions, COUNT(DISTINCT f.accession_number) AS n_filings,
                              SUM(CASE WHEN t.code = 'P' THEN {_value_expr()} END) AS buy_value,
                              SUM(CASE WHEN t.code = 'S' THEN {_value_expr()} END) AS sell_value {base} WHERE {w}""",
        params,
    )[0]
    rows = ctx.rows(
        f"SELECT {_TXN_COLS} {base} WHERE {w} ORDER BY dollar_value DESC NULLS LAST, filed DESC LIMIT ?",
        params + [a.limit],
    )
    return _result(
        _with_source(rows),
        tot["n_transactions"],
        totals=tot,
        window={"from": str(start), "to": str(end), "days": a.days},
        ordered_by="dollar_value, largest first",
    )


def insider_history(ctx: ToolContext, a: InsiderHistoryArgs) -> dict:
    if not ctx.has_table("filings") or not ctx.has_table("transactions"):
        return _unavailable("Filing data")
    c, p = _owner_filter(ctx, a.owner)
    w = f"{c}{_live(ctx)} AND t.code IN ('P','S')"
    base = "FROM transactions t JOIN filings f ON f.accession_number = t.accession_number"
    total = ctx.scalar(f"SELECT COUNT(*) {base} WHERE {w}", p)
    rows = ctx.rows(
        f"SELECT {_TXN_COLS} {base} WHERE {w} ORDER BY t.transaction_date DESC NULLS LAST LIMIT ?", p + [a.limit]
    )
    by_issuer = ctx.rows(
        f"""SELECT f.issuer_ticker AS ticker, COUNT(*) AS n_transactions,
                                    SUM(CASE WHEN t.code='P' THEN {_value_expr()} END) AS buy_value,
                                    SUM(CASE WHEN t.code='S' THEN {_value_expr()} END) AS sell_value
                             {base} WHERE {w} GROUP BY f.issuer_ticker ORDER BY n_transactions DESC LIMIT 10""",
        p,
    )
    names = [r[0] for r in ctx.db.query(f"SELECT DISTINCT f.insider_name {base} WHERE {w} LIMIT 6", p)]
    return _result(
        _with_source(rows),
        total,
        by_issuer=by_issuer,
        matched_insiders=names,
        note=None if rows else f"No open-market trades found for '{a.owner}'.",
    )


def cluster_events(ctx: ToolContext, a: ClusterEventsArgs) -> dict:
    if not ctx.has_table("filings") or not ctx.has_table("transactions"):
        return _unavailable("Filing data")
    start, end = _window(ctx, a.days)
    if end is None:
        return _result([], note="No filings loaded.")
    codes = {"BUY": ["P"], "SELL": ["S"]}.get(a.direction or "", ["P", "S"])
    base = (
        f"FROM transactions t JOIN filings f ON f.accession_number = t.accession_number "
        f"WHERE CAST(f.filing_date AS DATE) BETWEEN ? AND ? AND t.code IN ({','.join('?' * len(codes))}) "
        f"AND f.issuer_ticker IS NOT NULL{_live(ctx)}"
    )
    params = [start, end, *codes]
    grp = "GROUP BY f.issuer_ticker, t.code HAVING COUNT(DISTINCT f.insider_name) >= ?"
    total = ctx.scalar(f"SELECT COUNT(*) FROM (SELECT 1 {base} {grp})", params + [a.min_insiders])
    rows = ctx.rows(
        f"""SELECT f.issuer_ticker AS ticker, MAX(f.issuer_name) AS company,
                   CASE WHEN t.code = 'P' THEN 'BUY' ELSE 'SELL' END AS direction,
                   COUNT(DISTINCT f.insider_name) AS n_insiders, COUNT(*) AS n_transactions,
                   SUM({_value_expr()}) AS total_value, MIN(t.transaction_date) AS first_trade, MAX(t.transaction_date) AS last_trade,
                   list(DISTINCT f.insider_name) AS insiders
            {base} {grp} ORDER BY n_insiders DESC, total_value DESC LIMIT ?""",
        params + [a.min_insiders, a.limit],
    )
    for r in rows:
        r["insiders"] = (r["insiders"] or [])[:8]
        r["source"] = f"CLUSTER {r['ticker']} {r['direction']} {r['first_trade']}"
    return _result(
        rows,
        total,
        window={"from": str(start), "to": str(end), "days": a.days},
        note=None
        if rows
        else f"No company had {a.min_insiders}+ distinct insiders trading the same way in the window.",
    )


def insider_network(ctx: ToolContext, a: InsiderNetworkArgs) -> dict:
    """Other companies where this person is an insider, and what they did there (from the CIK-keyed insider_edges)."""
    if not ctx.has_table("insider_edges"):
        return _fallback_network(ctx, a.owner)
    if re.fullmatch(r"\d{4,10}", a.owner.strip()):
        cond, params = "e.owner_cik IN (?, ?)", [a.owner.strip().zfill(10), a.owner.strip()]
    else:
        cond, params = _like_tokens("e.owner_name", a.owner)
    total = ctx.scalar(f"SELECT COUNT(*) FROM insider_edges e WHERE {cond}", params)
    rows = ctx.rows(
        f"""SELECT e.owner_cik, e.owner_name, e.issuer_ticker AS ticker, e.issuer_name AS company,
                   e.is_officer, e.is_director, e.is_ten_pct, e.officer_title AS title,
                   CAST(e.first_accepted_at AS DATE) AS first_filing, CAST(e.last_accepted_at AS DATE) AS last_filing,
                   e.n_filings, e.buy_value, e.sell_value, e.n_buys, e.n_sells
            FROM insider_edges e WHERE {cond} ORDER BY e.last_accepted_at DESC NULLS LAST LIMIT 20""",
        params,
    )
    for r in rows:
        r["source"] = f"GRAPH {r['owner_cik']}"
    owners = sorted({(r["owner_cik"], r["owner_name"]) for r in rows})
    return _result(
        rows,
        total,
        matched_owners=[{"owner_cik": c, "name": n} for c, n in owners[:6]],
        note=None if rows else f"No insider matching '{a.owner}' in the network.",
    )


def _fallback_network(ctx: ToolContext, owner: str) -> dict:
    """No CIK graph yet: group the owner's filings by company using names (less reliable than CIKs)."""
    if not ctx.has_table("filings") or not ctx.has_table("transactions"):
        return _unavailable("The insider network")
    c, p = _owner_filter(ctx, owner)
    rows = ctx.rows(
        f"""SELECT f.insider_name AS owner_name, f.issuer_ticker AS ticker, MAX(f.issuer_name) AS company,
                   BOOL_OR(f.is_officer) AS is_officer, BOOL_OR(f.is_director) AS is_director, BOOL_OR(f.is_ten_pct) AS is_ten_pct,
                   CAST(MIN(f.filing_date) AS DATE) AS first_filing, CAST(MAX(f.filing_date) AS DATE) AS last_filing,
                   COUNT(DISTINCT f.accession_number) AS n_filings,
                   SUM(CASE WHEN t.code='P' THEN {_value_expr()} END) AS buy_value,
                   SUM(CASE WHEN t.code='S' THEN {_value_expr()} END) AS sell_value
            FROM filings f JOIN transactions t ON t.accession_number = f.accession_number
            WHERE {c}{_live(ctx)} AND t.code IN ('P','S') GROUP BY f.insider_name, f.issuer_ticker ORDER BY last_filing DESC LIMIT 20""",
        p,
    )
    for r in rows:
        r["source"] = f"GRAPH {r['owner_name']}"
    return _result(
        rows,
        len(rows),
        note=("CIK network not built yet; matched by name only." if rows else f"No insider matching '{owner}' found."),
        match="name",
    )


def co_insiders(ctx: ToolContext, a: CoInsidersArgs) -> dict:
    """Who else traded at this company in the window, and in how many other companies each of them is an insider."""
    if not ctx.has_table("filings") or not ctx.has_table("transactions"):
        return _unavailable("Filing data")
    start, end = _window(ctx, a.days)
    if end is None:
        return _result([], note="No filings loaded.")
    base_where = (
        f"f.issuer_ticker = ? AND CAST(f.filing_date AS DATE) BETWEEN ? AND ? AND t.code IN ('P','S'){_live(ctx)}"
    )
    params: list = [a.ticker, start, end]
    sums = (
        f"COUNT(*) AS n_transactions, SUM(CASE WHEN t.code='P' THEN {_value_expr()} END) AS buy_value, "
        f"SUM(CASE WHEN t.code='S' THEN {_value_expr()} END) AS sell_value, MAX(t.transaction_date) AS last_trade"
    )
    if ctx.has_table("reporting_owners"):
        other = (
            "(SELECT COUNT(*) FROM insider_edges e WHERE e.owner_cik = ro.owner_cik AND e.issuer_ticker <> ?) AS n_other_companies"
            if ctx.has_table("insider_edges")
            else "NULL AS n_other_companies"
        )
        oparams = [a.ticker] if ctx.has_table("insider_edges") else []
        q = (
            f"SELECT ro.owner_cik, ro.owner_name AS insider, BOOL_OR(ro.is_officer) AS is_officer, BOOL_OR(ro.is_director) AS is_director, "
            f"BOOL_OR(ro.is_ten_pct) AS is_ten_pct, {sums}, {other} "
            f"FROM transactions t JOIN filings f ON f.accession_number = t.accession_number "
            f"JOIN reporting_owners ro ON ro.accession_number = f.accession_number WHERE {base_where} "
            f"GROUP BY ro.owner_cik, ro.owner_name ORDER BY n_transactions DESC LIMIT ?"
        )
        rows = ctx.rows(q, oparams + params + [a.limit])
        for r in rows:
            r["source"] = f"GRAPH {r['owner_cik']}"
            if r.get("n_other_companies") is None:
                r.pop("n_other_companies", None)
        n_total = ctx.scalar(
            f"SELECT COUNT(DISTINCT ro.owner_cik) FROM transactions t JOIN filings f ON f.accession_number = t.accession_number "
            f"JOIN reporting_owners ro ON ro.accession_number = f.accession_number WHERE {base_where}",
            params,
        )
        note = None
    else:  # name-based fallback
        rows = ctx.rows(
            f"SELECT f.insider_name AS insider, f.insider_title AS title, BOOL_OR(f.is_officer) AS is_officer, "
            f"BOOL_OR(f.is_director) AS is_director, BOOL_OR(f.is_ten_pct) AS is_ten_pct, {sums} "
            f"FROM transactions t JOIN filings f ON f.accession_number = t.accession_number WHERE {base_where} "
            f"GROUP BY f.insider_name, f.insider_title ORDER BY n_transactions DESC LIMIT ?",
            params + [a.limit],
        )
        for r in rows:
            r["source"] = f"GRAPH {r['insider']}"
        n_total = ctx.scalar(
            f"SELECT COUNT(DISTINCT f.insider_name) FROM transactions t JOIN filings f ON f.accession_number = t.accession_number "
            f"WHERE {base_where}",
            params,
        )
        note = "CIK network not built yet; insiders matched by name only."
    return _result(
        rows,
        n_total,
        window={"from": str(start), "to": str(end), "days": a.days},
        note=note if rows else f"No insider trades for {a.ticker} in the window.",
    )


def sentiment(ctx: ToolContext, a: SentimentArgs) -> dict:
    if not ctx.has_table("sentiment_summary"):
        return _unavailable("Social sentiment")
    r = ctx.rows(
        """SELECT SUM(total_tweets) AS posts, SUM(bullish_count) AS bullish, SUM(bearish_count) AS bearish,
                  SUM(neutral_count) AS neutral, ROUND(SUM(sentiment_index * total_tweets) / NULLIF(SUM(total_tweets), 0), 2) AS sentiment_index,
                  MIN(summary_date) AS first_day, MAX(summary_date) AS last_day
           FROM sentiment_summary WHERE ticker = ?""",
        [a.ticker],
    )[0]
    if not r["posts"]:
        return _result([], note=f"No social-sentiment data is held for {a.ticker}.")
    r.update(
        ticker=a.ticker,
        source=f"SENT {a.ticker}",
        caveat="AI-classified social posts; noisy. sentiment_index runs from -1 (bearish) to +1 (bullish).",
    )
    return _result([r])


def data_status(ctx: ToolContext, a: DataStatusArgs) -> dict:
    row: dict = {}
    if ctx.has_table("pipeline_runs"):
        run = ctx.db.query("""SELECT run_id, status, completed_at, filings_ingested, trades_ranked, error_message
                              FROM pipeline_runs WHERE status <> 'running' ORDER BY started_at DESC LIMIT 1""")
        if run:
            rid, status, done, n_f, n_t, err = run[0]
            row.update(
                last_run_id=rid,
                last_run_status=status,
                last_run_completed_at=str(done)[:16] if done else None,
                last_run_filings_ingested=n_f,
                last_run_trades_ranked=n_t,
                last_run_error=err,
                source=f"RUN {rid}",
            )
    today = ctx.now().date()
    if ctx.has_table("filings"):
        row["filings_total"] = ctx.scalar("SELECT COUNT(*) FROM filings")
        newest = ctx.anchor()
        row["newest_filing_date"] = str(newest or "")
        if newest:
            row["newest_filing_age_days"] = (today - newest).days
    if ctx.has_table("top_trades"):
        latest = _to_date(ctx.scalar("SELECT MAX(run_date) FROM top_trades"))
        row["latest_ranking_date"] = str(latest or "")
        if latest:
            row["latest_ranking_age_days"] = (today - latest).days
    if ctx.rank_window_days:
        row["ranking_window_days"] = ctx.rank_window_days
    if "newest_filing_age_days" in row:
        row["freshness_note"] = (
            f"Data is refreshed about twice a month; the newest filing is {row['newest_filing_age_days']} day(s) old"
            + (f", and rankings cover the last {ctx.rank_window_days} days." if ctx.rank_window_days else ".")
        )
    return _result([row] if row else [], note=None if row else "The database is empty.")


# ── registry ─────────────────────────────────────────────────────────────────
def _simplify(schema: dict) -> dict:
    """Pydantic JSON-schema -> plain OpenAI-style parameters: no $defs/$ref, no titles, Optional[X] collapsed."""
    defs = schema.get("$defs", {})

    def walk(node):
        if isinstance(node, list):
            return [walk(x) for x in node]
        if not isinstance(node, dict):
            return node
        if "$ref" in node:
            return walk(defs[node["$ref"].split("/")[-1]])
        node = {k: v for k, v in node.items() if k not in ("title", "$defs")}
        if "anyOf" in node:
            opts = [o for o in node["anyOf"] if o.get("type") != "null"]
            if len(opts) == 1:
                merged = {k: v for k, v in node.items() if k != "anyOf"}
                merged.update(opts[0])
                if merged.get("default") is None:
                    merged.pop("default", None)
                return walk(merged)
        if node.get("default", 0) is None:
            node.pop("default")
        return {k: walk(v) for k, v in node.items()}

    out = walk(schema)
    out.setdefault("type", "object")
    out.setdefault("properties", {})
    return out


@dataclass
class ToolSpec:
    name: str
    description: str
    args: type[BaseModel]
    fn: Callable[[ToolContext, Any], dict]

    def openai(self) -> dict:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": _simplify(self.args.model_json_schema()),
            },
        }


@dataclass
class ToolOutcome:
    name: str
    args: dict
    ok: bool
    result: dict = field(default_factory=dict)
    error: str | None = None

    @property
    def n_rows(self) -> int:
        return len(self.result.get("rows") or []) if self.ok else 0

    @property
    def message(self) -> str:
        """The string handed back to the model as the tool message."""
        return json.dumps({"error": self.error} if not self.ok else self.result, default=str, separators=(",", ":"))


class ToolRegistry:
    def __init__(self, specs: list[ToolSpec]):
        self.specs = {s.name: s for s in specs}

    def names(self) -> list[str]:
        return list(self.specs)

    def openai_tools(self) -> list[dict]:
        return [s.openai() for s in self.specs.values()]

    def call(self, ctx: ToolContext, name: str, raw_args: str | dict | None) -> ToolOutcome:
        spec = self.specs.get(name)
        if spec is None:
            return ToolOutcome(
                name, {}, False, error=f"unknown tool '{name}'. Available tools: {', '.join(self.specs)}"
            )
        try:
            if isinstance(raw_args, str):
                raw_args = json.loads(raw_args) if raw_args.strip() else {}
            args = spec.args.model_validate(raw_args or {})
        except json.JSONDecodeError as e:
            return ToolOutcome(name, {}, False, error=f"arguments are not valid JSON: {e.msg}")
        except ValidationError as e:
            msg = "; ".join(f"{'.'.join(map(str, err['loc'])) or 'arguments'}: {err['msg']}" for err in e.errors())
            return ToolOutcome(
                name, raw_args if isinstance(raw_args, dict) else {}, False, error=f"invalid arguments: {msg[:400]}"
            )
        dumped = args.model_dump(exclude_none=True)
        try:
            result = _fit(spec.fn(ctx, args))
        except Exception as e:  # noqa: BLE001 - a broken query must not kill the turn
            return ToolOutcome(name, dumped, False, error=f"tool failed: {str(e)[:200]}")
        return ToolOutcome(name, dumped, True, result)


def _fit(result: dict, limit: int = MAX_RESULT_CHARS) -> dict:
    """Drop trailing rows until the JSON fits the size budget, and say so."""
    while len(json.dumps(result, default=str)) > limit and result.get("rows"):
        result["rows"] = result["rows"][:-1]
        result["row_count"] = len(result["rows"])
        result["truncated"] = True
    return result


def build_registry() -> ToolRegistry:
    return ToolRegistry(
        [
            ToolSpec(
                "top_trades",
                "Ranked insider trades (open-market buys/sells, ranked by absolute net dollars per company) for the latest ranking, "
                "a given date, or the last N days. Start here for 'biggest', 'top', 'today's' trades.",
                TopTradesArgs,
                top_trades,
            ),
            ToolSpec(
                "filings_for",
                "Individual Form 4 transactions for one company (ticker) and/or one insider (owner) over the last N days, largest first, "
                "with buy/sell totals.",
                FilingsForArgs,
                filings_for,
            ),
            ToolSpec(
                "insider_history",
                "All stored open-market trades by one insider across companies, newest first, with per-company totals.",
                InsiderHistoryArgs,
                insider_history,
            ),
            ToolSpec(
                "cluster_events",
                "Companies where several distinct insiders bought (or sold) in the same window. Cluster buying is the stronger signal.",
                ClusterEventsArgs,
                cluster_events,
            ),
            ToolSpec(
                "insider_network",
                "Multi-hop: the other companies where one person is an insider (director/officer/10% owner) and what they did there.",
                InsiderNetworkArgs,
                insider_network,
            ),
            ToolSpec(
                "co_insiders",
                "Who else traded at one company recently, and how many other companies each of them is an insider at.",
                CoInsidersArgs,
                co_insiders,
            ),
            ToolSpec(
                "sentiment",
                "Aggregate social-media sentiment counts for one ticker (no post text).",
                SentimentArgs,
                sentiment,
            ),
            ToolSpec(
                "data_status",
                "Freshness: how old the data is (newest filing and latest ranking age in days), last pipeline run, ranking window, filing count.",
                DataStatusArgs,
                data_status,
            ),
        ]
    )
