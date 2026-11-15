"""Streamlit UI.  Run with:  make app   (or `streamlit run app/streamlit_app.py` after `pip install -e .`)."""

import logging
import uuid
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from insider_intel.chat.metrics import chat_metrics
from insider_intel.config import settings
from insider_intel.demo import seed_demo
from insider_intel.health import check_all
from insider_intel.pipeline.run import STAGE_LABELS, STAGES
from insider_intel.system import build_system
from insider_intel.telemetry import current_session_id, llm_usage
from insider_intel.ui import charts

st.set_page_config(page_title="Insider Trading Intelligence", page_icon="📈", layout="wide")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
for noisy in ("httpx", "httpcore", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

st.markdown(
    """
<style>
.block-container {padding-top: 2.2rem; max-width: 1250px;}
.pill {display:inline-block; padding:2px 10px; border-radius:999px; font-size:0.78rem; margin-right:6px;
       background:rgba(127,127,127,.15); border:1px solid rgba(127,127,127,.25);}
.pill.good {background:rgba(22,163,74,.14); border-color:rgba(22,163,74,.4);}
.pill.warn {background:rgba(234,179,8,.16); border-color:rgba(234,179,8,.5);}
.pill.bad  {background:rgba(220,38,38,.14); border-color:rgba(220,38,38,.4);}
div[data-testid="stMetric"] {background:rgba(127,127,127,.07); border-radius:10px; padding:10px 14px;}
</style>
""",
    unsafe_allow_html=True,
)

STATUS_ICON = {"pending": "⚪", "running": "⏳", "ok": "✅", "partial": "⚠️", "failed": "❌", "skipped": "⏭️"}
CONF_PILL = {
    "high": ("good", "🟢 High confidence"),
    "moderate": ("warn", "🟡 Moderate confidence"),
    "low": ("bad", "🔴 Low confidence"),
}
SUGGESTIONS = [
    "What are the biggest insider trades in the latest data?",
    "Is there any cluster buying?",
    "Which sales are under a 10b5-1 plan?",
    "Chart the top trades",
    "Who is the lead insider at the top ticker?",
]


# ═══ system / state ═════════════════════════════════════════════════════════
@st.cache_resource(show_spinner="Starting agents…")
def get_system(mode: str):
    return build_system(mode, read_only=settings.app_read_only)


def ago(ts) -> str:
    if ts is None:
        return "never"
    secs = (datetime.now(timezone.utc).replace(tzinfo=None) - ts).total_seconds()
    if secs < 90:
        return "just now"
    if secs < 5400:
        return f"{int(secs // 60)} min ago"
    if secs < 172800:
        return f"{secs / 3600:.0f} h ago"
    return f"{secs / 86400:.0f} d ago"


def usd(x) -> str:
    a = abs(float(x))
    s = f"{a / 1e9:.2f}B" if a >= 1e9 else f"{a / 1e6:.2f}M" if a >= 1e6 else f"{a / 1e3:.0f}K"
    return ("-" if x < 0 else "") + "$" + s


demo_mode = st.sidebar.toggle(
    "Demo mode (fictional data)",
    value=st.session_state.get("demo", False),
    help="Uses a separate demo database with made-up companies. Your live data is never touched.",
)
st.session_state["demo"] = demo_mode
mode = "demo" if demo_mode else "live"
system = get_system(mode)
pipeline, chat, db = system.pipeline, system.chat, system.db
read_only = pipeline is None  # hosted snapshot: no refresh, no writes
current_session_id.set(st.session_state.setdefault("session_id", uuid.uuid4().hex[:12]))
WINDOW_DAYS = settings.rank_window_days
msg_key = f"messages_{mode}"
st.session_state.setdefault(msg_key, [])

if demo_mode and not read_only and db.query("SELECT COUNT(*) FROM top_trades")[0][0] == 0:
    with st.spinner("Preparing demo data…"):
        seed_demo(db)

NO_PROGRESS = {"running": False, "run_id": None, "trigger": None, "status": None, "error": None, "stages": {}}


def progress_snapshot() -> dict:
    return NO_PROGRESS if read_only else pipeline.progress.snapshot()


snap = progress_snapshot()


def finish_if_done():
    """Called from the status fragment: when a background run ends, refresh the whole page once."""
    if st.session_state.get("was_running") and not progress_snapshot()["running"]:
        st.session_state["was_running"] = False
        st.rerun()
    st.session_state["was_running"] = progress_snapshot()["running"]


# ═══ sidebar ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("📈 Insider Trading Intelligence")
    st.caption("SEC Form 4 insider trades, with a governed pipeline.")
    if demo_mode:
        st.info("**Demo mode** — fictional companies and people. Switch off to use live data.", icon="🧪")

    missing = (
        []
        if (settings.has_sec_user_agent or read_only)
        else ["SEC_USER_AGENT (name + contact email, required by SEC EDGAR)"]
    )
    if not (settings.has_key("groq_api_key") or settings.has_key("openrouter_api_key")):
        missing.append("GROQ_API_KEY or OPENROUTER_API_KEY")
    if missing and not demo_mode:
        st.warning("Missing in `.env`: " + ", ".join(missing))

    as_of = db.data_as_of()
    st.metric("Data last updated", ago(as_of))

    if read_only:
        st.caption(
            "Read-only snapshot: data is refreshed by a scheduled job (about twice a month), not from this page."
        )
    else:
        refresh_disabled = demo_mode or snap["running"]
        if st.button(
            "🔄 Refresh data",
            type="primary",
            width="stretch",
            disabled=refresh_disabled,
            help="Fetch new Form 4s from SEC EDGAR, rank them and run the governance gate. Runs in the background. "
            "Data is normally refreshed about twice a month.",
        ):
            pipeline.start_async("manual")
            st.session_state["was_running"] = True
            st.rerun()

    run_every = 2 if snap["running"] else None

    @st.fragment(run_every=run_every)
    def status_box():
        s = progress_snapshot()
        if s["running"]:
            done = sum(1 for v in s["stages"].values() if v["status"] not in ("pending", "running"))
            st.progress(done / len(STAGES), text="Pipeline running…")
            for name in STAGES:
                st.caption(f"{STATUS_ICON[s['stages'][name]['status']]} {STAGE_LABELS[name]}")
        elif s["status"]:
            icon = {"success": "✅", "partial": "⚠️", "quarantined": "🛑", "failed": "❌"}.get(s["status"], "")
            st.caption(f"{icon} Last run this session: **{s['status']}**" + (f" — {s['error']}" if s["error"] else ""))
            if s["status"] == "partial":
                st.caption(
                    "Partial run: some filings were not processed before the time limit; the next run continues from where it stopped."
                )
        finish_if_done()

    status_box()

    st.divider()
    st.caption("Informational only — not investment advice. Form 4s can be filed up to 2 business days after a trade.")

# ═══ header ═════════════════════════════════════════════════════════════════
st.title("SEC Insider Trading Intelligence")
n_filings = db.query(
    "SELECT COUNT(*) FROM filings WHERE filing_date >= ?",
    [datetime.now(timezone.utc).replace(tzinfo=None) - pd.Timedelta(days=WINDOW_DAYS)],
)[0][0]
primary = system.llm.chain()[0] if system.llm.chain() else None
st.markdown(
    f'<span class="pill {"warn" if demo_mode else "good"}">{"demo data" if demo_mode else "live data"}</span>'
    f'<span class="pill">{n_filings} filings · last {WINDOW_DAYS} days</span>'
    f'<span class="pill">{primary[1] if primary else "no LLM configured"}</span>',
    unsafe_allow_html=True,
)

tab_chat, tab_dash, tab_pipe, tab_health = st.tabs(["💬 Chat", "📊 Dashboard", "⚙️ Pipeline", "🩺 Health"])


# ═══ chat ═══════════════════════════════════════════════════════════════════
def render_tools(tools: list[dict]):
    """Collapsible "How I got this": every lookup the agent ran, with its arguments and how many rows came back."""
    if not tools:
        return
    with st.expander(f"How I got this ({len(tools)} lookup{'s' if len(tools) != 1 else ''})"):
        for t in tools:
            args = ", ".join(f"{k}={v!r}" for k, v in (t.get("args") or {}).items())
            status = f"{t.get('rows', 0)} row(s)" if t.get("ok", True) else f"rejected: {t.get('error')}"
            st.markdown(f"- `{t['name']}({args})` → {status}")


def render_meta(m: dict):
    """Confidence/citation badges, lookups, evidence expander and thumbs feedback under an assistant message."""
    meta = m.get("meta")
    if m["role"] != "assistant" or not meta:
        return
    cls, label = CONF_PILL.get(meta.get("confidence", "low"), CONF_PILL["low"])
    cites = meta.get("citations") or {}
    g = meta.get("grounding") or {}
    bits = [f'<span class="pill {cls}">{label}</span>']
    if meta.get("mode") == "degraded":
        bits.append(
            '<span class="pill warn" title="No tool-calling model available: answered from a pre-built context.">limited mode</span>'
        )
    if cites.get("valid"):
        bits.append(f'<span class="pill good">✔ {len(cites["valid"])} source(s) verified</span>')
    elif meta.get("intent") == "chat" and not cites.get("refused"):
        bits.append('<span class="pill warn">no verifiable citations</span>')
    if cites.get("invalid"):
        bits.append(f'<span class="pill bad">{len(cites["invalid"])} unverified tag(s)</span>')
    if g.get("unsupported"):
        bits.append(f'<span class="pill bad">{len(g["unsupported"])} unverified number(s)</span>')
    bits.append(f'<span class="pill">{meta.get("latency_ms", 0) / 1000:.1f}s</span>')
    if meta.get("model"):
        bits.append(f'<span class="pill">{meta["model"].split(":", 1)[-1]}</span>')
    st.markdown("".join(bits), unsafe_allow_html=True)
    if g.get("unsupported"):
        st.warning(
            "Could not verify these numbers against the data: "
            + ", ".join(f"`{x}`" for x in g["unsupported"][:8])
            + ". Check them in **How I got this** / the evidence before relying on them.",
            icon="⚠️",
        )
    if g.get("empty_but_asserted"):
        st.warning("The lookups returned nothing, yet the answer states facts. Treat it as unsupported.", icon="⚠️")
    render_tools(meta.get("tools") or [])
    if meta.get("evidence"):
        with st.expander("Evidence used"):
            if meta.get("reason"):
                st.caption(f"Confidence reason: {meta['reason']}")
            st.code(meta["evidence"], language="text")
    if m.get("query_id"):
        fb = st.feedback("thumbs", key=f"fb_{mode}_{m['query_id']}")
        if fb is not None:
            db.set_feedback(m["query_id"], 1 if fb == 1 else -1)


def render_message(m: dict, idx: int):
    with st.chat_message(m["role"]):
        st.markdown(m["content"])
        if m.get("chart") is not None:
            st.plotly_chart(m["chart"], width="stretch", key=f"chart_{mode}_{idx}")
        render_meta(m)


def answer(prompt: str):
    """Stream one turn at the bottom of the conversation (no page rerun needed)."""
    msgs = st.session_state[msg_key]
    history = [{"role": m["role"], "content": m["content"]} for m in msgs]
    msgs.append({"role": "user", "content": prompt})
    hold = {"chart": None, "meta": {}, "query_id": None, "tools": []}
    live: list[str] = []

    def gen():
        for ev in chat.stream_query(prompt, history):
            t = ev["type"]
            if t == "tool":
                hold["tools"].append(ev)
                live.append(
                    f"🔎 `{ev['name']}` → {ev['rows']} row(s)"
                    if ev.get("ok", True)
                    else f"🔎 `{ev['name']}` rejected, retrying"
                )
                lookups.caption("  \n".join(live))
            elif t == "delta":
                yield ev["text"]
            elif t == "chart":
                hold["chart"] = ev["figure"]
            elif t == "meta":
                hold["meta"].update(ev)
            elif t == "done":
                hold["meta"].update(ev)
                hold["query_id"] = ev.get("query_id")
            elif t == "error":
                yield f"⚠️ {ev['message']}"

    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        lookups = st.empty()  # live "looking things up" lines while the agent calls tools
        with st.spinner("Thinking…"):
            text = st.write_stream(gen())
        lookups.empty()  # replaced by the collapsible "How I got this" under the answer
        hold["meta"]["tools"] = hold["tools"]
        reply = {
            "role": "assistant",
            "content": text,
            "chart": hold["chart"],
            "meta": hold["meta"],
            "query_id": hold["query_id"],
        }
        if reply["chart"] is not None:
            st.plotly_chart(reply["chart"], width="stretch", key=f"chart_{mode}_{len(msgs)}")
        render_meta(reply)
    msgs.append(reply)


clicked = None
with tab_chat:
    msgs = st.session_state[msg_key]
    if not msgs:
        st.markdown(
            "Ask about recent insider activity (last 14 days), a specific ticker, planned (10b5-1) sales, or request a chart. "
            "Answers cite their sources and say so when the data can't support one."
        )
    cols = st.columns(len(SUGGESTIONS) + 1)
    for c, s_ in zip(cols, SUGGESTIONS):
        if c.button(s_, key=f"sug_{mode}_{s_}", width="stretch"):
            clicked = s_
    if cols[-1].button("🗑 Clear", disabled=not msgs, width="stretch"):
        st.session_state[msg_key] = []
        st.rerun()
    for i, m in enumerate(msgs):
        render_message(m, i)
    typed = st.chat_input("Ask about insider trades, a ticker, or say 'chart the top trades'…")
    prompt_now = clicked or typed
    if prompt_now:
        answer(prompt_now)

# ═══ dashboard ══════════════════════════════════════════════════════════════
with tab_dash:
    trades = db.latest_top_trades(settings.top_n_trades)
    if trades.empty:
        st.info(
            ("No ranked trades yet." if read_only else "No ranked trades yet. Click **Refresh data** in the sidebar")
            + (" or turn on demo mode." if not demo_mode else ".")
        )
    else:
        sent_tot = db.query(
            "SELECT COALESCE(SUM(total_tweets),0), SUM(sentiment_index*total_tweets)/NULLIF(SUM(total_tweets),0) FROM sentiment_summary"
        )[0]
        has_sentiment = int(sent_tot[0]) > 0  # only the demo data carries sentiment until it is re-sourced (E3)
        c = st.columns(4 if has_sentiment else 2)
        c[0].metric(f"Filings (last {WINDOW_DAYS} days)", f"{n_filings:,}")
        c[1].metric("Net flow, top 5", usd(trades["net_dollar_value"].sum()))
        if has_sentiment:
            c[2].metric("Posts analysed", f"{int(sent_tot[0]):,}")
            c[3].metric("Avg sentiment", "n/a" if sent_tot[1] is None else f"{sent_tot[1]:+.2f}")

        st.subheader("Top insider trades")
        st.caption(f"Ranked over the last {WINDOW_DAYS} days of filings. Data is refreshed about twice a month.")
        table = pd.DataFrame(
            {
                "Rank": trades["rank"],
                "Ticker": trades["issuer_ticker"],
                "Company": trades["issuer_name"],
                "Direction": trades["dominant_direction"].map({"BUY": "🟢 BUY", "SELL": "🔴 SELL"}),
                "Net $": trades["net_dollar_value"],
                "Bought $": trades["buy_value"],
                "Sold $": trades["sell_value"],
                "Insiders": trades["insider_count"],
                "Lead insider": trades["insider_name"] + " · " + trades["insider_title"].fillna(""),
                "% of position": trades["pct_holdings"],
                "Planned (10b5-1)": trades["planned_pct"],
                "Signals": trades["signal_tags"].str.replace(",", "  ·  "),
            }
        )
        st.dataframe(
            table,
            hide_index=True,
            width="stretch",
            column_config={
                "Net $": st.column_config.NumberColumn(format="$%,.0f"),
                "Bought $": st.column_config.NumberColumn(format="$%,.0f"),
                "Sold $": st.column_config.NumberColumn(format="$%,.0f"),
                "% of position": st.column_config.ProgressColumn(format="%.0f%%", min_value=0, max_value=1),
                "Planned (10b5-1)": st.column_config.ProgressColumn(format="%.0f%%", min_value=0, max_value=1),
            },
        )
        with st.expander("How to read this"):
            st.markdown(
                "- Only **open-market buys (P) and sells (S)** count; grants, option exercises and tax withholding are excluded.\n"
                "- **CLUSTER_BUY / CLUSTER_SELL**: several insiders trading the same way. Cluster *buying* is the stronger signal.\n"
                "- **Planned (10b5-1)**: share of dollars traded under a pre-scheduled plan — less informative than discretionary trades.\n"
                "- **% of position**: the largest trade relative to that insider's holdings."
            )

        left, right = st.columns(2)
        left.plotly_chart(charts.top_trades_summary(trades), width="stretch")
        right.plotly_chart(charts.buy_sell_breakdown(trades), width="stretch")

        gcols = st.columns(len(trades)) if has_sentiment else []
        if has_sentiment:
            st.subheader("Social sentiment on the top tickers")
        for col, tk in zip(gcols, trades["issuer_ticker"]):
            s = db.query(
                """SELECT SUM(total_tweets), SUM(bullish_count), SUM(bearish_count), SUM(neutral_count),
                                   SUM(sentiment_index*total_tweets)/NULLIF(SUM(total_tweets),0) FROM sentiment_summary WHERE ticker=?""",
                [tk],
            )[0]
            with col:
                st.markdown(f"**{tk}**")
                if s[0]:
                    st.plotly_chart(charts.sentiment_gauge(float(s[4])), width="stretch", key=f"g_{mode}_{tk}")
                    st.caption(f"{int(s[0])} posts · 🟢{int(s[1])} 🔴{int(s[2])} ⚪{int(s[3])}")
                else:
                    st.caption("no posts analysed")

        st.subheader("Latest filings behind the numbers")
        recent = db.query_df("""SELECT f.filing_date AS "Filed (UTC)", f.issuer_ticker AS "Ticker", f.insider_name AS "Insider",
                                       f.insider_title AS "Role", t.code AS "Type", t.shares AS "Shares", t.price_per_share AS "Price",
                                       t.shares*t.price_per_share AS "Value", f.is_10b5_1 AS "10b5-1", f.accession_number AS "Accession"
                                FROM transactions t JOIN filings f USING (accession_number)
                                WHERE t.code IN ('P','S') AND COALESCE(t.superseded, FALSE) = FALSE
                                ORDER BY f.filing_date DESC, "Value" DESC LIMIT 25""")
        st.dataframe(
            recent,
            hide_index=True,
            width="stretch",
            column_config={
                "Shares": st.column_config.NumberColumn(format="%,.0f"),
                "Price": st.column_config.NumberColumn(format="$%.2f"),
                "Value": st.column_config.NumberColumn(format="$%,.0f"),
            },
        )

# ═══ pipeline ═══════════════════════════════════════════════════════════════
with tab_pipe:

    @st.fragment(run_every=run_every)
    def pipeline_view():
        s = progress_snapshot()
        if s["running"] or s["status"]:
            st.subheader("Current / last run")
            for name in STAGES:
                st_ = s["stages"][name]
                sec = f" · {st_['seconds']:.1f}s" if st_["seconds"] is not None else ""
                with st.expander(
                    f"{STATUS_ICON[st_['status']]} {STAGE_LABELS[name]} — {st_['status']}{sec}",
                    expanded=st_["status"] in ("failed", "partial"),
                ):
                    st.json(st_["detail"] or {"info": "no details yet"})
            if s["error"]:
                st.error(s["error"])
        else:
            st.info(
                "No run in this session yet."
                + ("" if demo_mode or read_only else " Click **Refresh data** to start one.")
            )
        finish_if_done()

    pipeline_view()
    st.subheader("Run history")
    hist = db.query_df("""SELECT started_at AS "Started (UTC)", status AS "Status", trigger AS "Trigger", filings_ingested AS "Filings",
                                 trades_ranked AS "Ranked", error_message AS "Note"
                          FROM pipeline_runs ORDER BY started_at DESC LIMIT 20""")
    st.dataframe(hist, hide_index=True, width="stretch") if not hist.empty else st.caption("No runs recorded yet.")
    with st.expander("Governance: what protects the data?"):
        st.markdown(
            "Before a ranking is published, five independent checks run: **ingest reconciliation** (everything parsed is in the DB), "
            "**notional reconciliation** (SQL total = ranked + explicitly dropped), **ticker bounds**, **ranking integrity** "
            "(order, arithmetic, direction) and a **checksum**. If any fails the run is **quarantined**: nothing is published, "
            "and chat never reads it."
        )

# ═══ health ═════════════════════════════════════════════════════════════════
with tab_health:
    st.subheader("Connections")
    if st.button("Run live checks"):
        with st.spinner("Pinging SEC and the LLM providers…"):
            st.session_state["health"] = check_all(system)
    for r in st.session_state.get("health", []):
        st.markdown(f"{'✅' if r['ok'] else '❌'} **{r['service']}** — {r['detail']}")
    if "health" not in st.session_state:
        st.caption("Each check makes one tiny real API call (uses a sliver of free quota).")

    st.subheader("LLM usage (all recorded calls)")
    u = llm_usage(db)
    if u:
        c = st.columns(5)
        c[0].metric("Calls", f"{u['calls']:,}")
        c[1].metric("Latency p50 / p95", f"{u['p50_ms'] / 1000:.1f}s / {u['p95_ms'] / 1000:.1f}s")
        c[2].metric(
            "Failure / fallback rate",
            f"{u['failure_rate']:.0%}",
            help="Calls that errored and fell through to the next model in the chain.",
        )
        c[3].metric("Tokens", f"{u['prompt_tokens'] + u['completion_tokens']:,}")
        c[4].metric(
            "Notional cost",
            f"${u['est_cost_usd']:.4f}",
            help="ESTIMATE at approximate paid list prices. Free-tier models are used, so nothing is actually billed.",
        )
        left, right = st.columns(2)
        left.caption("Calls by model")
        left.dataframe(u["by_model"], hide_index=True, width="stretch")
        right.caption("Calls by purpose")
        right.dataframe(u["by_purpose"], hide_index=True, width="stretch")
    else:
        st.caption("No LLM calls recorded yet.")

    st.subheader("LLM usage this session")
    st_ = system.llm.stats
    if st_.calls:
        st.dataframe(
            pd.DataFrame([{"Model": k, "OK": v["ok"], "Failed": v["fail"]} for k, v in st_.by_model.items()]),
            hide_index=True,
            width="stretch",
        )
    else:
        st.caption("No LLM calls yet.")
    st.caption(
        f"Fallback chain: {' → '.join(f'{p}:{m}' for p, m in settings.chain)}  ·  paid models blocked: {not settings.allow_paid_models}"
    )

    st.subheader("Chat quality")
    m = chat_metrics(db)
    c = st.columns(4)
    c[0].metric("Questions answered", m["questions"])
    c[1].metric("Avg latency", f"{m['avg_latency_ms'] / 1000:.1f}s")
    c[2].metric("p90 latency", f"{m['p90_latency_ms'] / 1000:.1f}s")
    c[3].metric("👍 / 👎", f"{m['thumbs_up']} / {m['thumbs_down']}")
