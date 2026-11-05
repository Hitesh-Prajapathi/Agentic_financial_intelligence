"""Streamlit UI.  Run with:  streamlit run app.py   (from the project root, conda env `ai`)."""
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from agents.supervisor import STAGE_LABELS, STAGES  # noqa: E402
from config.settings import settings  # noqa: E402
from core.health import check_all  # noqa: E402
from core.system import build_system  # noqa: E402
from scripts.seed_test_data import seed_demo  # noqa: E402

st.set_page_config(page_title="Insider Trading Intelligence", page_icon="📈", layout="wide")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
for noisy in ("httpx", "httpcore", "lightrag", "nano-vectordb", "apify_client", "opentelemetry", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

st.markdown("""
<style>
.block-container {padding-top: 2.2rem; max-width: 1250px;}
.pill {display:inline-block; padding:2px 10px; border-radius:999px; font-size:0.78rem; margin-right:6px;
       background:rgba(127,127,127,.15); border:1px solid rgba(127,127,127,.25);}
.pill.good {background:rgba(22,163,74,.14); border-color:rgba(22,163,74,.4);}
.pill.warn {background:rgba(234,179,8,.16); border-color:rgba(234,179,8,.5);}
.pill.bad  {background:rgba(220,38,38,.14); border-color:rgba(220,38,38,.4);}
div[data-testid="stMetric"] {background:rgba(127,127,127,.07); border-radius:10px; padding:10px 14px;}
</style>
""", unsafe_allow_html=True)

STATUS_ICON = {"pending": "⚪", "running": "⏳", "ok": "✅", "partial": "⚠️", "failed": "❌", "skipped": "⏭️"}
CONF_PILL = {"high": ("good", "🟢 High confidence"), "moderate": ("warn", "🟡 Moderate confidence"), "low": ("bad", "🔴 Low confidence")}
SUGGESTIONS = ["What are today's biggest insider trades?", "Is there any cluster buying?",
               "Which sales are under a 10b5-1 plan?", "Chart the top trades", "How is sentiment on the top ticker?"]


# ═══ system / state ═════════════════════════════════════════════════════════
@st.cache_resource(show_spinner="Starting agents…")
def get_system(mode: str):
    return build_system(mode)


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


demo_mode = st.sidebar.toggle("Demo mode (fictional data)", value=st.session_state.get("demo", False),
                              help="Uses a separate demo database with made-up companies. Your live data is never touched.")
st.session_state["demo"] = demo_mode
mode = "demo" if demo_mode else "live"
system = get_system(mode)
sup, db = system.supervisor, system.db
msg_key = f"messages_{mode}"
st.session_state.setdefault(msg_key, [])

if demo_mode and db.query("SELECT COUNT(*) FROM top_trades")[0][0] == 0:
    with st.spinner("Preparing demo data…"):
        seed_demo(db)

snap = sup.progress.snapshot()


def finish_if_done():
    """Called from the status fragment: when a background run ends, refresh the whole page once."""
    if st.session_state.get("was_running") and not sup.progress.snapshot()["running"]:
        st.session_state["was_running"] = False
        st.rerun()
    st.session_state["was_running"] = sup.progress.snapshot()["running"]


# ═══ sidebar ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("📈 Insider Trading Intelligence")
    st.caption("SEC Form 4 filings × social sentiment, with a governed pipeline.")
    if demo_mode:
        st.info("**Demo mode** — fictional companies and people. Switch off to use live data.", icon="🧪")

    missing = [n for n, k in (("SEC_API_KEY", "sec_api_key"), ("APIFY_API_TOKEN", "apify_api_token")) if not settings.has_key(k)]
    if not (settings.has_key("groq_api_key") or settings.has_key("openrouter_api_key")):
        missing.append("GROQ_API_KEY or OPENROUTER_API_KEY")
    if missing and not demo_mode:
        st.warning("Missing in `.env`: " + ", ".join(missing))

    as_of = sup.retrieval.data_as_of()
    st.metric("Data last updated", ago(as_of))

    refresh_disabled = demo_mode or snap["running"]
    if st.button("🔄 Refresh data", type="primary", width="stretch", disabled=refresh_disabled,
                 help="Fetch new Form 4s, score them, scrape tweets, classify sentiment. Runs in the background."):
        sup.start_pipeline_async("manual")
        st.session_state["was_running"] = True
        st.rerun()

    run_every = 2 if snap["running"] else None

    @st.fragment(run_every=run_every)
    def status_box():
        s = sup.progress.snapshot()
        if s["running"]:
            done = sum(1 for v in s["stages"].values() if v["status"] not in ("pending", "running"))
            st.progress(done / len(STAGES), text="Pipeline running…")
            for name in STAGES:
                st.caption(f"{STATUS_ICON[s['stages'][name]['status']]} {STAGE_LABELS[name]}")
        elif s["status"]:
            icon = {"success": "✅", "partial": "⚠️", "quarantined": "🛑", "failed": "❌"}.get(s["status"], "")
            st.caption(f"{icon} Last run this session: **{s['status']}**" + (f" — {s['error']}" if s["error"] else ""))
        finish_if_done()

    status_box()

    st.divider()
    rag_on = st.toggle("LightRAG knowledge graph", value=system.rag.enabled,
                       help="Adds graph-retrieved narrative context to answers and indexes each run into LightRAG. "
                            "Uses a few extra LLM calls per run; SQL facts always take priority.")
    system.rag.enabled = rag_on
    if rag_on and system.rag.error:
        st.caption(f"⚠️ LightRAG unavailable: {system.rag.error}")
    st.caption("Informational only — not investment advice. Form 4s can be filed up to 2 business days after a trade.")

# ═══ header ═════════════════════════════════════════════════════════════════
st.title("SEC Insider Trading Intelligence")
n_filings = db.query("SELECT COUNT(*) FROM filings WHERE filing_date >= ?",
                     [datetime.now(timezone.utc).replace(tzinfo=None) - pd.Timedelta(hours=settings.filing_lookback_hours)])[0][0]
primary = system.llm.chain()[0] if system.llm.chain() else None
st.markdown(
    f'<span class="pill {"warn" if demo_mode else "good"}">{"demo data" if demo_mode else "live data"}</span>'
    f'<span class="pill">{n_filings} filings · {settings.filing_lookback_hours}h</span>'
    f'<span class="pill {"good" if rag_on else ""}">LightRAG {"on" if rag_on else "off"}</span>'
    f'<span class="pill">{primary[1] if primary else "no LLM configured"}</span>', unsafe_allow_html=True)

tab_chat, tab_dash, tab_pipe, tab_health = st.tabs(["💬 Chat", "📊 Dashboard", "⚙️ Pipeline", "🩺 Health"])


# ═══ chat ═══════════════════════════════════════════════════════════════════
def render_meta(m: dict):
    """Confidence/citation badges, evidence expander and thumbs feedback under an assistant message."""
    meta = m.get("meta")
    if m["role"] != "assistant" or not meta:
        return
    cls, label = CONF_PILL.get(meta.get("confidence", "low"), CONF_PILL["low"])
    cites = meta.get("citations") or {}
    bits = [f'<span class="pill {cls}">{label}</span>']
    if cites.get("valid"):
        bits.append(f'<span class="pill good">✔ {len(cites["valid"])} source(s) verified</span>')
    elif meta.get("intent") == "chat" and not cites.get("refused"):
        bits.append('<span class="pill warn">no verifiable citations</span>')
    if cites.get("invalid"):
        bits.append(f'<span class="pill bad">{len(cites["invalid"])} unverified tag(s)</span>')
    bits.append(f'<span class="pill">{meta.get("latency_ms", 0) / 1000:.1f}s</span>')
    if meta.get("model"):
        bits.append(f'<span class="pill">{meta["model"].split(":", 1)[-1]}</span>')
    if meta.get("used_lightrag"):
        bits.append('<span class="pill">+ knowledge graph</span>')
    st.markdown("".join(bits), unsafe_allow_html=True)
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
    hold = {"chart": None, "meta": {}, "query_id": None}

    def gen():
        for ev in sup.stream_query(prompt, history):
            t = ev["type"]
            if t == "delta":
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
        with st.spinner("Thinking…"):
            text = st.write_stream(gen())
        reply = {"role": "assistant", "content": text, "chart": hold["chart"], "meta": hold["meta"], "query_id": hold["query_id"]}
        if reply["chart"] is not None:
            st.plotly_chart(reply["chart"], width="stretch", key=f"chart_{mode}_{len(msgs)}")
        render_meta(reply)
    msgs.append(reply)


clicked = None
with tab_chat:
    msgs = st.session_state[msg_key]
    if not msgs:
        st.markdown("Ask about today's insider activity, a specific ticker, planned (10b5-1) sales, or request a chart. "
                    "Answers cite their sources and say so when the data can't support one.")
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
        st.info("No ranked trades yet. Click **Refresh data** in the sidebar" + (" or turn on demo mode." if not demo_mode else "."))
    else:
        sent_tot = db.query("SELECT COALESCE(SUM(total_tweets),0), SUM(sentiment_index*total_tweets)/NULLIF(SUM(total_tweets),0) FROM sentiment_summary")[0]
        c = st.columns(4)
        c[0].metric("Filings (24h)", f"{n_filings:,}")
        c[1].metric("Net flow, top 5", usd(trades["net_dollar_value"].sum()))
        c[2].metric("Tweets analysed", f"{int(sent_tot[0]):,}")
        c[3].metric("Avg sentiment", "n/a" if sent_tot[1] is None else f"{sent_tot[1]:+.2f}")

        st.subheader("Top insider trades")
        table = pd.DataFrame({
            "Rank": trades["rank"], "Ticker": trades["issuer_ticker"], "Company": trades["issuer_name"],
            "Direction": trades["dominant_direction"].map({"BUY": "🟢 BUY", "SELL": "🔴 SELL"}),
            "Net $": trades["net_dollar_value"], "Bought $": trades["buy_value"], "Sold $": trades["sell_value"],
            "Insiders": trades["insider_count"], "Lead insider": trades["insider_name"] + " · " + trades["insider_title"].fillna(""),
            "% of position": trades["pct_holdings"], "Planned (10b5-1)": trades["planned_pct"], "Signals": trades["signal_tags"].str.replace(",", "  ·  "),
        })
        st.dataframe(table, hide_index=True, width="stretch", column_config={
            "Net $": st.column_config.NumberColumn(format="$%,.0f"), "Bought $": st.column_config.NumberColumn(format="$%,.0f"),
            "Sold $": st.column_config.NumberColumn(format="$%,.0f"),
            "% of position": st.column_config.ProgressColumn(format="%.0f%%", min_value=0, max_value=1),
            "Planned (10b5-1)": st.column_config.ProgressColumn(format="%.0f%%", min_value=0, max_value=1)})
        with st.expander("How to read this"):
            st.markdown(
                "- Only **open-market buys (P) and sells (S)** count; grants, option exercises and tax withholding are excluded.\n"
                "- **CLUSTER_BUY / CLUSTER_SELL**: several insiders trading the same way. Cluster *buying* is the stronger signal.\n"
                "- **Planned (10b5-1)**: share of dollars traded under a pre-scheduled plan — less informative than discretionary trades.\n"
                "- **% of position**: the largest trade relative to that insider's holdings.")

        left, right = st.columns(2)
        left.plotly_chart(system.charts.top_trades_summary(trades), width="stretch")
        right.plotly_chart(system.charts.buy_sell_breakdown(trades), width="stretch")

        st.subheader("Social sentiment on the top tickers")
        gcols = st.columns(len(trades))
        for col, tk in zip(gcols, trades["issuer_ticker"]):
            s = db.query("""SELECT SUM(total_tweets), SUM(bullish_count), SUM(bearish_count), SUM(neutral_count),
                                   SUM(sentiment_index*total_tweets)/NULLIF(SUM(total_tweets),0) FROM sentiment_summary WHERE ticker=?""", [tk])[0]
            with col:
                st.markdown(f"**{tk}**")
                if s[0]:
                    st.plotly_chart(system.charts.sentiment_gauge(float(s[4])), width="stretch", key=f"g_{mode}_{tk}")
                    st.caption(f"{int(s[0])} tweets · 🟢{int(s[1])} 🔴{int(s[2])} ⚪{int(s[3])}")
                else:
                    st.caption("no tweets analysed")

        st.subheader("Latest filings behind the numbers")
        recent = db.query_df("""SELECT f.filing_date AS "Filed (UTC)", f.issuer_ticker AS "Ticker", f.insider_name AS "Insider",
                                       f.insider_title AS "Role", t.code AS "Type", t.shares AS "Shares", t.price_per_share AS "Price",
                                       t.shares*t.price_per_share AS "Value", f.is_10b5_1 AS "10b5-1", f.accession_number AS "Accession"
                                FROM transactions t JOIN filings f USING (accession_number)
                                ORDER BY f.filing_date DESC, "Value" DESC LIMIT 25""")
        st.dataframe(recent, hide_index=True, width="stretch", column_config={
            "Shares": st.column_config.NumberColumn(format="%,.0f"), "Price": st.column_config.NumberColumn(format="$%.2f"),
            "Value": st.column_config.NumberColumn(format="$%,.0f")})

# ═══ pipeline ═══════════════════════════════════════════════════════════════
with tab_pipe:
    @st.fragment(run_every=run_every)
    def pipeline_view():
        s = sup.progress.snapshot()
        if s["running"] or s["status"]:
            st.subheader("Current / last run")
            for name in STAGES:
                st_ = s["stages"][name]
                sec = f" · {st_['seconds']:.1f}s" if st_["seconds"] is not None else ""
                with st.expander(f"{STATUS_ICON[st_['status']]} {STAGE_LABELS[name]} — {st_['status']}{sec}", expanded=st_["status"] in ("failed", "partial")):
                    st.json(st_["detail"] or {"info": "no details yet"})
            if s["error"]:
                st.error(s["error"])
        else:
            st.info("No run in this session yet." + ("" if demo_mode else " Click **Refresh data** to start one."))
        finish_if_done()

    pipeline_view()
    st.subheader("Run history")
    hist = db.query_df("""SELECT started_at AS "Started (UTC)", status AS "Status", trigger AS "Trigger", filings_ingested AS "Filings",
                                 trades_ranked AS "Ranked", tweets_scraped AS "Tweets", tweets_classified AS "Classified",
                                 documents_indexed AS "Indexed", error_message AS "Note"
                          FROM pipeline_runs ORDER BY started_at DESC LIMIT 20""")
    st.dataframe(hist, hide_index=True, width="stretch") if not hist.empty else st.caption("No runs recorded yet.")
    with st.expander("Governance: what protects the data?"):
        st.markdown(
            "Before a ranking is published, five independent checks run: **ingest reconciliation** (everything parsed is in the DB), "
            "**notional reconciliation** (SQL total = ranked + explicitly dropped), **ticker bounds**, **ranking integrity** "
            "(order, arithmetic, direction) and a **checksum**. If any fails the run is **quarantined**: nothing is published, "
            "no API money is spent on tweets/LLM, and the learning loop never sees it.")

# ═══ health ═════════════════════════════════════════════════════════════════
with tab_health:
    st.subheader("Connections")
    if st.button("Run live checks"):
        with st.spinner("Pinging SEC, Apify and the LLM providers…"):
            st.session_state["health"] = check_all(system)
    for r in st.session_state.get("health", []):
        st.markdown(f"{'✅' if r['ok'] else '❌'} **{r['service']}** — {r['detail']}")
    if "health" not in st.session_state:
        st.caption("Each check makes one tiny real API call (uses a sliver of free quota).")

    st.subheader("LLM usage this session")
    st_ = system.llm.stats
    if st_.calls:
        st.dataframe(pd.DataFrame([{"Model": k, "OK": v["ok"], "Failed": v["fail"]} for k, v in st_.by_model.items()]),
                     hide_index=True, width="stretch")
    else:
        st.caption("No LLM calls yet.")
    st.caption(f"Fallback chain: {' → '.join(f'{p}:{m}' for p, m in settings.chain)}  ·  paid models blocked: {not settings.allow_paid_models}")

    st.subheader("Learning loop")
    m = system.learning.metrics()
    c = st.columns(4)
    c[0].metric("Tweets labelled", m["tweets_classified"])
    c[1].metric("Avg label confidence", f"{m['avg_confidence']:.2f}")
    c[2].metric("Second-opinion agreement", "n/a" if m["review_agreement"] is None else f"{m['review_agreement']:.0%}")
    c[3].metric("Few-shot examples learned", m["examples"])
    c = st.columns(4)
    c[0].metric("Questions answered", m["questions"])
    c[1].metric("p90 latency", f"{m['p90_latency_ms'] / 1000:.1f}s")
    c[2].metric("👍 / 👎", f"{m['thumbs_up']} / {m['thumbs_down']}")
    c[3].metric("Approved runs", m["approved_runs"])

    st.subheader("LightRAG")
    st.write({"enabled": system.rag.enabled, "ready": system.rag.ready, "error": system.rag.error,
              "workspace": str(system.rag.workspace), "embeddings": system.llm.embedding_id})
