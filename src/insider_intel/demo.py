"""Demo data: entirely FICTIONAL companies and people, written to the separate demo database.

It never touches the live database. Run `python -m insider_intel seed-demo` or use the UI's demo mode.
"""

from datetime import datetime, timedelta, timezone

from insider_intel.config import settings
from insider_intel.pipeline.ingest import mark_superseded
from insider_intel.pipeline.rank import validate_and_rank
from insider_intel.store.db import DBTool, get_db
from insider_intel.store.graph import rebuild_insider_edges

# (ticker, company, [(insider, title, officer, director, code, shares, price, plan, post_shares)])
SCENARIOS = [
    (
        "ACME",
        "Acme Robotics Inc.",
        [
            ("Dana Whitfield", "Chief Executive Officer", True, True, "S", 180_000, 52.40, True, 900_000),
            ("Marcus Lee", "Chief Financial Officer", True, False, "S", 40_000, 52.10, True, 160_000),
        ],
    ),
    (
        "GLBX",
        "Globex Energy Corp.",
        [
            ("Priya Natarajan", "Director", False, True, "P", 60_000, 18.25, False, 140_000),
            ("Tom Okafor", "Chief Operating Officer", True, False, "P", 25_000, 18.30, False, 75_000),
            ("Helen Brandt", "Director", False, True, "P", 15_000, 18.10, False, 45_000),
        ],
    ),
    (
        "NOVA",
        "Nova Biotherapeutics",
        [
            ("Samuel Ortiz", "10% Owner", False, False, "S", 250_000, 7.80, False, 750_000),
            # the same fictional person is an insider at two demo companies (ACME CEO, NOVA director): graph demo
            ("Dana Whitfield", "Director", False, True, "P", 4_000, 7.70, False, 24_000),
        ],
    ),
    (
        "QRTZ",
        "Quartz Semiconductor",
        [("Elena Voss", "Chief Executive Officer", True, True, "P", 12_000, 140.0, False, 40_000)],
    ),
    (
        "LUMI",
        "Lumiere Retail Group",
        [
            ("Kenji Arai", "Chief Financial Officer", True, False, "S", 90_000, 31.0, False, 400_000),
            ("Ruth Calloway", "Director", False, True, "S", 30_000, 30.8, True, 120_000),
        ],
    ),
]

TWEETS = {
    "ACME": [
        ("Founder selling again? Not a good sign for $ACME.", "bearish"),
        ("$ACME robotics demo was impressive, long term bullish", "bullish"),
        ("Acme Robotics reports earnings Thursday", "neutral"),
        ("$ACME overvalued at these levels imo", "bearish"),
    ],
    "GLBX": [
        ("Three insiders buying $GLBX at once - that's conviction", "bullish"),
        ("$GLBX breakout incoming, adding more", "bullish"),
        ("Globex Energy announces dividend date", "neutral"),
        ("Insider cluster buy at $GLBX, bullish", "bullish"),
    ],
    "NOVA": [
        ("$NOVA 10% holder dumping a quarter of his stake. Ouch.", "bearish"),
        ("Trial data for Nova Biotherapeutics next month", "neutral"),
        ("$NOVA bearish until the overhang clears", "bearish"),
    ],
    "QRTZ": [
        ("CEO buying $QRTZ with her own money. Bullish.", "bullish"),
        ("$QRTZ supply chain looking stronger", "bullish"),
        ("Quartz Semiconductor added to an index", "neutral"),
    ],
    "LUMI": [
        ("$LUMI CFO selling 90k shares, not great", "bearish"),
        ("Lumiere Retail same-store sales flat", "neutral"),
        ("$LUMI looks weak into the holidays", "bearish"),
    ],
}


def seed_demo(db: DBTool | None = None) -> dict:
    db = db or get_db(settings.demo_db_path)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for t in (
        "tweet_sentiments",
        "tweets",
        "sentiment_summary",
        "top_trades",
        "insider_edges",
        "reporting_owners",
        "footnotes",
        "transactions",
        "filings",
        "pipeline_runs",
    ):
        db.execute(f"DELETE FROM {t}")  # demo DB only

    # fictional CIKs (9xxxxxx): one per person and per company, stable across reseeds
    owner_cik = {n: str(9_100_000 + i) for i, n in enumerate(sorted({r[0] for _, _, rows in SCENARIOS for r in rows}))}
    issuer_cik = {tk: str(9_000_000 + i) for i, (tk, _, _) in enumerate(SCENARIOS)}

    filings, txns, owners, acc = [], [], [], 0
    for ticker, company, rows in SCENARIOS:
        for insider, title, officer, director, code, shares, price, plan, post in rows:
            acc += 1
            an = f"DEMO-{now:%y}-{acc:05d}"
            accepted = now - timedelta(hours=acc * 2)
            ten_pct = title == "10% Owner"
            filings.append(
                dict(
                    accession_number=an,
                    filing_date=accepted,
                    accepted_at=accepted,
                    issuer_name=company,
                    issuer_ticker=ticker,
                    issuer_cik=issuer_cik[ticker],
                    insider_name=insider,
                    insider_title=title,
                    is_officer=officer,
                    is_director=director,
                    is_ten_pct=ten_pct,
                    is_10b5_1=plan,
                    owner_ciks=owner_cik[insider],
                )
            )
            owners.append(
                dict(
                    accession_number=an,
                    owner_cik=owner_cik[insider],
                    owner_name=insider,
                    is_director=director,
                    is_officer=officer,
                    is_ten_pct=ten_pct,
                    officer_title=title if officer else None,
                )
            )
            txns.append(
                dict(
                    accession_number=an,
                    row_idx=0,
                    transaction_date=(now - timedelta(days=1)).date(),
                    security_title="Common Stock",
                    code=code,
                    acquired_disposed="A" if code == "P" else "D",
                    shares=float(shares),
                    price_per_share=price,
                    dollar_value=round(shares * price, 2),
                    ownership_nature="D",
                    post_transaction_shares=float(post),
                )
            )
    db.insert_filings(filings)
    db.insert_reporting_owners(owners)
    db.insert_transactions(txns)
    mark_superseded(db)
    rebuild_insider_edges(db)

    df = db.query_df("""SELECT t.*, f.issuer_cik, f.owner_ciks, f.issuer_ticker, f.issuer_name, f.insider_name, f.insider_title, f.is_officer, f.is_director, f.is_10b5_1
                        FROM transactions t JOIN filings f USING (accession_number)""")
    ranked, checksum, _ = validate_and_rank(df, settings.top_n_trades, 0)
    trades = ranked.drop(columns=["abs_net"]).to_dict("records")
    for t in trades:
        t["validation_checksum"] = checksum
    run_id = "demo-run"
    db.start_run(run_id, now - timedelta(minutes=2), "demo")
    db.update_run(
        run_id,
        status="success",
        completed_at=now,
        filings_ingested=len(filings),
        trades_ranked=len(trades),
        stages="{}",
    )
    db.replace_top_trades(run_id, now.strftime("%Y-%m-%d"), trades)

    tweets, sents, n = [], [], 0
    for ticker, items in TWEETS.items():
        for text, label in items:
            n += 1
            tweets.append(
                dict(
                    tweet_id=f"demo-{n}",
                    ticker=ticker,
                    text=text,
                    author_username=f"user{n}",
                    author_followers=100 * n,
                    created_at=now - timedelta(hours=n),
                    retweet_count=n,
                    like_count=3 * n,
                    reply_count=n // 2,
                )
            )
            sents.append(
                dict(tweet_id=f"demo-{n}", ticker=ticker, sentiment_label=label, confidence_score=0.85, model="demo")
            )
    db.insert_tweets(tweets)
    db.insert_sentiments(sents)
    db.refresh_sentiment_summary()
    return {
        "filings": len(filings),
        "trades": len(trades),
        "tweets": len(tweets),
        "insider_edges": db.query("SELECT COUNT(*) FROM insider_edges")[0][0],
    }
