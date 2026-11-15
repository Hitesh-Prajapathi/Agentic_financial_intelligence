"""Insider network on a small hand-built database."""

from datetime import date, datetime

import pytest

from ingest_testkit import make_filing
from insider_intel.pipeline.ingest import filing_to_db_rows, mark_superseded
from insider_intel.store.graph import co_insiders, insider_network, normalize_name, rebuild_insider_edges

T0 = datetime(2026, 11, 2, 14)


def add(db, f):
    r = filing_to_db_rows(f)
    db.insert_filings([r["filing"]])
    db.insert_reporting_owners(r["owners"])
    db.insert_transactions(r["txns"])


@pytest.fixture
def g(db):
    d = date(2026, 11, 2)
    # George Kurtz: CEO+director at CRWD (buys), director at ZZZ (sells); name stored "Kurtz George" at ZZZ
    add(
        db,
        make_filing(
            "0000000001-26-000001",
            issuer_cik="100",
            ticker="CRWD",
            issuer_name="Crowd Inc",
            accepted=T0,
            owners=(("500", "George Kurtz"),),
            txns=[("P", 1000, 10.0, d), ("S", 200, 11.0, d)],
        ),
    )
    add(
        db,
        make_filing(
            "0000000001-26-000002",
            issuer_cik="200",
            ticker="ZZZ",
            issuer_name="Zed Corp",
            accepted=datetime(2026, 11, 5, 14),
            owners=(("500", "Kurtz George"),),
            txns=[("S", 300, 20.0, d)],
        ),
    )
    # a second insider at CRWD, joint filing with a fund that also holds ZZZ
    add(
        db,
        make_filing(
            "0000000001-26-000003",
            issuer_cik="100",
            ticker="CRWD",
            issuer_name="Crowd Inc",
            accepted=datetime(2026, 11, 9, 14),
            owners=(("600", "Ann Lee"), ("700", "Big Fund LP")),
            txns=[("P", 500, 12.0, date(2026, 11, 9))],
        ),
    )
    # filing with only a grant (M/A/F): the edge exists with zero P/S
    add(
        db,
        make_filing(
            "0000000001-26-000004",
            issuer_cik="200",
            ticker="ZZZ",
            issuer_name="Zed Corp",
            accepted=datetime(2026, 11, 6, 14),
            owners=(("600", "Ann Lee"),),
            txns=[("A", 100, 0.0, d)],
        ),
    )
    rebuild_insider_edges(db)
    return db


def test_edges_aggregate_roles_and_values(g):
    rows = {
        (r[0], r[1]): r
        for r in g.query("""SELECT owner_cik, issuer_cik, n_filings, buy_value, sell_value, n_buys, n_sells,
                                              is_officer, officer_title, ticker FROM (
                                              SELECT *, issuer_ticker AS ticker FROM insider_edges)""")
    }
    k100 = rows[("500", "100")]
    assert (
        (k100[2], k100[3], k100[4], k100[5], k100[6]) == (1, 10_000.0, 2_200.0, 1, 1) and k100[7] and k100[8] == "CEO"
    )
    assert rows[("500", "200")][4] == 6_000.0
    assert (
        rows[("700", "100")][3] == 6_000.0 and rows[("600", "100")][3] == 6_000.0
    )  # joint filing counts for each owner
    assert rows[("600", "200")][3:7] == (0.0, 0.0, 0, 0)  # grant-only filing still an edge
    assert len(rows) == 5


def test_name_normalisation():
    assert normalize_name("Kurtz George") == normalize_name("George Kurtz") == normalize_name("George R. Kurtz Jr.")
    assert normalize_name("George Kurtz") != normalize_name("George Kurtis")


def test_insider_network_by_cik_and_both_name_orders(g):
    by_cik = insider_network(g, "500")
    by_last_first = insider_network(g, "Kurtz George")
    by_first_last = insider_network(g, "george kurtz")
    for df in (by_cik, by_last_first, by_first_last):
        assert sorted(df["issuer_ticker"]) == ["CRWD", "ZZZ"] and set(df["owner_cik"]) == {"500"}
    assert list(insider_network(g, "500")["issuer_ticker"]) == ["ZZZ", "CRWD"]  # newest activity first
    assert list(insider_network(g, "George Kurtz", exclude_issuer="crwd")["issuer_ticker"]) == [
        "ZZZ"
    ]  # ticker is case-insensitive
    assert list(insider_network(g, "George Kurtz", exclude_issuer="CRWD")["issuer_ticker"]) == ["ZZZ"]
    assert insider_network(g, "Nobody Here").empty and insider_network(g, "").empty


def test_co_insiders_by_ticker_cik_and_since(g):
    all_time = co_insiders(g, "CRWD")
    assert set(all_time["owner_cik"]) == {"500", "600", "700"}
    assert list(co_insiders(g, "100")["owner_cik"]) == list(all_time["owner_cik"])
    recent = co_insiders(g, "CRWD", since=date(2026, 11, 8))
    assert set(recent["owner_cik"]) == {"600", "700"}
    assert co_insiders(g, "CRWD", exclude_owner="Kurtz George")["owner_cik"].tolist() == ["600", "700"]
    assert co_insiders(g, "Crowd")["issuer_ticker"].unique().tolist() == ["CRWD"]  # name substring
    assert co_insiders(g, "NOPE").empty


def test_rebuild_is_deterministic_and_removes_stale_edges(g):
    before = g.query("SELECT * FROM insider_edges ORDER BY owner_cik, issuer_cik")
    rebuild_insider_edges(g)
    assert g.query("SELECT * FROM insider_edges ORDER BY owner_cik, issuer_cik") == before
    g.execute("INSERT INTO insider_edges (owner_cik, issuer_cik) VALUES ('999', '999')")
    rebuild_insider_edges(g)
    assert g.query("SELECT COUNT(*) FROM insider_edges WHERE owner_cik = '999'") == [(0,)]


def test_amendment_and_duplicate_filing_are_not_double_counted(g):
    d = date(2026, 11, 2)
    base = g.query(
        "SELECT buy_value, sell_value, n_filings FROM insider_edges WHERE owner_cik='500' AND issuer_cik='100'"
    )[0]
    # 4/A restates the CRWD sells (200 -> 150 shares); the original sell row must stop counting
    add(
        g,
        make_filing(
            "0000000001-26-000010",
            form="4/A",
            issuer_cik="100",
            ticker="CRWD",
            accepted=datetime(2026, 11, 3, 14),
            owners=(("500", "George Kurtz"),),
            txns=[("S", 150, 11.0, d)],
        ),
    )
    # the same trade filed again under another accession by the same owner set (joint duplicate)
    add(
        g,
        make_filing(
            "0000000001-26-000011",
            issuer_cik="100",
            ticker="CRWD",
            accepted=datetime(2026, 11, 3, 15),
            owners=(("500", "George Kurtz"),),
            txns=[("P", 1000, 10.0, d)],
        ),
    )
    mark_superseded(g)
    rebuild_insider_edges(g)
    after = g.query(
        "SELECT buy_value, sell_value, n_filings FROM insider_edges WHERE owner_cik='500' AND issuer_cik='100'"
    )[0]
    assert (after[0], after[1]) == (10_000.0, 1_650.0)  # buy not doubled, sell = amended 150 x 11
    assert after[2] == base[2] + 2  # filings count includes the amendment and the duplicate
