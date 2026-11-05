from tools.sec_api_tool import SecApiTool, mentions_trading_plan


def test_parse_real_filings(raw_filings):
    tool = SecApiTool()
    total = 0
    for raw in raw_filings:
        filing, txns = tool.parse_filing(raw)
        assert filing["accession_number"] == raw["accessionNo"]
        assert filing["issuer_ticker"] and filing["issuer_ticker"].isupper()
        assert filing["filing_date"] is not None and filing["filing_date"].tzinfo is None  # naive UTC
        for t in txns:
            total += 1
            assert t["code"] in ("P", "S")                       # only open-market signal rows survive
            assert t["shares"] > 0 and t["price_per_share"] > 0
            assert t["dollar_value"] == round(t["shares"] * t["price_per_share"], 2)
    assert total > 0


def test_unusable_rows_are_skipped():
    raw = {"accessionNo": "X-1", "filedAt": "2026-11-05T10:00:00-05:00", "issuer": {"name": "A", "tradingSymbol": "aa"},
           "reportingOwner": {"name": "Z", "relationship": {"isDirector": True}},
           "nonDerivativeTable": {"transactions": [
               {"coding": {"code": "A"}, "amounts": {"shares": 10, "pricePerShare": 5}},       # grant → ignored
               {"coding": {"code": "P"}, "amounts": {"shares": 10}},                            # no price → ignored
               {"coding": {"code": "P"}, "amounts": {"shares": 10, "pricePerShare": 5, "acquiredDisposedCode": "A"}}]}}
    filing, txns = SecApiTool().parse_filing(raw)
    assert filing["issuer_ticker"] == "AA" and filing["insider_title"] == "Director"
    assert len(txns) == 1 and txns[0]["row_idx"] == 2


def test_filed_at_converted_to_utc():
    raw = {"accessionNo": "X-2", "filedAt": "2026-11-05T11:15:01-05:00", "issuer": {"tradingSymbol": "T"}, "reportingOwner": {}}
    filing, _ = SecApiTool().parse_filing(raw)
    assert filing["filing_date"].hour == 16


def test_trading_plan_detection():
    assert mentions_trading_plan("Sold pursuant to a Rule 10b5-1 trading plan adopted in May.")
    assert not mentions_trading_plan("This sale was not made pursuant to a Rule 10b5-1 plan.")
    assert not mentions_trading_plan("")
