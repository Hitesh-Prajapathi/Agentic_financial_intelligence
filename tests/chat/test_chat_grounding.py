import pytest

from insider_intel.chat.grounding import check_grounding, citation_tags, extract_numbers, is_abstention


def vals(text):
    return [(n.raw, n.value, n.kind) for n in extract_numbers(text)]


def one(text):
    out = extract_numbers(text)
    assert len(out) == 1, [n.raw for n in out]
    return out[0]


# ── extraction ───────────────────────────────────────────────────────────────
@pytest.mark.parametrize("text,value,kind", [
    ("$17.4M", 17.4e6, "usd"), ("$17.4 million", 17.4e6, "usd"), ("$1.2B", 1.2e9, "usd"), ("1.2 billion", 1.2e9, "plain"),
    ("$150K", 150e3, "usd"), ("$17,375,692", 17375692, "usd"), ("43%", 43, "pct"), ("43.5 percent", 43.5, "pct"),
    ("180,000 shares", 180000, "plain"), ("$52.40", 52.4, "usd"), ("-$8.7M", 8.7e6, "usd"), ("$-8,762,500", 8762500, "usd"),
    ("(1,200)", 1200, "plain"), ("3 insiders", 3, "plain"), ("+0.67", 0.67, "plain"),
])
def test_extracts_numbers(text, value, kind):
    n = one(text)
    assert n.value == pytest.approx(value) and n.kind == kind


@pytest.mark.parametrize("text", [
    "in 2026", "on 2026-11-05", "filed 11/05/2026", "Nov 5, 2026", "November 5th", "5 Nov 2026", "at 14:30",
    "accession 0001193125-26-449032", "DEMO-26-00001", "ranked #1", "the #2 trade", "rank 3", "No. 4",
    "the 1st and 2nd", "a Rule 10b5-1 plan", "a Form 4", "Form 4/A", "Forms 3, 4 and 5", "the 10-K", "Q3", "FY2026", "S&P 500",
    "[RANK 2026-11-05 #1]", "[SEC Form 4 0001193125-26-449032]", "[GRAPH 0000000003]", "[CLUSTER GLBX BUY 2026-11-11]",
    "1. first item",
])
def test_ignores_non_claims(text):
    assert extract_numbers(text) == []


def test_decimal_and_comma_edge_cases():
    assert [n.value for n in extract_numbers("buys of 1,000, 2,500 and 3.5")] == [1000, 2500, 3.5]
    assert [n.value for n in extract_numbers("a v2 model and gpt5")] == []   # digits glued to words are not numbers
    assert [n.value for n in extract_numbers("**$1.5M** bought")] == [1.5e6]  # markdown bold


def test_year_like_number_with_comma_or_unit_is_kept():
    assert [n.value for n in extract_numbers("2,026 shares")] == [2026]
    assert [n.value for n in extract_numbers("$2020")] == [2020]


# ── matching ─────────────────────────────────────────────────────────────────
def outcome(**result):
    result.setdefault("rows", [{"x": 1}])
    return {"name": "t", "args": {}, "ok": True, "result": result}


def g(answer, *outs, question="", history=None):
    return check_grounding(answer, list(outs), question, history)


def test_display_rounding_matches():
    o = outcome(rows=[{"net": -17375692.0, "price": 52.4, "source": "RANK 2026-11-05 #1"}])
    for ok in ["$17.4M", "$17.38M", "$17,375,692", "$17.375692M", "17.4 million", "$17M", "$52.40", "$52.4", "$52"]:
        assert g(f"net {ok}", o)["unsupported"] == [], ok
    for bad in ["$18.4M", "$52.90", "$17.9M", "$9.4M"]:
        assert g(f"net {bad}", o)["unsupported"] == [bad], bad


def test_relative_tolerance_half_percent():
    o = outcome(rows=[{"v": 1000000.0}])
    assert g("$1,004,000", o)["unsupported"] == []
    assert g("$1,010,000", o)["unsupported"] == ["$1,010,000"]


def test_percent_matches_fractions_and_percent_points():
    o = outcome(rows=[{"pct": 0.4312, "other": 33.3}])
    assert g("43% of his position", o)["unsupported"] == []
    assert g("33.3% planned", o)["unsupported"] == []
    assert g("55% of his position", o)["unsupported"] == ["55%"]


def test_small_plain_integers_must_match_exactly():
    o = outcome(rows=[{"n": 3.4}])
    assert g("3 insiders", o)["unsupported"] == ["3"]  # 3.4 is not 3: no display-rounding for bare counts
    assert g("3.4 insiders", o)["unsupported"] == []


def test_row_counts_args_and_question_numbers_are_in_the_pool():
    o = {"name": "top_trades", "args": {"n": 5, "days": 14}, "ok": True, "result": {"rows": [{"a": 1}, {"a": 2}]}}
    assert g("Here are 5 trades over 14 days, 2 shown.", o)["unsupported"] == []
    assert g("About $2 million moved", o, question="did more than $2 million move?")["unsupported"] == []


def test_history_numbers_carry_over():
    o = outcome(rows=[{"a": 1}])
    hist = [{"role": "assistant", "content": "ACME sold $9.4M"}]
    assert g("As said, $9.4M", o, history=hist)["unsupported"] == []
    assert g("As said, $9.4M", o)["unsupported"] == ["$9.4M"]


def test_derived_numbers_are_flagged():
    o = outcome(rows=[{"buy": 1000000.0}, {"buy": 500000.0}])
    assert g("Total $1.5M", o)["unsupported"] == ["$1.5M"]


def test_sign_is_ignored():
    o = outcome(rows=[{"net": -1950000.0}])
    assert g("net selling of $1.95M", o)["unsupported"] == []


def test_numeric_string_leaves_and_nested_values():
    o = outcome(rows=[{"d": {"v": "1,234.5"}, "l": [7, 8]}])
    assert g("1,234.5 and 8", o)["unsupported"] == []


def test_source_ids_are_not_numbers_in_the_pool():
    o = outcome(rows=[{"source": "RANK 2026-11-05 #77"}])
    assert g("77 trades", o)["unsupported"] == ["77"]


# ── citations ────────────────────────────────────────────────────────────────
def test_citations_must_exist_in_this_turns_tool_results():
    o = outcome(rows=[{"source": "RANK 2026-11-05 #1"}, {"source": "SEC Form 4 0001-26-000001"}])
    r = g("A [RANK 2026-11-05 #1] and B [SEC Form 4 0001-26-000001] and C [RANK 1999-01-01 #9]", o)
    assert r["citations_valid"] == ["RANK 2026-11-05 #1", "SEC Form 4 0001-26-000001"]
    assert r["citations_invalid"] == ["RANK 1999-01-01 #9"]


def test_citation_normalisation_and_new_prefixes():
    assert citation_tags("x [**RANK 2026‑11‑05 #1**] 【SENT BROS】 [GRAPH 123] [CLUSTER A BUY 2026-01-01] [RUN r1] [other]") == {
        "RANK 2026-11-05 #1", "SENT BROS", "GRAPH 123", "CLUSTER A BUY 2026-01-01", "RUN r1"}
    o = outcome(rows=[{"source": "GRAPH 0000000003"}])
    assert g("Priya [GRAPH 0000000003]", o)["citations_valid"] == ["GRAPH 0000000003"]


def test_citations_from_a_previous_turn_are_invalid_this_turn():
    assert g("see [RANK 2026-11-05 #1]", outcome(rows=[{"source": "SENT X"}]))["citations_invalid"] == ["RANK 2026-11-05 #1"]


# ── abstention ───────────────────────────────────────────────────────────────
@pytest.mark.parametrize("text", ["I don't have data on that.", "I do not have any data for ZZZZ.", "No matching filings were found.",
                                  "That is not available in this database.", "I couldn't find anything."])
def test_abstention_phrases(text):
    assert is_abstention(text)


def test_empty_results_with_asserted_facts_is_flagged():
    empty = {"name": "t", "args": {}, "ok": True, "result": {"rows": []}}
    assert g("ZZZZ insiders bought $5M of shares.", empty)["empty_but_asserted"] is True
    assert g("I don't have data on that.", empty)["empty_but_asserted"] is False
    assert g("I don't have data on that.", empty)["abstained"] is True
    assert g("Hello! Ask me about insider trades.", empty)["empty_but_asserted"] is False       # no facts stated
    assert g("It was [RANK 2026-11-05 #1].", empty)["empty_but_asserted"] is True               # a citation is a claim
    assert g("ZZZZ saw $5M", {"name": "t", "args": {}, "ok": False, "result": {}, "error": "x"})["empty_but_asserted"] is True
    assert g("5M", outcome())["empty_but_asserted"] is False


def test_no_tool_calls_and_numbers_is_flagged():
    assert check_grounding("They sold $5M", [])["empty_but_asserted"] is True


def test_evidence_text_mode_for_degraded_answers():
    ev = "#1 ACME: net $1,234,567 [RANK 2026-11-05 #1]"
    r = check_grounding("ACME net $1.23M [RANK 2026-11-05 #1] and $9M", [], evidence_text=ev, extra_sources={"RANK 2026-11-05 #1"})
    assert r["unsupported"] == ["$9M"] and r["citations_valid"] == ["RANK 2026-11-05 #1"] and not r["empty_but_asserted"]


def test_result_shape():
    r = g("x", outcome())
    assert {"supported", "unsupported", "citations_valid", "citations_invalid", "abstained"} <= set(r)
