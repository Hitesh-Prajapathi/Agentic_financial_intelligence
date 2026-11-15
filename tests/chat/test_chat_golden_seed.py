"""The E2a golden-set seed is well-formed: gold SQL runs on the fixture, expected tools/args are real."""

import json
import re
from pathlib import Path

import pytest
from chat_testkit import MemDB

from insider_intel.chat.tools import build_registry

SEED = Path(__file__).resolve().parents[2] / "datasets" / "chat_golden" / "seed.jsonl"
ROWS = [json.loads(line) for line in SEED.read_text().splitlines() if line.strip()]
REG = build_registry()
CATS = {"lookup", "aggregation", "multi_hop", "temporal", "unanswerable"}


def test_seed_shape():
    assert 15 <= len(ROWS) <= 25 and len({r["id"] for r in ROWS}) == len(ROWS)
    assert {r["category"] for r in ROWS} == CATS
    for r in ROWS:
        assert {
            "id",
            "category",
            "question",
            "gold_sql",
            "expected_tool",
            "expected_tools",
            "expected_args",
            "abstain",
            "tolerance",
            "answer_type",
        } <= set(r)


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["id"])
def test_row_is_consistent(row):
    if row["expected_tool"] is None:
        assert row["abstain"] and row["gold_sql"] is None and row["expected_tools"] == []
        return
    assert row["expected_tool"] == row["expected_tools"][0]
    for name, args in row["expected_args"].items():
        assert name in REG.specs
        REG.specs[name].args.model_validate(args)  # expected args are valid for the tool
    assert set(row["expected_tools"]) <= set(REG.specs)


@pytest.mark.parametrize("row", [r for r in ROWS if r["gold_sql"]], ids=lambda r: r["id"])
def test_gold_sql_runs_and_is_reproducible(row):
    sql = row["gold_sql"]
    assert re.match(r"\s*SELECT\b", sql) and not re.search(
        r"now\(\)|current_date|current_timestamp|today\(\)", sql, re.I
    )
    rows = MemDB().query(sql)
    if row["abstain"]:
        assert not rows or all(v in (0, None) for v in rows[0])  # nothing in the data supports an answer
    else:
        assert rows and any(v is not None for v in rows[0]), row["id"]
