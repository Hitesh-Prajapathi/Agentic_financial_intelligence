import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
os.chdir(
    ROOT
)  # .env / relative data paths resolve from the project root; `insider_intel` comes from pythonpath=["src"]

EDGAR_FIXTURES = ROOT / "tests" / "edgar" / "fixtures"  # reconstructed real + synthetic Form 4 XML (read-only)


@pytest.fixture
def db():
    from insider_intel.store.db import DBTool

    d = DBTool(":memory:")
    yield d
    d.close()


from ingest_testkit import FakeSource, day_ts, recent_weekdays  # noqa: E402


@pytest.fixture
def fixture_source():
    """FakeSource over the 24 real/synthetic XML fixtures spread over the last 3 weekdays (accepted_at set per day)."""
    from insider_intel.edgar.form4 import parse_form4

    days = recent_weekdays(3)
    items = []
    for i, p in enumerate(sorted(EDGAR_FIXTURES.glob("*.xml"))):
        d = days[i % 3]
        items.append((d, parse_form4(p.read_bytes(), p.stem, day_ts(d, 14 + i % 6))))
    src = FakeSource(items)
    src.days = days
    return src


@pytest.fixture
def cfg():
    from insider_intel.config import Settings

    return Settings(
        _env_file=None,
        rank_window_days=14,
        min_trade_notional=0,
        top_n_trades=5,
        edgar_max_runtime_minutes=0,
        sec_user_agent="unit-test-agent contact@test.invalid",
    )
