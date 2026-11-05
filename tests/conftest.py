import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)  # prompts/schema are loaded relative to the project root


@pytest.fixture
def db():
    from tools.db_tool import DBTool
    d = DBTool(":memory:")
    yield d
    d.close()


@pytest.fixture(scope="session")
def raw_filings():
    return json.loads((ROOT / "tests" / "fixtures" / "sec_form4_sample.json").read_text())
