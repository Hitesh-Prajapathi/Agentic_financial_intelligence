import json

import numpy as np
import pandas as pd
import pytest

from insider_intel.eval.harness import (BudgetExceeded, BudgetPlanner, ReportWriter, ResponseCache,
                                        bootstrap_ci, md_table, prompt_hash)


def test_prompt_hash():
    h = prompt_hash("hello")
    assert len(h) == 12 and h == prompt_hash("hello") and h != prompt_hash("hellO")


def test_md_table_list_and_df():
    rows = [{"a": 1, "b": 0.5}, {"a": 2, "b": None}]
    t = md_table(rows)
    assert t.splitlines()[0] == "| a | b |"
    assert "0.5000" in t
    assert md_table(pd.DataFrame(rows)) == t
    assert "empty" in md_table([])


def test_report_writer(tmp_path):
    w = ReportWriter("demo", root=tmp_path)
    p = w.write(metrics={"acc": 0.9}, tables={"Results": [{"x": 1, "y": 2.0}]}, notes="hello notes",
                metadata={"dataset": "d", "dataset_sha256": "abc", "seed": 0, "models": ["m1"]})
    d = tmp_path / "demo"
    assert p.parent == d and p.name.endswith(".json")
    assert (d / "latest.md").exists() and (d / "latest.json").exists()
    data = json.loads((d / "latest.json").read_text())
    meta = data["metadata"]
    for k in ("git_sha", "git_dirty", "timestamp_utc", "python", "dataset", "dataset_sha256", "seed", "models"):
        assert k in meta
    assert data["metrics"]["acc"] == 0.9
    md = (d / "latest.md").read_text()
    assert md.index("## Metadata") < md.index("## Results") < md.index("## Notes")
    assert "| x | y |" in md and "hello notes" in md


def test_cache_hit_miss(tmp_path):
    c = ResponseCache(tmp_path / "c" / "llm.sqlite")
    calls = []

    def fn():
        calls.append(1)
        return {"text": "hi"}

    kp = dict(provider="groq", model="m", prompt_hash=prompt_hash("p"),
              messages=[{"role": "user", "content": "x"}], params={"t": 0, "a": 1})
    assert c.cached_call(fn, kp) == {"text": "hi"}
    assert c.cached_call(fn, kp) == {"text": "hi"}
    assert len(calls) == 1 and c.stats() == {"hits": 1, "misses": 1}
    # param key order is canonical; changed param -> miss
    kp2 = dict(kp, params={"a": 1, "t": 0})
    c.cached_call(fn, kp2)
    assert len(calls) == 1
    c.cached_call(fn, dict(kp, params={"a": 1, "t": 1}))
    assert len(calls) == 2
    # persisted on disk
    c2 = ResponseCache(tmp_path / "c" / "llm.sqlite")
    key = ResponseCache.make_key("groq", "m", prompt_hash("p"), kp["messages"], kp["params"])
    assert c2.get(key) == {"text": "hi"} and c2.get("nope") is None
    assert c2.stats() == {"hits": 1, "misses": 1}


def test_budget():
    b = BudgetPlanner(planned_calls=100, est_tokens_per_call=2000)
    e = b.check()
    assert e["minutes_at_tpm"] == pytest.approx(25.0)
    assert e["daily_quota_share"] == pytest.approx(0.1)
    assert "25.0 min" in b.summary()
    with pytest.raises(BudgetExceeded):
        BudgetPlanner(100, 2000, remaining_requests=99).check()
    with pytest.raises(BudgetExceeded):
        BudgetPlanner(1001, 10).check()


def test_bootstrap_ci():
    rng = np.random.default_rng(1)
    x = rng.normal(5, 1, 200)
    p, lo, hi = bootstrap_ci(x, n=500, seed=0)
    assert p == pytest.approx(x.mean()) and lo < p < hi and hi - lo < 0.6
    assert bootstrap_ci(x, n=500, seed=0) == (p, lo, hi)  # deterministic
    yt = rng.integers(0, 2, 300)
    yp = np.where(rng.random(300) < 0.8, yt, 1 - yt)
    acc = lambda a, b: float((a == b).mean())
    p2, lo2, hi2 = bootstrap_ci(yt, yp, acc, n=500)
    assert lo2 < p2 < hi2 and 0.7 < p2 < 0.9
    assert bootstrap_ci([1, 1, 1, 1], n=50) == (1.0, 1.0, 1.0)
