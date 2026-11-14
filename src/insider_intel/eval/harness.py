"""Shared evaluation harness: report writer, response cache, budget planner, bootstrap CI."""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import platform
import sqlite3
import subprocess
import threading
from pathlib import Path
from typing import Any, Callable, Sequence

import numpy as np

__all__ = [
    "ReportWriter", "prompt_hash", "ResponseCache", "BudgetPlanner",
    "BudgetExceeded", "bootstrap_ci", "md_table",
]


def prompt_hash(text: str) -> str:
    """sha256 of text, first 12 hex chars."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def _canon(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False)


# --------------------------------------------------------------------------- reports
def _git(args: list[str], cwd: str | Path | None = None) -> str | None:
    try:
        out = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, timeout=10)
        return out.stdout.strip() if out.returncode == 0 else None
    except Exception:
        return None


def git_info(cwd: str | Path | None = None) -> dict:
    sha = _git(["rev-parse", "HEAD"], cwd)
    # dirty = tracked files differ from the commit; untracked outputs (reports/, notes) do not change the code
    dirty = _git(["status", "--porcelain", "--untracked-files=no"], cwd)
    return {"git_sha": sha or "unknown", "git_dirty": bool(dirty) if dirty is not None else None}


def md_table(rows: Any, floatfmt: str = "{:.4f}") -> str:
    """Render a list of dicts or a pandas DataFrame as a markdown table."""
    if hasattr(rows, "to_dict") and hasattr(rows, "columns"):
        rows = rows.to_dict(orient="records")
    rows = list(rows)
    if not rows:
        return "_(empty)_\n"
    cols: list[str] = []
    for r in rows:
        for k in r:
            if k not in cols:
                cols.append(k)

    def fmt(v: Any) -> str:
        if isinstance(v, float):
            return "" if v != v else floatfmt.format(v)
        if v is None:
            return ""
        return str(v).replace("|", "\\|").replace("\n", " ")

    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for r in rows:
        lines.append("| " + " | ".join(fmt(r.get(c)) for c in cols) + " |")
    return "\n".join(lines) + "\n"


class ReportWriter:
    """Writes reports/<name>/<YYYYMMDD>_<shortsha>.json plus latest.md and latest.json."""

    def __init__(self, name: str, root: str | Path = "reports", repo_dir: str | Path | None = None):
        self.name = name
        self.dir = Path(root) / name
        self.repo_dir = repo_dir

    def metadata(self, **extra: Any) -> dict:
        meta = {
            **git_info(self.repo_dir),
            "timestamp_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
            "python": platform.python_version(),
        }
        meta.update(extra)
        return meta

    def write(self, metrics: dict[str, Any] | None = None, tables: dict[str, Any] | None = None,
              notes: str = "", metadata: dict | None = None) -> Path:
        """metrics: JSON-able dict (stored in json). tables: {title: list[dict] | DataFrame}.

        metadata keys (dataset name/version/sha256, model ids, prompt hashes, seed,
        call/token totals, ...) are merged into the auto-filled block.
        """
        meta = self.metadata(**(metadata or {}))
        tables = tables or {}
        self.dir.mkdir(parents=True, exist_ok=True)
        table_rows = {}
        for title, t in tables.items():
            table_rows[title] = t.to_dict(orient="records") if hasattr(t, "to_dict") and hasattr(t, "columns") else list(t)
        payload = {"name": self.name, "metadata": meta, "metrics": metrics or {},
                   "tables": table_rows, "notes": notes}
        short = str(meta["git_sha"])[:7]
        stamp = meta["timestamp_utc"][:10].replace("-", "")
        dated = self.dir / f"{stamp}_{short}.json"
        text = json.dumps(payload, indent=2, default=str)
        dated.write_text(text)
        (self.dir / "latest.json").write_text(text)

        md = [f"# Report: {self.name}", "", "## Metadata", ""]
        for k, v in meta.items():
            md.append(f"- **{k}**: `{v if not isinstance(v, (dict, list)) else _canon(v)}`")
        md.append("")
        if metrics:
            flat = [{"metric": k, "value": v} for k, v in metrics.items()
                    if isinstance(v, (int, float, str, bool)) or v is None]
            if flat:
                md += ["## Metrics", "", md_table(flat)]
        for title, t in tables.items():
            md += [f"## {title}", "", md_table(t)]
        if notes:
            md += ["## Notes", "", notes, ""]
        (self.dir / "latest.md").write_text("\n".join(md))
        return dated


# --------------------------------------------------------------------------- cache
class ResponseCache:
    """sqlite3 on-disk cache for LLM responses; records hits/misses."""

    def __init__(self, path: str | Path = "data/cache/llm.sqlite"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(str(self.path), check_same_thread=False)
        self._db.execute("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value TEXT NOT NULL, created TEXT NOT NULL)")
        self._db.commit()
        self.hits = 0
        self.misses = 0

    @staticmethod
    def make_key(provider: str, model: str, prompt_hash_: str, messages: Any = None, params: Any = None) -> str:
        return hashlib.sha256(_canon([provider, model, prompt_hash_, messages, params]).encode()).hexdigest()

    def get(self, key: str) -> Any | None:
        with self._lock:
            row = self._db.execute("SELECT value FROM cache WHERE key=?", (key,)).fetchone()
            if row is None:
                self.misses += 1
                return None
            self.hits += 1
            return json.loads(row[0])

    def has(self, key: str) -> bool:
        with self._lock:
            return self._db.execute("SELECT 1 FROM cache WHERE key=?", (key,)).fetchone() is not None

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO cache VALUES (?,?,?)",
                             (key, json.dumps(value, default=str),
                              _dt.datetime.now(_dt.timezone.utc).isoformat()))
            self._db.commit()

    def cached_call(self, fn: Callable[[], Any], key_parts: dict) -> Any:
        """key_parts: provider, model, prompt_hash, messages (opt), params (opt). Value must be JSON-able."""
        key = self.make_key(key_parts["provider"], key_parts["model"], key_parts["prompt_hash"],
                            key_parts.get("messages"), key_parts.get("params"))
        with self._lock:
            row = self._db.execute("SELECT value FROM cache WHERE key=?", (key,)).fetchone()
            if row is not None:
                self.hits += 1
                return json.loads(row[0])
            self.misses += 1
        value = fn()
        self.put(key, value)
        return value

    def stats(self) -> dict:
        return {"hits": self.hits, "misses": self.misses}

    def close(self) -> None:
        self._db.close()


# --------------------------------------------------------------------------- budget
class BudgetExceeded(RuntimeError):
    pass


class BudgetPlanner:
    """Pre-flight check of planned (uncached) LLM calls against free-tier quotas."""

    def __init__(self, planned_calls: int, est_tokens_per_call: int, daily_request_limit: int = 1000,
                 tpm_limit: int = 8000, remaining_requests: int | None = None):
        self.planned_calls = planned_calls
        self.est_tokens_per_call = est_tokens_per_call
        self.daily_request_limit = daily_request_limit
        self.tpm_limit = tpm_limit
        self.remaining_requests = daily_request_limit if remaining_requests is None else remaining_requests

    def estimate(self) -> dict:
        total_tokens = self.planned_calls * self.est_tokens_per_call
        return {
            "planned_calls": self.planned_calls,
            "total_tokens": total_tokens,
            "minutes_at_tpm": total_tokens / self.tpm_limit if self.tpm_limit else float("inf"),
            "daily_quota_share": self.planned_calls / self.daily_request_limit,
            "remaining_requests": self.remaining_requests,
        }

    def check(self) -> dict:
        est = self.estimate()
        if self.planned_calls > self.remaining_requests:
            raise BudgetExceeded(
                f"{self.planned_calls} planned uncached calls exceed remaining quota "
                f"{self.remaining_requests} (daily limit {self.daily_request_limit})")
        return est

    def summary(self) -> str:
        e = self.estimate()
        return (f"{e['planned_calls']} calls x ~{self.est_tokens_per_call} tok = {e['total_tokens']} tokens; "
                f"~{e['minutes_at_tpm']:.1f} min at {self.tpm_limit} TPM; "
                f"{e['daily_quota_share']:.1%} of daily quota ({e['remaining_requests']} requests remaining)")


# --------------------------------------------------------------------------- bootstrap
def bootstrap_ci(values_or_ytrue: Sequence | np.ndarray, y_pred: Sequence | None = None,
                 metric_fn: Callable | None = None, n: int = 2000, alpha: float = 0.05,
                 seed: int = 0) -> tuple[float, float, float]:
    """Percentile bootstrap CI. Either bootstrap_ci(values[, metric_fn=np.mean]) or
    bootstrap_ci(y_true, y_pred, metric_fn). Returns (point, lo, hi)."""
    rng = np.random.default_rng(seed)
    a = np.asarray(values_or_ytrue)
    N = len(a)
    if y_pred is None:
        fn = metric_fn or np.mean
        point = float(fn(a))
        stats = [fn(a[rng.integers(0, N, N)]) for _ in range(n)]
    else:
        if metric_fn is None:
            raise ValueError("metric_fn required with (y_true, y_pred)")
        b = np.asarray(y_pred)
        point = float(metric_fn(a, b))
        stats = []
        for _ in range(n):
            idx = rng.integers(0, N, N)
            stats.append(metric_fn(a[idx], b[idx]))
    lo, hi = np.nanpercentile(np.asarray(stats, dtype=float), [100 * alpha / 2, 100 * (1 - alpha / 2)])
    return point, float(lo), float(hi)
