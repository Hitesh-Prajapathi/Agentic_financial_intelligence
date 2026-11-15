"""Shared helpers for the evaluation CLI: SEC User-Agent guard, request-budget estimate, JSON coercion."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import numpy as np

USER_AGENT_ENV = "SEC_USER_AGENT"
SEC_RATE_PER_SEC = 8.0
CONFIRM_ABOVE_REQUESTS = 200


class MissingUserAgent(RuntimeError):
    pass


def require_sec_user_agent(env: dict | None = None) -> str:
    """Return SEC_USER_AGENT or raise. Call this BEFORE any code path that can touch sec.gov
    (sec_datasets.download_quarter silently falls back to a generic UA that SEC rejects)."""
    ua = (env if env is not None else os.environ).get(USER_AGENT_ENV, "").strip()
    if not ua:
        raise MissingUserAgent(
            f"{USER_AGENT_ENV} is not set; no request was made. SEC requires a descriptive User-Agent "
            f"with contact info, e.g.\n  export {USER_AGENT_ENV}='your-app-name you@example.com'"
        )
    return ua


@dataclass
class FetchPlan:
    """BudgetPlanner-style pre-flight estimate for HTTP requests (not LLM calls)."""

    label: str
    requests: int
    rate_per_sec: float = SEC_RATE_PER_SEC

    @property
    def minutes(self) -> float:
        return self.requests / self.rate_per_sec / 60.0

    def summary(self) -> str:
        return f"{self.label}: ~{self.requests} requests, ~{self.minutes:.1f} min at {self.rate_per_sec:g} req/s"

    def needs_confirmation(self, threshold: int = CONFIRM_ABOVE_REQUESTS) -> bool:
        return self.requests > threshold


def confirm_plan(plan: FetchPlan, yes: bool, out: Callable[[str], None] = print) -> bool:
    """Print the plan; return True if the run may proceed (<= threshold requests or --yes given)."""
    out(plan.summary())
    if plan.needs_confirmation() and not yes:
        out(f"This is more than {CONFIRM_ABOVE_REQUESTS} requests. Re-run with --yes to proceed.")
        return False
    return True


def jsonable(o: Any) -> Any:
    """Recursively coerce numpy / pandas scalars to plain Python (ReportWriter drops np.int64 from its
    markdown metrics table and stringifies it in JSON)."""
    import pandas as pd

    if isinstance(o, dict):
        return {str(k): jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple, set)):
        return [jsonable(v) for v in o]
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        f = float(o)
        return None if f != f else f
    if isinstance(o, float):
        return None if o != o else o
    if isinstance(o, np.ndarray):
        return jsonable(o.tolist())
    if isinstance(o, (pd.Timestamp,)):
        return None if pd.isna(o) else o.isoformat()
    if o is pd.NaT:
        return None
    return o


def eprint(*a: Any) -> None:
    print(*a, file=sys.stderr)


def export_sec_user_agent_from_settings() -> None:
    """Shell entry points only: copy SEC_USER_AGENT from the app settings (.env) into the environment.

    Library code and tests read os.environ only, so tests can never pick up a developer's real .env.
    Call this from ``if __name__ == "__main__":`` blocks, never from ``main()``.
    """
    if os.environ.get(USER_AGENT_ENV, "").strip():
        return
    try:
        from ..config import settings
    except Exception:  # noqa: BLE001 - a broken .env must not crash the guard; the guard reports it
        return
    ua = (getattr(settings, "sec_user_agent", "") or "").strip()
    if ua:
        os.environ[USER_AGENT_ENV] = ua
