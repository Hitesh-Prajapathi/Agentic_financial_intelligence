"""Minimal observability: a `timed()` context manager that logs one JSON line per stage (stdlib logging only).

    with timed("pipeline.run", run_id=run_id) as span:
        ...
        span.set(status="success")

Never put raw user text in attributes (use lengths / hashes instead).
"""
from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager

logger = logging.getLogger("insider_intel.obs")


class Span:
    def __init__(self, attrs: dict):
        self.attrs = dict(attrs)
        self.duration_ms: float | None = None  # set when the block exits

    def set(self, **attrs) -> None:
        self.attrs.update(attrs)


@contextmanager
def timed(name: str, **attrs):
    span = Span(attrs)
    t0 = time.perf_counter()
    outcome = "ok"
    try:
        yield span
    except GeneratorExit:  # a streaming consumer stopped early; not a failure
        outcome = "cancelled"
        raise
    except BaseException:
        outcome = "error"
        raise
    finally:
        span.duration_ms = round((time.perf_counter() - t0) * 1000, 1)
        logger.info(json.dumps({"event": name, "duration_ms": span.duration_ms, "outcome": outcome, **span.attrs}, default=str))
