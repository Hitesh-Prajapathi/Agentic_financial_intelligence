"""Offline guard for the eval tests: no real HTTP, and no SEC_USER_AGENT leaking in from the shell."""
import httpx
import pytest


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    def boom(*a, **k):
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", boom)
    monkeypatch.setattr(httpx, "stream", boom)
