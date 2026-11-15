"""EdgarClient: rate limiting, retries, caching, UA handling. No network (httpx.MockTransport)."""

import httpx
import pytest

from insider_intel.edgar.client import (
    EdgarBlockedError,
    EdgarClient,
    EdgarConfigError,
    EdgarHTTPError,
    EdgarNotFound,
    TokenBucket,
)

ACC = "0000000000-26-000001"


class FakeTime:
    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, s):
        self.sleeps.append(s)
        self.now += s


def make(handler, tmp_path, ft=None, **kw):
    ft = ft or FakeTime()
    c = EdgarClient(
        user_agent="test-agent contact@example.invalid",
        cache_dir=tmp_path / "raw",
        transport=httpx.MockTransport(handler),
        clock=ft.clock,
        sleep=ft.sleep,
        **kw,
    )
    return c, ft


def test_missing_user_agent_raises_clear_error(monkeypatch):
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    with pytest.raises(EdgarConfigError, match="SEC_USER_AGENT"):
        EdgarClient()


def test_user_agent_from_env_is_sent(monkeypatch, tmp_path):
    monkeypatch.setenv("SEC_USER_AGENT", "env-agent contact@example.invalid")
    seen = {}

    def handler(req):
        seen["ua"] = req.headers["user-agent"]
        return httpx.Response(200, text="ok")

    c = EdgarClient(cache_dir=tmp_path, transport=httpx.MockTransport(handler))
    assert c.get_text("https://www.sec.gov/x") == "ok"
    assert seen["ua"] == "env-agent contact@example.invalid"


def test_injected_transport_needs_no_user_agent(monkeypatch, tmp_path):
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    EdgarClient(cache_dir=tmp_path, transport=httpx.MockTransport(lambda r: httpx.Response(200)))


def test_rate_above_sec_limit_rejected(tmp_path):
    with pytest.raises(ValueError):
        EdgarClient(user_agent="x", rate_per_sec=10, cache_dir=tmp_path)


def test_token_bucket_spaces_requests_at_the_configured_rate():
    ft = FakeTime()
    b = TokenBucket(8.0, 1.0, ft.clock, ft.sleep)
    start = ft.now
    for _ in range(17):
        b.acquire()
    assert ft.now - start == pytest.approx(16 / 8.0)  # 17 requests -> 16 gaps of 125 ms


def test_client_never_exceeds_8_requests_per_second(tmp_path):
    stamps = []
    ft = FakeTime()

    def handler(req):
        stamps.append(ft.now)
        return httpx.Response(200, text="ok")

    c, _ = make(handler, tmp_path, ft)
    for _ in range(40):
        c.get("https://www.sec.gov/x")
    for i in range(len(stamps)):
        in_window = [t for t in stamps if stamps[i] <= t < stamps[i] + 1.0]
        assert len(in_window) <= 8


def test_retries_429_then_succeeds_honouring_retry_after(tmp_path):
    calls = []

    def handler(req):
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "3"})
        return httpx.Response(200, text="done")

    c, ft = make(handler, tmp_path)
    assert c.get_text("https://www.sec.gov/x") == "done"
    assert len(calls) == 2
    assert 3.0 in ft.sleeps


def test_exponential_backoff_on_5xx_then_gives_up(tmp_path):
    def handler(req):
        return httpx.Response(503)

    c, ft = make(handler, tmp_path, max_retries=3, backoff_base=0.5)
    with pytest.raises(EdgarHTTPError) as ei:
        c.get("https://www.sec.gov/x")
    assert ei.value.status == 503
    assert c.request_count == 4  # 1 + 3 retries
    backoffs = [s for s in ft.sleeps if s >= 0.5 and s in (0.5, 1.0, 2.0)]
    assert backoffs == [0.5, 1.0, 2.0]


def test_timeouts_are_retried(tmp_path):
    n = {"i": 0}

    def handler(req):
        n["i"] += 1
        if n["i"] < 3:
            raise httpx.ReadTimeout("slow", request=req)
        return httpx.Response(200, text="ok")

    c, _ = make(handler, tmp_path)
    assert c.get_text("https://www.sec.gov/x") == "ok"
    assert n["i"] == 3


def test_403_is_not_retried(tmp_path):
    n = {"i": 0}

    def handler(req):
        n["i"] += 1
        return httpx.Response(403)

    c, _ = make(handler, tmp_path)
    with pytest.raises(EdgarBlockedError):
        c.get("https://www.sec.gov/x")
    assert n["i"] == 1


def test_404_raises_not_found_without_retry(tmp_path):
    n = {"i": 0}

    def handler(req):
        n["i"] += 1
        return httpx.Response(404)

    c, _ = make(handler, tmp_path)
    with pytest.raises(EdgarNotFound):
        c.get("https://www.sec.gov/x")
    assert n["i"] == 1


def test_document_is_fetched_once_and_cached_on_disk(tmp_path):
    n = {"i": 0}

    def handler(req):
        n["i"] += 1
        return httpx.Response(200, content=b"<ownershipDocument/>")

    c, _ = make(handler, tmp_path)
    assert c.fetch_document_cached(ACC, "https://www.sec.gov/doc.xml") == b"<ownershipDocument/>"
    assert c.fetch_document_cached(ACC, "https://www.sec.gov/doc.xml") == b"<ownershipDocument/>"
    assert n["i"] == 1
    assert (tmp_path / "raw" / f"{ACC}.xml").read_bytes() == b"<ownershipDocument/>"
    # a fresh client over the same cache dir does no network at all
    c2, _ = make(lambda r: pytest.fail("network used"), tmp_path)
    assert c2.fetch_document_cached(ACC, "https://www.sec.gov/doc.xml") == b"<ownershipDocument/>"


def test_cache_is_immutable_write_once(tmp_path):
    c, _ = make(lambda r: httpx.Response(200), tmp_path)
    c.write_cached(ACC, b"first")
    c.write_cached(ACC, b"second")
    assert c.read_cached(ACC) == b"first"
    assert [p.name for p in (tmp_path / "raw").iterdir()] == [f"{ACC}.xml"]  # no temp files left


def test_bad_accession_cannot_escape_cache_dir(tmp_path):
    c, _ = make(lambda r: httpx.Response(200), tmp_path)
    with pytest.raises(ValueError):
        c.cache_path("../../etc/passwd")


def test_get_json(tmp_path):
    c, _ = make(lambda r: httpx.Response(200, json={"a": 1}), tmp_path)
    assert c.get_json("https://www.sec.gov/x.json") == {"a": 1}
