import pytest

from eventsourcing.interfaces import Message
from eventsourcing.middleware import dedupe_middleware
from eventsourcing.middleware import retry_middleware


@pytest.mark.asyncio
async def test_retry_succeeds_after_failure(monkeypatch):
    calls = []

    async def flaky(msg):
        calls.append(1)
        if len(calls) < 2:
            raise ValueError("fail")
        return []

    msg = Message(name="T", payload={}, stream="s")
    out = await retry_middleware(msg, flaky, retries=3, backoff=0)
    assert out == []
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_retry_exhausts():
    async def always_fail(msg):
        raise RuntimeError

    msg = Message(name="U", payload={}, stream="s")
    with pytest.raises(RuntimeError):
        await retry_middleware(msg, always_fail, retries=2, backoff=0)


@pytest.mark.asyncio
async def test_dedupe_prevents_double(monkeypatch):
    processed = []

    async def handler(msg):
        processed.append(msg.id)
        return []

    msg = Message(name="V", payload={}, stream="s")
    # first
    _ = await dedupe_middleware(msg, handler)
    out2 = await dedupe_middleware(msg, handler)
    assert processed == [msg.id]
    assert out2 == []
