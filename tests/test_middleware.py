import pytest

from eventsourcing.interfaces import Message
from eventsourcing.middleware import dedupe_middleware
from eventsourcing.middleware import retry_middleware


@pytest.mark.asyncio
async def test_retry_middleware(monkeypatch):
    calls = []

    async def flaky(msg):
        calls.append(1)
        if len(calls) < 2:
            raise ValueError("fail")
        return []

    out = await retry_middleware(
        Message(name="T", payload={}, stream="s"), flaky, retries=3, backoff=0
    )
    assert out == []
    assert len(calls) == 2

    with pytest.raises(RuntimeError):
        await retry_middleware(
            Message(name="U", payload={}, stream="s"),
            lambda m: (_ for _ in ()).throw(RuntimeError()),
            retries=2,
            backoff=0,
        )


@pytest.mark.asyncio
async def test_dedupe_middleware():
    processed = []

    async def handler(msg):
        processed.append(msg.id)
        return []

    msg = Message(name="V", payload={}, stream="s")
    assert await dedupe_middleware(msg, handler) == []
    # second time, it's deduped
    assert await dedupe_middleware(msg, handler) == []
    assert processed == [msg.id]
