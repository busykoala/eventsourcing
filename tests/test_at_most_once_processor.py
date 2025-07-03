import asyncio
from typing import Any
from typing import Dict

import pytest

from eventsourcing.interfaces import Message


@pytest.mark.asyncio
async def test_at_most_once_drops_on_publish_error(
    processor, outbox, event_store, broker, stop_event, monkeypatch
):
    # subscribe
    q = await broker.subscribe("s")

    # simulate publish failure
    async def fail_publish(stream, *msgs):
        raise RuntimeError("boom")

    monkeypatch.setattr(broker, "publish", fail_publish)

    # enqueue and run
    await outbox.enqueue(
        [Message[Dict[str, Any]](name="E", payload={}, stream="s")]
    )
    task = asyncio.create_task(processor.run())
    await asyncio.sleep(0.05)
    stop_event.set()
    await task

    # should still store, but not deliver
    assert [e.name for e in await event_store.read_stream("s")] == ["E"]
    assert q.empty()
