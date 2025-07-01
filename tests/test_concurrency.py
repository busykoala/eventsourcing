import asyncio

import pytest

from eventsourcing.interfaces import Message
from eventsourcing.store.event_store.in_memory import InMemoryEventStore


@pytest.mark.asyncio
async def test_concurrent_appends():
    store = InMemoryEventStore()
    msgs = [Message(name=f"E{i}", payload={}, stream="s") for i in range(10)]

    async def append_part(part):
        await store.append_to_stream(part)

    # run two coroutines appending interleaved
    await asyncio.gather(
        append_part(msgs[:5]),
        append_part(msgs[5:]),
    )
    events = await store.read_stream("s")
    assert len(events) == 10
