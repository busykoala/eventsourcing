import asyncio

import pytest

from eventsourcing import Message


@pytest.mark.asyncio
async def test_aloproc_basic_delivery(aloproc, outbox, broker, stop_event):
    q = await broker.subscribe("s")
    await outbox.enqueue([Message(name="E", payload={}, stream="s")])
    t = asyncio.create_task(aloproc.run())
    await asyncio.sleep(0.05)
    stop_event.set()
    await t
    assert await q.get()
