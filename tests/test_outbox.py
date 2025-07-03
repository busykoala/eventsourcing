from typing import Any
from typing import Dict

import pytest

from eventsourcing.interfaces import Message


@pytest.mark.asyncio
async def test_outbox_enqueue_dequeue(outbox):
    m1: Message[Dict[str, Any]] = Message[Dict[str, Any]](
        name="O1", payload={}, stream="s"
    )
    m2: Message[Dict[str, Any]] = Message[Dict[str, Any]](
        name="O2", payload={}, stream="s"
    )
    await outbox.enqueue([m1, m2])
    assert await outbox.dequeue() == [m1, m2]
    assert await outbox.dequeue() == []


@pytest.mark.asyncio
async def test_outbox_mark_failed(outbox):
    m: Message[Dict[str, Any]] = Message[Dict[str, Any]](
        name="F", payload={}, stream="s"
    )
    await outbox.enqueue([m])
    # should not raise
    await outbox.mark_failed([m], Exception("err"))
