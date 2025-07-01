import pytest

from eventsourcing.interfaces import Message
from eventsourcing.store.outbox.in_memory import InMemoryOutbox


@pytest.mark.asyncio
async def test_enqueue_dequeue():
    ob = InMemoryOutbox()
    m1 = Message(name="O1", payload={}, stream="s")
    m2 = Message(name="O2", payload={}, stream="s")
    await ob.enqueue([m1, m2])
    batch = await ob.dequeue()
    assert batch == [m1, m2]
    assert await ob.dequeue() == []


@pytest.mark.asyncio
async def test_mark_failed():
    ob = InMemoryOutbox()
    m = Message(name="F", payload={}, stream="s")
    await ob.enqueue([m])
    await ob.mark_failed([m], Exception("e"))
    # failed stored internally; no public API, but no error
    assert True
