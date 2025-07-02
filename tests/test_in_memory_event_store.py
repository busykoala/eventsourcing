import pytest

from eventsourcing.interfaces import Message
from eventsourcing.store.event_store.in_memory import InMemoryEventStore


@pytest.mark.asyncio
async def test_append_and_read():
    store = InMemoryEventStore()
    m1 = Message(name="E1", payload={}, stream="s")
    m2 = Message(name="E2", payload={}, stream="s")
    await store.append_to_stream([m1, m2])
    events = await store.read_stream("s")
    assert [e.name for e in events] == ["E1", "E2"]
    assert events[0].version == 1
    assert events[1].version == 2


@pytest.mark.asyncio
async def test_version_conflict():
    store = InMemoryEventStore()
    m = Message(name="X", payload={}, stream="s")
    await store.append_to_stream([m], expected_version=0)
    m2 = Message(name="Y", payload={}, stream="s")
    with pytest.raises(RuntimeError):
        await store.append_to_stream([m2], expected_version=0)


@pytest.mark.asyncio
async def test_dedupe_by_id():
    store = InMemoryEventStore()
    m = Message(name="D", payload={}, stream="s")
    await store.append_to_stream([m])
    # same ID should be ignored
    dup = Message.model_construct(**m.model_dump())
    await store.append_to_stream([dup])
    events = await store.read_stream("s")
    assert len(events) == 1
