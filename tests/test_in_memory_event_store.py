import pytest

from eventsourcing.interfaces import Message
from eventsourcing.store.event_store.in_memory import InMemoryEventStore


@pytest.mark.asyncio
async def test_append_read_and_versioning():
    store = InMemoryEventStore()
    # Append three events
    events = [Message(name=f"E{i}", payload={}, stream="s") for i in range(3)]
    await store.append_to_stream(events, expected_version=0)

    got = await store.read_stream("s")
    assert [e.version for e in got] == [1, 2, 3]

    # Version conflict
    with pytest.raises(RuntimeError):
        await store.append_to_stream(
            [Message(name="X", payload={}, stream="s")], expected_version=0
        )

    # Deduplication: reusing an existing ID should not increase count
    dup = Message.model_construct(**got[0].model_dump())
    await store.append_to_stream([dup])
    after = await store.read_stream("s")
    assert len(after) == 3
