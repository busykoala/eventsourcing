import pytest

from eventsourcing.interfaces import Message
from eventsourcing.pubsub.in_memory import InMemoryPubSub


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "limit, count, expected",
    [
        (3, 5, 3),
        (2, 2, 2),
        (1, 0, 0),
    ],
)
async def test_backlog_cap_eviction(limit, count, expected):
    pubsub = InMemoryPubSub(backlog_limit=limit)
    # Publish count messages to empty stream (buffers)
    for i in range(count):
        await pubsub.publish("t", Message(name=str(i), payload={}, stream="t"))
    # Inspect backlog directly
    backlog = pubsub._backlog.get("t", [])
    assert len(backlog) == expected
