import pytest

from eventsourcing.interfaces import Message
from eventsourcing.pubsub.in_memory import InMemoryPubSub


@pytest.mark.asyncio
async def test_publish_subscribe_single():
    broker = InMemoryPubSub()
    q = await broker.subscribe("topic1")
    m = Message(name="A", payload={"x": 1}, stream="topic1")
    await broker.publish("topic1", m)
    got = await q.get()
    assert got == m


@pytest.mark.asyncio
async def test_multiple_subscribers_get_same():
    broker = InMemoryPubSub()
    q1, q2 = await broker.subscribe("t"), await broker.subscribe("t")
    m = Message(name="B", payload={}, stream="t")
    await broker.publish("t", m)
    assert await q1.get() == m
    assert await q2.get() == m


@pytest.mark.asyncio
async def test_close_puts_none():
    broker = InMemoryPubSub()
    q = await broker.subscribe("x")
    await broker.close()
    assert await q.get() is None
