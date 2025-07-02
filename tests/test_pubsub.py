import pytest

from eventsourcing.interfaces import Message


@pytest.mark.asyncio
async def test_publish_subscribe(pubsub):
    q = await pubsub.subscribe("t1")
    m = Message(name="A", payload={"x": 1}, stream="t1")
    await pubsub.publish("t1", m)
    assert await q.get() == m

    # multiple subscribers
    q1, q2 = await pubsub.subscribe("t2"), await pubsub.subscribe("t2")
    m2 = Message(name="B", payload={}, stream="t2")
    await pubsub.publish("t2", m2)
    assert await q1.get() == m2 and await q2.get() == m2

    # close
    q3 = await pubsub.subscribe("t3")
    await pubsub.close()
    assert await q3.get() is None
