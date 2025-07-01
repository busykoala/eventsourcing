import asyncio

import pytest

from eventsourcing.config import ESConfig
from eventsourcing.interfaces import Message
from eventsourcing.processor import OutboxProcessor
from eventsourcing.pubsub.in_memory import InMemoryPubSub
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox


@pytest.mark.asyncio
async def test_dead_stream_branch(monkeypatch):
    es = InMemoryEventStore()
    ob = InMemoryOutbox()
    pub = InMemoryPubSub()
    stop = asyncio.Event()
    cfg = ESConfig(
        publisher=pub,
        subscriber=pub,
        dead_stream="dead",
        polling_interval=0.01,
    )
    proc = OutboxProcessor(es, ob, pub, cfg, stop)
    # Enqueue one for success
    m1 = Message(name="A", payload={}, stream="x")
    await ob.enqueue([m1])
    # Enqueue one that will fail version check
    m2 = Message(name="B", payload={}, stream="x")
    await ob.enqueue([m2])

    # Monkey-patch append_to_stream to always fail
    async def fail_append(*args, **kwargs):
        raise RuntimeError("conflict")

    monkeypatch.setattr(es, "append_to_stream", fail_append)
    # Run one iteration
    stop.set()
    await proc.run()
    q = await pub.subscribe("dead")
    got = [await q.get(), await q.get()]
    assert {msg.name for msg in got} == {"A", "B"}
