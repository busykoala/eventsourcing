import asyncio

import pytest

from eventsourcing.interfaces import Message
from eventsourcing.processor import OutboxProcessor
from eventsourcing.pubsub.in_memory import InMemoryPubSub
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox


@pytest.mark.asyncio
async def test_processor_appends_and_publishes():
    broker = InMemoryPubSub()
    es = InMemoryEventStore()
    ob = InMemoryOutbox()
    processor = OutboxProcessor(es, ob, broker, dead_stream="dead")
    # subscribe to stream
    q = await broker.subscribe("s")
    # enqueue one
    m = Message(name="P", payload={}, stream="s")
    await ob.enqueue([m])
    # run one iteration
    task = asyncio.create_task(processor.run())
    await asyncio.sleep(0.1)
    # event store should contain it
    events = await es.read_stream("s")
    assert events and events[0].name == "P"
    # broker queue should have message
    got = await q.get()
    assert got.name == "P"
    # clean up
    task.cancel()
