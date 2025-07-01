import asyncio
import logging

import pytest

from eventsourcing.interfaces import Message
from eventsourcing.processor import OutboxProcessor
from eventsourcing.pubsub.in_memory import InMemoryPubSub
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox


@pytest.mark.asyncio
async def test_pubsub_logs_publish_and_buffer(caplog):
    caplog.set_level(logging.INFO)
    broker = InMemoryPubSub()

    # Case 1: no subscriber yet, so should buffer and log DEBUG
    caplog.set_level(logging.DEBUG)
    m1 = Message(name="Test1", payload={}, stream="s1")
    await broker.publish("s1", m1)
    assert any(
        "Buffered 1 msgs on 's1'" in rec.getMessage()
        and rec.levelno == logging.DEBUG
        for rec in caplog.records
    )

    # Case 2: with subscriber, should log INFO on publish
    caplog.clear()
    await broker.subscribe("s1")
    caplog.set_level(logging.INFO)
    m2 = Message(name="Test2", payload={}, stream="s1")
    await broker.publish("s1", m2)
    assert any(
        "Published 1 msgs to 's1'" in rec.getMessage()
        and rec.levelno == logging.INFO
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_outbox_and_processor_logging(caplog):
    caplog.set_level(logging.INFO)
    es = InMemoryEventStore()
    ob = InMemoryOutbox()
    broker = InMemoryPubSub()

    # subscribe to the default stream
    _ = await broker.subscribe("s2")

    proc = OutboxProcessor(es, ob, broker, dead_stream="dead")
    task = asyncio.create_task(proc.run())

    # enqueue one message
    m = Message(name="Evt", payload={}, stream="s2")
    await ob.enqueue([m])

    # give processor a moment
    await asyncio.sleep(0.2)
    task.cancel()

    # Check that append_to_stream logged an info
    assert any(
        "Appended 1 msgs to 's2'" in rec.getMessage()
        and rec.levelno == logging.INFO
        for rec in caplog.records
    )

    # Check that forwarder logged an info
    assert any(
        "Forwarded batch of 1 to 's2'" in rec.getMessage()
        and rec.levelno == logging.INFO
        for rec in caplog.records
    )
