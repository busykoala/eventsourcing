import asyncio
import logging
from typing import Any
from typing import Dict

import pytest

from eventsourcing.interfaces import Message


@pytest.mark.asyncio
async def test_pubsub_logging(pubsub, caplog):
    caplog.set_level(logging.DEBUG)
    m1: Message[Dict[str, Any]] = Message[Dict[str, Any]](
        name="Test1", payload={}, stream="s1"
    )
    await pubsub.publish("s1", m1)
    assert "Buffered 1 msgs on 's1'" in caplog.text

    caplog.clear()
    await pubsub.subscribe("s1")
    caplog.set_level(logging.INFO)
    m2: Message[Dict[str, Any]] = Message[Dict[str, Any]](
        name="Test2", payload={}, stream="s1"
    )
    await pubsub.publish("s1", m2)
    assert "Published 1 msgs to 's1'" in caplog.text


@pytest.mark.asyncio
async def test_outbox_and_aloproc_logging(aloproc, outbox, broker, caplog):
    caplog.set_level(logging.INFO)
    # Prime subscription
    await broker.subscribe("s2")

    task = asyncio.create_task(aloproc.run())
    await outbox.enqueue(
        [Message[Dict[str, Any]](name="Evt", payload={}, stream="s2")]
    )
    await asyncio.sleep(0.1)
    aloproc.stop_event.set()
    await task

    assert "Appended 1 msgs to 's2'" in caplog.text
    assert "Delivered 1 msgs to 's2'" in caplog.text
