import uuid

import aiosqlite
import pytest
import pytest_asyncio

from eventsourcing.interfaces import Message
from eventsourcing.store.event_store.sqlite import SQLiteEventStore


@pytest_asyncio.fixture
async def sqlite_store():
    conn = await aiosqlite.connect(":memory:")
    store = SQLiteEventStore(":memory:", connection=conn)
    await store._ensure_initialized()
    yield store
    await conn.close()


@pytest.mark.asyncio
async def test_sqlite_event_store_roundtrip(sqlite_store: SQLiteEventStore):
    stream = "test-stream"
    headers = {
        "content-type": "application/json",
        "correlation-id": str(uuid.uuid4()),
    }
    msg = Message(name="TestEvent", payload={"foo": "bar"}, headers=headers)

    await sqlite_store.append(stream, [msg])
    result = await sqlite_store.read(stream)

    assert len(result) == 1
    stored = result[0]
    assert stored.name == msg.name
    assert stored.payload == msg.payload
    assert stored.headers == msg.headers
