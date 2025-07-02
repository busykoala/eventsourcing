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
async def test_sqlite_roundtrip(sqlite_store):
    msg = Message(name="TestEvent", payload=b'{"foo": "bar"}')
    await sqlite_store.append("stream", [msg])
    res = await sqlite_store.read("stream")
    assert len(res) == 1
    assert res[0].payload == msg.payload
