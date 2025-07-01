import pytest

from eventsourcing.store.read_model.in_memory import InMemoryReadModelStore


@pytest.mark.asyncio
async def test_save_and_load():
    rm = InMemoryReadModelStore()
    await rm.save("key", {"a": 1})
    assert await rm.load("key") == {"a": 1}
    assert await rm.load("missing") is None
