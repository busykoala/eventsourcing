import pytest


@pytest.mark.asyncio
async def test_read_model_crud(read_model):
    assert await read_model.load("nofound") is None
    await read_model.save("k", {"a": 1})
    assert await read_model.load("k") == {"a": 1}
