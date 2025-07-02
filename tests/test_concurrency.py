import asyncio

import pytest


@pytest.mark.parametrize("total,split", [(10, 5), (20, 10), (0, 0), (1, 1)])
@pytest.mark.asyncio
async def test_concurrent_appends(event_store, make_message, total, split):
    msgs = [make_message(name=f"E{i}", stream="s") for i in range(total)]

    async def append_part(part):
        await event_store.append_to_stream(part)

    # Run two coroutines appending interleaved slices
    await asyncio.gather(append_part(msgs[:split]), append_part(msgs[split:]))

    stored = await event_store.read_stream("s")
    assert len(stored) == total
    assert {m.name for m in stored} == {f"E{i}" for i in range(total)}
