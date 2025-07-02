import pytest


@pytest.mark.asyncio
async def test_dead_stream_batching_on_append_failure(
    batchproc,
    event_store,
    outbox,
    broker,
    make_message,
    stop_event,
    monkeypatch,
):
    # Enqueue two messages on the same stream
    msgs = [make_message(name=n, stream="x") for n in ("A", "B")]
    await outbox.enqueue(msgs)

    # Patch append_to_stream to always fail
    async def fail_append(*args, **kwargs):
        raise RuntimeError("conflict")

    monkeypatch.setattr(event_store, "append_to_stream", fail_append)

    # Signal stop so that run() will process one batch then exit
    stop_event.set()
    await batchproc.run()

    # Subscribe to dead stream and verify both messages ended up there
    q = await broker.subscribe("dead")
    received = {await q.get(), await q.get()}
    assert {m.name for m in received} == {"A", "B"}
