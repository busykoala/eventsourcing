import pytest

from eventsourcing.command_handler import CommandHandler
from eventsourcing.interfaces import Message
from eventsourcing.query_handler import QueryHandler


@pytest.mark.asyncio
async def test_command_handler_enqueues(outbox):
    enqueued = []

    async def fake_enqueue(msgs):
        enqueued.extend(msgs)

    handler = CommandHandler(fake_enqueue)
    cmd = Message(name="DoIt", payload={"x": 1}, stream="s")
    await handler.handle(cmd)
    assert enqueued and enqueued[0].name == "DoItExecuted"
    assert enqueued[0].causation_id == cmd.id


@pytest.mark.asyncio
async def test_query_handler_reads(read_model):
    await read_model.save("Q", {"val": 42})
    handler = QueryHandler(read_model)
    qry = Message(name="Q", payload={}, stream="s")
    result = await handler.handle(qry)
    assert result == {"val": 42}
