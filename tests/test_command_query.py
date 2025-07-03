from typing import Any
from typing import Dict

import pytest

from eventsourcing.interfaces import Message


@pytest.mark.asyncio
async def test_command_and_query(outbox, read_model):
    # CommandHandler
    enq = []

    async def fake_e(msgs):
        enq.extend(msgs)

    from eventsourcing.command_handler import CommandHandler

    cmd: Message[Dict[str, Any]] = Message[Dict[str, Any]](
        name="DoIt", payload={"x": 1}, stream="s"
    )
    await CommandHandler(fake_e).handle(cmd)
    assert enq and enq[0].name == "DoItExecuted"

    # QueryHandler
    await read_model.save("Q", {"v": 1})
    from eventsourcing.query_handler import QueryHandler

    res = await QueryHandler(read_model).handle(
        Message[Dict[str, Any]](name="Q", payload={}, stream="s")
    )
    assert res == {"v": 1}
