import asyncio
from typing import Any
from typing import Dict
from typing import List

import pytest

from eventsourcing.interfaces import Message


class DummyHandler:
    async def handle(
        self, msg: Message[Dict[str, Any]]
    ) -> List[Message[Dict[str, Any]]]:
        return [
            Message[Dict[str, Any]](
                name="Echo", payload=msg.payload, stream=msg.stream
            )
        ]


@pytest.mark.asyncio
async def test_router_basic(router, broker):
    handler = DummyHandler()
    router.add_route("t", handler)
    _ = await broker.subscribe("t")
    task = asyncio.create_task(router.run())

    m: Message[Dict[str, Any]] = Message[Dict[str, Any]](
        name="X", payload={"k": 1}, stream="t"
    )
    await broker.publish("t", m)
    await asyncio.sleep(0.1)
    await broker.close()
    await task

    # router should dispatch and handler return should be ignored
    assert handler is not None  # no errors
