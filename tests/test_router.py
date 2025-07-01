import asyncio

import pytest

from eventsourcing.interfaces import Message
from eventsourcing.pubsub.in_memory import InMemoryPubSub
from eventsourcing.router import Router


class DummyHandler:
    def __init__(self):
        self.received = []

    async def handle(self, msg: Message):
        self.received.append(msg)
        return []


@pytest.mark.asyncio
async def test_routing_and_middleware(event_loop):
    broker = InMemoryPubSub()
    router = Router(broker)
    handler = DummyHandler()
    router.add_route("t", handler)
    # no middleware
    # start router
    task = asyncio.create_task(router.run())
    # publish a message
    m = Message(name="X", payload={}, stream="t")
    await broker.publish("t", m)
    await asyncio.sleep(0.1)
    assert handler.received == [m]
    await broker.close()
    await task
