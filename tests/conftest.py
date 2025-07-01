import asyncio

import pytest

from eventsourcing.interfaces import Message
from eventsourcing.processor import OutboxProcessor
from eventsourcing.pubsub.in_memory import InMemoryPubSub
from eventsourcing.router import Router
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox
from eventsourcing.store.read_model.in_memory import InMemoryReadModelStore


@pytest.fixture
def pubsub():
    return InMemoryPubSub()


@pytest.fixture
def event_store():
    return InMemoryEventStore()


@pytest.fixture
def outbox():
    return InMemoryOutbox()


@pytest.fixture
def read_model():
    return InMemoryReadModelStore()


@pytest.fixture
def router(pubsub):
    return Router(pubsub)


@pytest.fixture
def processor(event_store, outbox, pubsub):
    return OutboxProcessor(event_store, outbox, pubsub, dead_stream="dead")


@pytest.fixture(autouse=True)
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def make_message(name="Test", payload=None, stream="s"):
    return Message(name=name, payload=payload or {}, stream=stream)
