import asyncio
import uuid
from contextlib import asynccontextmanager

import pytest
from fastapi import Depends
from fastapi import FastAPI

from eventsourcing.config import ESConfig
from eventsourcing.interfaces import Message
from eventsourcing.processor.at_least_once_processor import (
    AtLeastOnceProcessor,
)
from eventsourcing.processor.batch_processor import BatchOutboxProcessor
from eventsourcing.pubsub.in_memory import InMemoryPubSub
from eventsourcing.router import Router
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox
from eventsourcing.store.read_model.in_memory import InMemoryReadModelStore


@pytest.fixture
def broker():
    return InMemoryPubSub()


@pytest.fixture
def pubsub(broker):
    return broker


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
def stop_event():
    return asyncio.Event()


@pytest.fixture
def es_config(broker):
    return ESConfig(
        publisher=broker,
        subscriber=broker,
        dead_stream="dead",
        polling_interval=0.01,
    )


@pytest.fixture
def aloproc(event_store, outbox, broker, stop_event, es_config):
    return AtLeastOnceProcessor(
        es=event_store,
        outbox=outbox,
        publisher=broker,
        dead_stream=es_config.dead_stream,
        polling_interval=es_config.polling_interval,
        stop_event=stop_event,
    )


@pytest.fixture
def batchproc(event_store, outbox, broker, stop_event, es_config):
    return BatchOutboxProcessor(
        es=event_store,
        outbox=outbox,
        publisher=broker,
        dead_stream=es_config.dead_stream,
        polling_interval=es_config.polling_interval,
        stop_event=stop_event,
    )


# alias the 'processor' fixture for legacy tests
@pytest.fixture
def processor(aloproc):
    return aloproc


@pytest.fixture
def router(broker):
    return Router(broker)


@pytest.fixture
def make_message():
    def _make(name="Test", payload=None, stream="s"):
        return Message(name=name, payload=payload or {}, stream=stream)

    return _make


# FastAPI app fixture for the integration test
@pytest.fixture
def fastapi_app(broker, event_store, outbox, read_model):
    from eventsourcing.middleware import dedupe_middleware
    from eventsourcing.middleware import logging_middleware
    from eventsourcing.middleware import metrics_middleware
    from eventsourcing.middleware import retry_middleware

    router = Router(broker)
    router.add_middleware(logging_middleware)
    router.add_middleware(retry_middleware)
    router.add_middleware(metrics_middleware)
    router.add_middleware(dedupe_middleware)

    class ItemProjection:
        def __init__(self):
            self.calls = 0

        async def handle(self, msg: Message):
            self.calls += 1
            if self.calls == 1:
                # first failure to trigger retry
                raise RuntimeError("temporary failure")
            items = await read_model.load("items") or []
            items.append(msg.payload)
            await read_model.save("items", items)
            return []

    router.add_route("items.events", ItemProjection())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # subscribe to dead and events streams
        await broker.subscribe("dead")
        await broker.subscribe("items.events")
        stop = asyncio.Event()
        cfg = ESConfig(
            publisher=broker,
            subscriber=broker,
            dead_stream="dead",
            polling_interval=0.01,
        )
        proc = AtLeastOnceProcessor(
            es=event_store,
            outbox=outbox,
            publisher=broker,
            dead_stream=cfg.dead_stream,
            polling_interval=cfg.polling_interval,
            stop_event=stop,
        )
        # start router + processor
        tr = asyncio.create_task(router.run())
        tp = asyncio.create_task(proc.run())
        yield
        # shutdown
        proc.stop_event.set()
        await tp
        tr.cancel()

    app = FastAPI(lifespan=lifespan)

    async def get_outbox():
        return outbox

    @app.post("/items/")
    async def create_item(cmd: dict, ob: InMemoryOutbox = Depends(get_outbox)):
        msg = Message(
            name="ItemCreated",
            payload=cmd,
            stream="items.events",
            correlation_id=str(uuid.uuid4()),
        )
        await ob.enqueue([msg])
        return {"accepted": True}

    @app.get("/items/")
    async def list_items():
        return {"items": await read_model.load("items") or []}

    return app


@pytest.fixture(autouse=True)
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
