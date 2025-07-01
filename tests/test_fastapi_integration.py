import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager

import pytest
from fastapi import Depends
from fastapi import FastAPI

from eventsourcing.config import ESConfig
from eventsourcing.interfaces import Message
from eventsourcing.middleware import dedupe_middleware
from eventsourcing.middleware import logging_middleware
from eventsourcing.middleware import metrics_middleware
from eventsourcing.middleware import retry_middleware
from eventsourcing.processor import OutboxProcessor
from eventsourcing.pubsub.in_memory import InMemoryPubSub
from eventsourcing.router import Router
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox
from eventsourcing.store.read_model.in_memory import InMemoryReadModelStore

logging.getLogger("uvicorn").setLevel(logging.WARNING)


@pytest.fixture
def fastapi_app():
    broker = InMemoryPubSub()
    event_store = InMemoryEventStore()
    outbox = InMemoryOutbox()
    projection = InMemoryReadModelStore()

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
                raise RuntimeError("temporary failure")
            items = await projection.load("items") or []
            items.append(msg.payload)
            await projection.save("items", items)
            return []

    handler = ItemProjection()
    router.add_route("items.events", handler)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # startup
        await broker.subscribe("dead")
        await broker.subscribe("items.events")

        stop = asyncio.Event()
        cfg = ESConfig(publisher=broker, subscriber=broker, dead_stream="dead")
        processor = OutboxProcessor(event_store, outbox, broker, cfg, stop)

        t1 = asyncio.create_task(router.run())
        t2 = asyncio.create_task(processor.run())
        yield
        # shutdown
        t1.cancel()
        t2.cancel()

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
        return {"items": await projection.load("items") or []}

    return app


def test_fastapi_roundtrip(fastapi_app, caplog):
    caplog.set_level(logging.DEBUG)
    from fastapi.testclient import TestClient

    with TestClient(fastapi_app) as client:
        resp = client.post("/items/", json={"foo": "bar"})
        assert resp.status_code == 200

        # Poll until the item appears or timeout
        start = time.time()
        items = []
        while time.time() - start < 2:
            resp = client.get("/items/")
            items = resp.json().get("items", [])
            if items:
                break
            time.sleep(0.1)

        assert items == [{"foo": "bar"}]

    assert any(
        "Retry 1/3 for ItemCreated" in rec.getMessage()
        for rec in caplog.records
    )
    assert any(
        "→ ItemCreated @ items.events" in rec.getMessage()
        for rec in caplog.records
    )
    assert any(
        "✓ ItemCreated handled, produced 0" in rec.getMessage()
        for rec in caplog.records
    )
    assert any(
        "METRICS ItemCreated" in rec.getMessage() for rec in caplog.records
    )
    assert any(
        "Enqueued 1 msgs to outbox" in rec.getMessage()
        for rec in caplog.records
    )
    assert any(
        "Forwarded batch of 1 to 'items.events'" in rec.getMessage()
        for rec in caplog.records
    )
