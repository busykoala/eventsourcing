import asyncio
import uuid
from typing import Any
from typing import AsyncGenerator

import uvicorn
from fastapi import Body
from fastapi import Depends
from fastapi import FastAPI
from fastapi import Query

from eventsourcing.config import ESConfig
from eventsourcing.fastapi_utils import lifespan_manager
from eventsourcing.interfaces import Message
from eventsourcing.log_config import configure_logging
from eventsourcing.middleware import dedupe_middleware
from eventsourcing.middleware import logging_middleware
from eventsourcing.middleware import metrics_middleware
from eventsourcing.middleware import retry_middleware
from eventsourcing.processor.at_least_once_processor import (
    AtLeastOnceProcessor,
)
from eventsourcing.pubsub.in_memory import InMemoryPubSub
from eventsourcing.router import Router
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox
from eventsourcing.store.read_model.in_memory import InMemoryReadModelStore

# Core components
broker = InMemoryPubSub(backlog_limit=1000)
event_store = InMemoryEventStore()
outbox = InMemoryOutbox()
read_store = InMemoryReadModelStore()

# Router + middleware
router = Router(broker)
router.add_middleware(logging_middleware)
router.add_middleware(retry_middleware)
router.add_middleware(metrics_middleware)
router.add_middleware(dedupe_middleware)


# Projection
class ItemProjection:
    async def handle(self, msg: Message) -> list[Message]:
        items = await read_store.load("items") or []
        items.append(msg.payload)
        await read_store.save("items", items)
        return []


router.add_route("items.events", ItemProjection())

# Logging config
cfg = ESConfig(
    publisher=broker,
    subscriber=broker,
    dead_stream="dead",
    polling_interval=0.1,
    json_logging=False,
)
configure_logging(cfg.json_logging)

# Processor (at-least-once)
stop_event = asyncio.Event()
processor = AtLeastOnceProcessor(
    es=event_store,
    outbox=outbox,
    publisher=broker,
    polling_interval=cfg.polling_interval,
    stop_event=stop_event,
)

# FastAPI app
app = FastAPI(
    lifespan=lifespan_manager(
        router, processor, streams=["dead", "items.events"]
    )
)


# Outbox dependency
async def get_outbox() -> AsyncGenerator[InMemoryOutbox, None]:
    yield outbox


@app.post("/items/")
async def create_item(
    cmd: Any = Body(..., description="Command payload"),
    ob: InMemoryOutbox = Depends(get_outbox),
    name: str = Query(..., description="Event name"),
    stream: str = Query("items.events", description="Event stream"),
) -> dict:
    msg = Message(
        name=name,
        payload=cmd,
        stream=stream,
        correlation_id=str(uuid.uuid4()),
    )
    await ob.enqueue([msg])
    return {"accepted": True}


@app.get("/items/")
async def list_items() -> dict:
    items = await read_store.load("items") or []
    return {"items": items}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
