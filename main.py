import asyncio
import uuid

import uvicorn
from fastapi import Depends
from fastapi import FastAPI

from eventsourcing.config import ESConfig
from eventsourcing.fastapi_utils import lifespan_manager
from eventsourcing.interfaces import HandlerProtocol
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

# 1. Instantiate components
broker = InMemoryPubSub(backlog_limit=1000)
event_store = InMemoryEventStore()
outbox = InMemoryOutbox()
read_store = InMemoryReadModelStore()

# 2. Build router and apply middleware
router = Router(broker)
router.add_middleware(logging_middleware)
router.add_middleware(retry_middleware)
router.add_middleware(metrics_middleware)
router.add_middleware(dedupe_middleware)


# 3. Projection handler
class ItemProjection(HandlerProtocol):
    async def handle(self, msg: Message) -> list:
        items = await read_store.load("items") or []
        items.append(msg.payload)
        await read_store.save("items", items)
        return []


router.add_route("items.events", ItemProjection())

# 4. Configure OutboxProcessor
stop_event = asyncio.Event()
cfg = ESConfig(
    publisher=broker,
    subscriber=broker,
    dead_stream="dead",
    polling_interval=0.1,
)
processor = OutboxProcessor(
    event_store,
    outbox,
    broker,
    config=cfg,
    stop_event=stop_event,
)

# 5. Create FastAPI app with lifespan_manager
app = FastAPI(
    lifespan=lifespan_manager(
        router,
        processor,
        streams=["dead", "items.events"],
    )
)


# 6. Dependency injection
async def get_outbox() -> InMemoryOutbox:
    return outbox


# 7. Command endpoint
@app.post("/items/")
async def create_item(
    cmd: dict, ob: InMemoryOutbox = Depends(get_outbox)
) -> dict:
    msg = Message(
        name="ItemCreated",
        payload=cmd,
        stream="items.events",
        correlation_id=str(uuid.uuid4()),
    )
    await ob.enqueue([msg])
    return {"accepted": True}


# 8. Query endpoint
@app.get("/items/")
async def list_items() -> dict:
    return {"items": await read_store.load("items") or []}


# 9. Run uvicorn if main
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
