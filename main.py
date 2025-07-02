import asyncio
import uuid
from typing import Any

import uvicorn
from fastapi import Depends
from fastapi import FastAPI
from fastapi import Header
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message as PBMessage
from google.protobuf.struct_pb2 import Struct

from eventsourcing.config import ESConfig
from eventsourcing.fastapi_utils import lifespan_manager
from eventsourcing.interfaces import HandlerProtocol
from eventsourcing.interfaces import Message
from eventsourcing.log_config import configure_logging
from eventsourcing.middleware import dedupe_middleware
from eventsourcing.middleware import deserialization_middleware
from eventsourcing.middleware import logging_middleware
from eventsourcing.middleware import metrics_middleware
from eventsourcing.middleware import retry_middleware
from eventsourcing.processor import OutboxProcessor
from eventsourcing.pubsub.in_memory import InMemoryPubSub
from eventsourcing.router import Router
from eventsourcing.serializers.avro_serializer import AvroSerializer
from eventsourcing.serializers.json_serializer import JSONSerializer
from eventsourcing.serializers.protobuf_serializer import ProtobufSerializer
from eventsourcing.serializers.registry import registry
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox
from eventsourcing.store.read_model.in_memory import InMemoryReadModelStore

# -----------------------------------------------------------------------------
# 0) Register built-in serializers
# -----------------------------------------------------------------------------
registry.register("application/json", JSONSerializer())
_avro_schema = {
    "name": "Item",
    "type": "record",
    "fields": [
        {"name": "foo", "type": "string"},
        {"name": "n", "type": "int"},
    ],
}
registry.register("application/avro", AvroSerializer(_avro_schema))
registry.register("application/x-protobuf", ProtobufSerializer(Struct))

# -----------------------------------------------------------------------------
# 1) Core components
# -----------------------------------------------------------------------------
broker = InMemoryPubSub(backlog_limit=1000)
event_store = InMemoryEventStore()
outbox = InMemoryOutbox()
read_store = InMemoryReadModelStore()

# -----------------------------------------------------------------------------
# 2) Router + middleware
# -----------------------------------------------------------------------------
router = Router(broker)
router.add_middleware(deserialization_middleware)
router.add_middleware(logging_middleware)
router.add_middleware(retry_middleware)
router.add_middleware(metrics_middleware)
router.add_middleware(dedupe_middleware)


# -----------------------------------------------------------------------------
# 3) Projection (CQRS read side)
# -----------------------------------------------------------------------------
class ItemProjection(HandlerProtocol):
    async def handle(self, msg: Message) -> list[Message]:
        items = await read_store.load("items") or []
        items.append(msg.payload)
        await read_store.save("items", items)
        return []


router.add_route("items.events", ItemProjection())

# -----------------------------------------------------------------------------
# 4) Logging configuration
# -----------------------------------------------------------------------------
cfg = ESConfig(
    publisher=broker,
    subscriber=broker,
    dead_stream="dead",
    polling_interval=0.1,
    json_logging=False,
)
configure_logging(cfg.json_logging)

# -----------------------------------------------------------------------------
# 5) OutboxProcessor
# -----------------------------------------------------------------------------
stop_event = asyncio.Event()
processor = OutboxProcessor(
    es=event_store,
    outbox=outbox,
    publisher=broker,
    config=cfg,
    stop_event=stop_event,
)

# -----------------------------------------------------------------------------
# 6) FastAPI app + shared lifespan
# -----------------------------------------------------------------------------
app = FastAPI(
    lifespan=lifespan_manager(
        router, processor, streams=["dead", "items.events"]
    )
)


# -----------------------------------------------------------------------------
# 7) Dependency for the outbox
# -----------------------------------------------------------------------------
async def get_outbox() -> InMemoryOutbox:
    return outbox


# -----------------------------------------------------------------------------
# Helper: generic protobuf → JSON
# -----------------------------------------------------------------------------
def to_jsonable(obj: Any) -> Any:
    if isinstance(obj, PBMessage):
        return MessageToDict(obj)
    return obj


# -----------------------------------------------------------------------------
# 8) Command endpoint
# -----------------------------------------------------------------------------
@app.post("/items/")
async def create_item(
    request: Request,
    ob: InMemoryOutbox = Depends(get_outbox),
    content_type: str = Header("application/json", alias="Content-Type"),
    format: str | None = Query(None, description="override MIME type"),
) -> dict:
    ctype = format or content_type
    if not registry.is_registered(ctype):
        raise HTTPException(400, f"Unsupported format: {ctype}")

    if ctype == "application/json":
        payload = await request.json()
    else:
        payload = await request.body()

    msg = Message(
        name="ItemCreated",
        payload=payload,
        stream="items.events",
        correlation_id=str(uuid.uuid4()),
    )
    msg.headers["content-type"] = ctype
    await ob.enqueue([msg])
    return {"accepted": True}


# -----------------------------------------------------------------------------
# 9) Query endpoint
# -----------------------------------------------------------------------------
@app.get("/items/")
async def list_items() -> dict:
    raw = await read_store.load("items") or []
    return {"items": [to_jsonable(e) for e in raw]}


# -----------------------------------------------------------------------------
# 10) Uvicorn entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
