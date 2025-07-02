# eventsourcing

A lightweight, extensible Python library for building event-sourced applications.  
Provides **pluggable** interfaces (via `Protocol`) for Pub/Sub, Event Store, Outbox, and Read-Model stores—swap out any component by implementing the right interface. Includes built-in in-memory implementations, middleware (logging, retry, dedupe, metrics), a router, and an outbox processor. Integrates with FastAPI.

## Single Working Example

```python
import asyncio
import uuid

import uvicorn
from fastapi import Depends
from fastapi import FastAPI
from fastapi import Header
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from google.protobuf.json_format import MessageToDict
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
# 0) Register built-in serializers at startup
#    - JSON as the default wire format
#    - Avro with a simple schema
#    - Protobuf using google.protobuf.Struct as an example
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
# 1) Instantiate core components
#    - broker: in-memory Pub/Sub
#    - event_store: append-only store
#    - outbox: durable outbox queue
#    - read_store: projection-backed read model
# -----------------------------------------------------------------------------
broker = InMemoryPubSub(backlog_limit=1000)
event_store = InMemoryEventStore()
outbox = InMemoryOutbox()
read_store = InMemoryReadModelStore()

# -----------------------------------------------------------------------------
# 2) Build the Router and apply middleware
#    a) deserialization_middleware: bytes → Python object
#    b) logging, retry, metrics, dedupe (built-in utilities)
# -----------------------------------------------------------------------------
router = Router(broker)
router.add_middleware(deserialization_middleware)  # deserialize first
router.add_middleware(logging_middleware)  # logs before/after handlers
router.add_middleware(retry_middleware)  # retries on failure
router.add_middleware(metrics_middleware)  # measures handler latency
router.add_middleware(dedupe_middleware)  # prevents duplicate processing


# -----------------------------------------------------------------------------
# 3) Define a projection handler (CQRS read side)
#    - consumes ItemCreated events
#    - updates an in-memory list under key "items"
# -----------------------------------------------------------------------------
class ItemProjection(HandlerProtocol):
  async def handle(self, msg: Message) -> list[Message]:
    # load existing items (or start empty)
    items = await read_store.load("items") or []
    # append the decoded payload (dict or Struct)
    items.append(msg.payload)
    # save updated list back to read-model
    await read_store.save("items", items)
    # no further commands/events → return empty list
    return []


# route the "items.events" stream to our projection
router.add_route("items.events", ItemProjection())

# -----------------------------------------------------------------------------
# 4) Configure logging format (text vs JSON)
# -----------------------------------------------------------------------------
cfg = ESConfig(
  publisher=broker,
  subscriber=broker,
  dead_stream="dead",
  polling_interval=0.1,
  json_logging=False,  # set True for JSON-structured logs
)
configure_logging(cfg.json_logging)

# -----------------------------------------------------------------------------
# 5) Wire up the OutboxProcessor
#    - serializes payloads → bytes (using msg.headers["content-type"])
#    - appends to event store, publishes via broker
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
# 6) Create FastAPI app with shared lifespan
#    - on startup: subscribe router + start router & processor loops
#    - on shutdown: signal stop_event, close queues, await cleanup
# -----------------------------------------------------------------------------
app = FastAPI(
  lifespan=lifespan_manager(
    router,
    processor,
    streams=["dead", "items.events"],  # router subscribes here
  )
)


# -----------------------------------------------------------------------------
# 7) Dependency injection for the outbox
# -----------------------------------------------------------------------------
async def get_outbox() -> InMemoryOutbox:
  return outbox


# -----------------------------------------------------------------------------
# 8) Command endpoint: POST /items/
#    - accepts JSON body by default, but you can override via:
#        • Content-Type header
#        • ?format=<mime-type> query parameter
#    - enqueues an ItemCreated message with the chosen content-type
# -----------------------------------------------------------------------------
@app.post("/items/")
async def create_item(
        request: Request,
        ob: InMemoryOutbox = Depends(get_outbox),
        content_type: str = Header("application/json", alias="Content-Type"),
        format: str | None = Query(None, description="override MIME type"),
) -> dict:
  """
  Command endpoint: enqueues an ItemCreated message in whatever wire format
  the client asked for (JSON, Avro or Protobuf).  The payload is left
  as raw bytes or a Python dict, tagged by headers["content-type"], and
  the OutboxProcessor + Router will do the actual serialize/deserialize.
  """
  # Pick final MIME type (query param beats header)
  ctype = format or content_type

  if ctype not in registry.is_registered(ctype):
    raise HTTPException(400, f"Unsupported format: {ctype}")

  # If JSON, let FastAPI decode for us; otherwise grab raw bytes
  if ctype == "application/json":
    payload = await request.json()
  else:
    payload = await request.body()  # raw bytes

  # Build the domain message with the correct header
  msg = Message(
    name="ItemCreated",
    payload=payload,
    stream="items.events",
    correlation_id=str(uuid.uuid4()),
  )
  msg.headers["content-type"] = ctype

  # Enqueue for reliable delivery
  await ob.enqueue([msg])
  return {"accepted": True}


# -----------------------------------------------------------------------------
# 9) Query endpoint: GET /items/
#    - convert any protobuf Struct back to dict so FastAPI can JSON it
# -----------------------------------------------------------------------------
@app.get("/items/")
async def list_items() -> dict:
  raw = await read_store.load("items") or []
  out: list[dict] = []
  for entry in raw:
    if isinstance(entry, Struct):
      # turn protobuf Struct into plain dict
      out.append(MessageToDict(entry))
    else:
      out.append(entry)
  return {"items": out}


# -----------------------------------------------------------------------------
# 10) Run via Uvicorn if invoked as a script
# -----------------------------------------------------------------------------
if __name__ == "__main__":
  uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
```

## Development

- **Run tests**  
  ```bash
  uv run poe test
  ```

- **Format & lint**  
  ```bash
  uv run poe format
  ```

- **Type-check**  
  ```bash
  uv run poe types
  ```

- **Run example app**  
  ```bash
  uv run poe dev
  ```

## Contributing

1. Fork & clone
2. Create a feature branch
3. Implement your own `Protocol`-compliant component in `pubsub/`, `store/`, or `middleware/`
4. Add tests under `tests/`
5. Open a pull request  
