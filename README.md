# eventsourcing

A lightweight, extensible Python library for building event-sourced applications.  
Provides **pluggable** interfaces (via `Protocol`) for Pub/Sub, Event Store, Outbox, and Read-Model stores—swap out any component by implementing the right interface. Includes built-in in-memory implementations, middleware (logging, retry, dedupe, metrics), a router, and an outbox processor. Integrates with FastAPI.

## Single Working Example

```python
import asyncio
import uuid
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient

# 1. Core imports from eventsourcing
from eventsourcing.interfaces import Message
from eventsourcing.interfaces import HandlerProtocol
from eventsourcing.pubsub.in_memory import InMemoryPubSub  # implements Publisher & Subscriber
from eventsourcing.store.event_store.in_memory import InMemoryEventStore  # implements EventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox  # implements Outbox
from eventsourcing.store.read_model.in_memory import InMemoryReadModelStore  # implements ReadModelStore
from eventsourcing.router import Router
from eventsourcing.processor import OutboxProcessor
from eventsourcing.middleware import (
    logging_middleware,  # logs before/after
    retry_middleware,  # retries on error
    metrics_middleware,  # measures timing
    dedupe_middleware,  # prevents duplicates
)

# 2. Instantiate your components (all interchangeable)
broker      = InMemoryPubSub()
event_store = InMemoryEventStore()
outbox      = InMemoryOutbox()
read_store  = InMemoryReadModelStore()

# 3. Build router and apply middleware in desired order
router = Router(broker)
router.add_middleware(logging_middleware)
router.add_middleware(retry_middleware)
router.add_middleware(metrics_middleware)
router.add_middleware(dedupe_middleware)

# 4. Projection handler (updates read model)
class ItemProjection(HandlerProtocol):
    async def handle(self, msg: Message):
        items = await read_store.load("items") or []
        items.append(msg.payload)
        await read_store.save("items", items)
        return []

router.add_route("items.events", ItemProjection())

# 5. FastAPI app with lifespan manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup: subscribe streams and launch background tasks
    await broker.subscribe("dead")
    await broker.subscribe("items.events")
    t1 = asyncio.create_task(router.run())
    t2 = asyncio.create_task(
        OutboxProcessor(
            event_store,
            outbox,
            broker,
            dead_stream="dead"
        ).run()
    )
    yield
    # shutdown: cancel background tasks
    t1.cancel()
    t2.cancel()

app = FastAPI(lifespan=lifespan)

# 6. Dependency to inject the outbox
async def get_outbox() -> InMemoryOutbox:
    return outbox

# 7. Command endpoint: enqueue an ItemCreated event
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

# 8. Query endpoint: return current read model
@app.get("/items/")
async def list_items():
    return {"items": await read_store.load("items") or []}

# 9. Example TestClient usage
if __name__ == "__main__":
    client = TestClient(app)
    client.post("/items/", json={"foo": "bar"})
    time.sleep(0.2)  # let background tasks run
    print(client.get("/items/").json())
# => {'items': [{'foo': 'bar'}]}
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
