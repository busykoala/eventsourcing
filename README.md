# eventsourcing

A lightweight, extensible Python library for building event-sourced applications.  
Provides **pluggable** interfaces (via `Protocol`) for Pub/Sub, Event Store, Outbox, and Read-Model stores—swap out any component by implementing the right interface. Includes built-in in-memory implementations, middleware (logging, retry, dedupe, metrics), a router, and an outbox processor. Integrates with FastAPI.

## Single Working Example

```python
import asyncio
import uuid
from typing import Any
from typing import AsyncGenerator

import uvicorn
from fastapi import Depends
from fastapi import FastAPI
from fastapi import Query
from fastapi import Body

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
