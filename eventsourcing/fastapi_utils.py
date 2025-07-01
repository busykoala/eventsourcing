import asyncio
from contextlib import asynccontextmanager
from typing import AsyncContextManager
from typing import AsyncGenerator
from typing import Callable
from typing import List

from fastapi import FastAPI

from eventsourcing.processor import OutboxProcessor
from eventsourcing.router import Router


def lifespan_manager(
    router: Router,
    outbox_processor: OutboxProcessor,
    streams: List[str],
) -> Callable[[FastAPI], AsyncContextManager[None]]:
    """
    Returns a FastAPI lifespan function that:
    - Shares the same stop_event between router and processor
    - Subscribes to provided streams on startup
    - Starts router.run() and outbox_processor.run() as background tasks
    - On shutdown, signals stop_event, closes the subscriber, and waits for tasks
    """

    @asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        # Use the same stop_event for both router and processor
        router.stop_event = outbox_processor.stop_event

        # Subscribe to each stream before starting
        for s in streams:
            await router._subscriber.subscribe(s)

        # Launch both background loops
        t_router = asyncio.create_task(router.run())
        t_proc = asyncio.create_task(outbox_processor.run())

        try:
            yield
        finally:
            # Signal shutdown
            outbox_processor.stop_event.set()
            # Close the subscriber so router gets None sentinels
            await router._subscriber.close()
            # Await both loops to exit cleanly
            await t_router
            await t_proc

    return _lifespan
