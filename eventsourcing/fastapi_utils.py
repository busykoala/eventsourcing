import asyncio
from contextlib import asynccontextmanager
from typing import Any
from typing import AsyncContextManager
from typing import AsyncGenerator
from typing import Callable
from typing import List

from fastapi import FastAPI

from eventsourcing.processor.base import OutboxProcessorProtocol
from eventsourcing.router import Router


def lifespan_manager(
    router: Router,
    processor: OutboxProcessorProtocol,
    streams: List[str],
) -> Callable[[FastAPI], AsyncContextManager[None]]:
    """
    Returns a FastAPI lifespan function that:
      1) Shares the same stop_event between router and processor
      2) Subscribes to provided streams on startup
      3) Starts router.run() and processor.run() as background tasks
      4) On shutdown, signals stop_event and cleans up
    """

    @asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncGenerator[None, Any]:
        # share stop_event if available
        if hasattr(processor, "stop_event"):
            router.stop_event = processor.stop_event

        # subscribe once to each stream
        for s in streams:
            await router._subscriber.subscribe(s)

        t_router = asyncio.create_task(router.run())
        t_proc = asyncio.create_task(processor.run())
        try:
            yield
        finally:
            if hasattr(processor, "stop_event"):
                processor.stop_event.set()
            await router.shutdown()
            await t_proc
            t_router.cancel()

    return _lifespan
