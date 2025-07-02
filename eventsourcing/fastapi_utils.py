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
      1) Shares the same stop_event between router and processor
      2) Subscribes to provided streams on startup
      3) Starts router.run() and outbox_processor.run() as background tasks
      4) On shutdown, invokes router.shutdown() and waits for the processor
    """

    @asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        # share the same stop_event
        router.stop_event = outbox_processor.stop_event

        # subscribe before starting loops
        await router.subscribe_to_streams(streams)

        _ = asyncio.create_task(router.run())
        t_proc = asyncio.create_task(outbox_processor.run())
        try:
            yield
        finally:
            # clean shutdown of router + then processor
            await router.shutdown()
            await t_proc

    return _lifespan
