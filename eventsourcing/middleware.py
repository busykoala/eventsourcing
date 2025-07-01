import asyncio
import time
from typing import Awaitable
from typing import Callable

from eventsourcing.interfaces import Message
from eventsourcing.interfaces import Messages
from eventsourcing.log_config import root_logger as logger
from eventsourcing.store.dedupe.base import DedupeStore
from eventsourcing.store.dedupe.in_memory import InMemoryDedupeStore

# Default in-memory dedupe store
_default_store: DedupeStore = InMemoryDedupeStore()


async def logging_middleware(
    msg: Message, handler: Callable[[Message], Awaitable[Messages]]
) -> Messages:
    """Logs before and after handling each message."""
    logger.info("→ %s @ %s id=%s", msg.name, msg.stream, msg.id)
    out = await handler(msg)
    logger.info("✓ %s handled, produced %d", msg.name, len(out))
    return out


async def retry_middleware(
    msg: Message,
    handler: Callable[[Message], Awaitable[Messages]],
    retries: int = 3,
    backoff: float = 0.5,
) -> Messages:
    """
    Retries a failing handler up to `retries` times,
    with exponential backoff multiplier.
    """
    last_exc: BaseException = RuntimeError(f"Retries exhausted for {msg.name}")
    for attempt in range(1, retries + 1):
        try:
            return await handler(msg)
        except BaseException as e:
            last_exc = e
            logger.warning("Retry %d/%d for %s", attempt, retries, msg.name)
            await asyncio.sleep(backoff * attempt)
    logger.error("All retries failed for %s", msg.name)
    raise last_exc


async def dedupe_middleware(
    msg: Message, handler: Callable[[Message], Awaitable[Messages]]
) -> Messages:
    """
    Prevents the same message (by ID) from being handled twice,
    using the in-memory dedupe store.
    """
    if await _default_store.seen(msg.id):
        logger.debug("Deduped %s id=%s", msg.name, msg.id)
        return []
    out = await handler(msg)
    await _default_store.mark(msg.id)
    return out


async def metrics_middleware(
    msg: Message, handler: Callable[[Message], Awaitable[Messages]]
) -> Messages:
    """Measures and logs the time taken to handle each message."""
    start = time.time()
    out = await handler(msg)
    duration = time.time() - start
    logger.info("METRICS %s took %.3fs", msg.name, duration)
    return out
