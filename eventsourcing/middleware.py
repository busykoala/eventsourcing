import asyncio
import time
from typing import Awaitable
from typing import Callable
from typing import List

from .interfaces import Message
from .log_config import root_logger as logger


async def logging_middleware(
    msg: Message, handler: Callable[[Message], Awaitable[List[Message]]]
) -> List[Message]:
    logger.info("→ %s @ %s id=%s", msg.name, msg.stream, msg.id)
    try:
        out = await handler(msg)
        logger.info("✓ %s handled, produced %d", msg.name, len(out))
        return out
    except Exception:
        logger.exception("✗ Exception in handling %s", msg.name)
        raise


async def retry_middleware(
    msg: Message,
    handler: Callable[[Message], Awaitable[List[Message]]],
    retries: int = 3,
    backoff: float = 0.5,
) -> List[Message]:
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
    msg: Message, handler: Callable[[Message], Awaitable[List[Message]]]
) -> List[Message]:
    key = f"handled:{msg.id}"
    if msg.headers.get(key):
        logger.debug("Deduped %s id=%s", msg.name, msg.id)
        return []
    out = await handler(msg)
    msg.headers[key] = "1"
    return out


async def metrics_middleware(
    msg: Message, handler: Callable[[Message], Awaitable[List[Message]]]
) -> List[Message]:
    start = time.time()
    out = await handler(msg)
    duration = time.time() - start
    logger.info("METRICS %s took %.3fs", msg.name, duration)
    return out
