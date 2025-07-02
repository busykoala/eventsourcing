import asyncio
from typing import List

from eventsourcing.interfaces import Message
from eventsourcing.log_config import logger
from eventsourcing.store.outbox.base import Outbox


class InMemoryOutbox(Outbox):
    """
    In-memory implementation of Outbox.
    Not persistent—intended for testing or simple demos.
    """

    def __init__(self) -> None:
        self._queue: List[Message] = []
        self._failed: List[Message] = []
        self._lock = asyncio.Lock()

    async def enqueue(self, msgs: List[Message]) -> None:
        async with self._lock:
            self._queue.extend(msgs)
        logger.info("Enqueued %d msgs to outbox", len(msgs))

    async def dequeue(self, batch_size: int = 50) -> List[Message]:
        async with self._lock:
            batch = self._queue[:batch_size]
            self._queue = self._queue[batch_size:]
        if batch:
            logger.debug("Dequeued %d msgs from outbox", len(batch))
        return batch

    async def mark_failed(self, msgs: List[Message], error: Exception) -> None:
        async with self._lock:
            self._failed.extend(msgs)
        logger.error("Marked %d msgs as failed: %s", len(msgs), error)
