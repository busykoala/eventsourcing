import asyncio
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

from eventsourcing.interfaces import Message
from eventsourcing.log_config import logger
from eventsourcing.pubsub.base import Publisher
from eventsourcing.pubsub.base import Subscriber


class EvictList(list[Message]):
    """
    List that evicts oldest elements when exceeding a limit.
    """

    def __init__(self, limit: int) -> None:
        super().__init__()
        self.limit: int = limit

    def append(self, item: Message) -> None:
        super().append(item)
        if len(self) > self.limit:
            del self[0]

    def extend(self, items: Iterable[Message]) -> None:
        for item in items:
            self.append(item)


class InMemoryPubSub(Publisher, Subscriber):
    """
    In-memory Pub/Sub. Buffers messages if no subscribers,
    with a configurable backlog limit and automatic eviction.
    When someone subscribes, the backlog for that stream is
    atomically popped and delivered, so it's cleared for the next subscriber.
    """

    def __init__(self, backlog_limit: int = 1000) -> None:
        self._streams: Dict[str, List[asyncio.Queue[Optional[Message]]]] = {}
        self._backlog_limit = backlog_limit
        self._backlog: Dict[str, EvictList] = {}
        self._lock = asyncio.Lock()

    async def publish(self, stream: str, *msgs: Message) -> None:
        async with self._lock:
            queues = self._streams.get(stream, [])
            if not queues:
                buf = self._backlog.setdefault(
                    stream, EvictList(self._backlog_limit)
                )
                buf.extend(msgs)
                logger.debug("Buffered %d msgs on '%s'", len(msgs), stream)
                return
            for q in queues:
                for m in msgs:
                    await q.put(m)
            logger.info("Published %d msgs to '%s'", len(msgs), stream)

    async def subscribe(self, stream: str) -> asyncio.Queue[Optional[Message]]:
        # Atomically pop & flush the backlog on first subscription
        async with self._lock:
            q: asyncio.Queue[Optional[Message]] = asyncio.Queue()
            self._streams.setdefault(stream, []).append(q)
            pending = self._backlog.pop(stream, EvictList(self._backlog_limit))
            for m in pending:
                q.put_nowait(m)
        logger.info(
            "Subscriber added for '%s', flushed %d buffered msgs",
            stream,
            len(pending),
        )
        return q

    async def close(self) -> None:
        async with self._lock:
            for queues in self._streams.values():
                for q in queues:
                    q.put_nowait(None)
            self._streams.clear()
        logger.info("Closed all subscriber queues")
