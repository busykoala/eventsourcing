import asyncio
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

from eventsourcing.interfaces import Message
from eventsourcing.log_config import root_logger as logger
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
    In-memory Pub/Sub implementation. Buffers messages if no subscribers,
    with a configurable backlog limit and automatic eviction.
    """

    def __init__(self, backlog_limit: int = 1000) -> None:
        self._streams: Dict[str, List[asyncio.Queue[Optional[Message]]]] = {}
        self._backlog_limit = backlog_limit
        self._backlog: Dict[str, EvictList] = {}

    async def publish(self, stream: str, *msgs: Message) -> None:
        try:
            queues = self._streams.get(stream, [])
            if not queues:
                buf = self._backlog.get(stream)
                if buf is None:
                    buf = EvictList(self._backlog_limit)
                    self._backlog[stream] = buf
                buf.extend(msgs)
                logger.debug("Buffered %d msgs on '%s'", len(msgs), stream)
                return
            for q in queues:
                for m in msgs:
                    await q.put(m)
            logger.info("Published %d msgs to '%s'", len(msgs), stream)
        except Exception:
            logger.exception("Error publishing to '%s'", stream)
            raise

    async def subscribe(self, stream: str) -> asyncio.Queue[Optional[Message]]:
        try:
            q: asyncio.Queue[Optional[Message]] = asyncio.Queue()
            self._streams.setdefault(stream, []).append(q)
            pending = self._backlog.pop(stream, EvictList(self._backlog_limit))
            for m in pending:
                await q.put(m)
            logger.info(
                "Subscriber added for '%s', flushed %d buffered msgs",
                stream,
                len(pending),
            )
            return q
        except Exception:
            logger.exception("Error subscribing to '%s'", stream)
            raise

    async def close(self) -> None:
        for queues in self._streams.values():
            for q in queues:
                q.put_nowait(None)
        logger.info("Closed all subscriber queues")
