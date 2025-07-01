import asyncio
from typing import Dict
from typing import List

from eventsourcing.interfaces import Message
from eventsourcing.log_config import root_logger as logger
from eventsourcing.pubsub.base import Publisher
from eventsourcing.pubsub.base import Subscriber


class InMemoryPubSub(Publisher, Subscriber):
    def __init__(self) -> None:
        self._streams: Dict[str, List[asyncio.Queue]] = {}
        self._backlog: Dict[str, List[Message]] = {}

    async def publish(self, stream: str, *msgs: Message) -> None:
        try:
            queues = self._streams.get(stream, [])
            if not queues:
                self._backlog.setdefault(stream, []).extend(msgs)
                logger.debug("Buffered %d msgs on '%s'", len(msgs), stream)
                return
            for q in queues:
                for m in msgs:
                    await q.put(m)
            logger.info("Published %d msgs to '%s'", len(msgs), stream)
        except Exception:
            logger.exception("Error publishing to '%s'", stream)
            raise

    async def subscribe(self, stream: str) -> asyncio.Queue:
        try:
            q: asyncio.Queue = asyncio.Queue()
            self._streams.setdefault(stream, []).append(q)
            pending = self._backlog.pop(stream, [])
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
