import asyncio
from typing import Dict

from eventsourcing.config import DEFAULT_STREAM
from eventsourcing.log_config import logger
from eventsourcing.processor.base import OutboxProcessorProtocol
from eventsourcing.pubsub.base import Publisher
from eventsourcing.store.event_store.base import EventStore
from eventsourcing.store.outbox.base import Outbox


class BatchOutboxProcessor(OutboxProcessorProtocol):
    """
    Polls the outbox at a fixed interval, batching messages per stream.
    On any error, routes the batch to a dead-letter stream.
    """

    def __init__(
        self,
        es: EventStore,
        outbox: Outbox,
        publisher: Publisher,
        dead_stream: str = "dead",
        polling_interval: float = 0.1,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self.es = es
        self.outbox = outbox
        self.publisher = publisher
        self.dead_stream = dead_stream
        self.polling_interval = polling_interval
        self.stop_event = stop_event or asyncio.Event()
        self._last_version: Dict[str, int] = {}

    async def run(self) -> None:
        logger.info("BatchOutboxProcessor starting")
        try:
            while True:
                batch = await self.outbox.dequeue()
                if not batch:
                    if self.stop_event.is_set():
                        break
                    await asyncio.sleep(self.polling_interval)
                    continue

                stream = batch[0].stream or DEFAULT_STREAM
                expected = self._last_version.get(stream, 0)

                try:
                    await self.es.append_to_stream(
                        batch, expected_version=expected
                    )
                    await self.publisher.publish(stream, *batch)
                    self._last_version[stream] = expected + len(batch)
                    logger.info(
                        "Forwarded batch of %d to '%s'", len(batch), stream
                    )
                except Exception:
                    logger.exception("Error forwarding batch on '%s'", stream)
                    await self.publisher.publish(self.dead_stream, *batch)
        finally:
            logger.info("BatchOutboxProcessor stopped")
