import asyncio
from typing import Dict

from eventsourcing.config import DEFAULT_STREAM
from eventsourcing.config import ESConfig
from eventsourcing.log_config import root_logger as logger
from eventsourcing.pubsub.base import Publisher
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox


class OutboxProcessor:
    """
    Continuously processes messages from the outbox, appends them
    to the event store, and publishes them via pub/sub, with clean shutdown.
    """

    def __init__(
        self,
        es: InMemoryEventStore,
        outbox: InMemoryOutbox,
        publisher: Publisher,
        config: ESConfig,
        stop_event: asyncio.Event,
    ) -> None:
        self.es = es
        self.outbox = outbox
        self.publisher = publisher
        self.dead_stream = config.dead_stream
        self.polling_interval = config.polling_interval
        self.stop_event = stop_event
        # Cache the last appended version per stream
        self._last_version: Dict[str, int] = {}

    async def run(self) -> None:
        logger.info("OutboxProcessor starting")
        try:
            while True:
                batch = await self.outbox.dequeue()
                if not batch:
                    # If no messages and shutdown requested, exit
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
                    await self.outbox.mark_failed(
                        batch, Exception("forward error")
                    )
                await asyncio.sleep(0)
        finally:
            logger.info("OutboxProcessor stopped")
