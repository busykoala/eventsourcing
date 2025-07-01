import asyncio

from eventsourcing import ESConfig
from eventsourcing.config import DEFAULT_STREAM
from eventsourcing.log_config import root_logger as logger
from eventsourcing.pubsub.base import Publisher
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox


class OutboxProcessor:
    """
    Processes messages until stop_event + empty outbox, then exits.
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
        self._last_version: dict[str, int] = {}

    async def run(self) -> None:
        logger.info("OutboxProcessor starting")
        try:
            while True:
                batch = await self.outbox.dequeue()
                if batch:
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
                    except Exception as e:
                        logger.exception(
                            "Error forwarding batch on '%s'", stream
                        )
                        await self.publisher.publish(self.dead_stream, *batch)
                        await self.outbox.mark_failed(batch, e)
                else:
                    if self.stop_event.is_set():
                        # queue empty + stop requested → exit
                        break
                    await asyncio.sleep(self.polling_interval)
        finally:
            logger.info("OutboxProcessor stopped")
