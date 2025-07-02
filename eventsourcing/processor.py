import asyncio

from eventsourcing import ESConfig
from eventsourcing.config import DEFAULT_STREAM
from eventsourcing.log_config import logger
from eventsourcing.pubsub.base import Publisher
from eventsourcing.serializers.registry import registry
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox


class OutboxProcessor:
    """
    Processes messages until stop_event + empty outbox, then exits.
    Serializes payloads to bytes based on msg.headers['content-type']
    before appending and publishing, and restores them on the consumer side.
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
                if not batch:
                    if self.stop_event.is_set():
                        break
                    await asyncio.sleep(self.polling_interval)
                    continue

                # 1) serialize each payload in-place, but skip if it's already bytes
                for msg in batch:
                    content_type = msg.headers.get(
                        "content-type", "application/json"
                    )
                    serializer = registry.get(content_type)

                    if not isinstance(msg.payload, (bytes, bytearray)):
                        msg.payload = serializer.serialize(msg.payload)

                    # record the content_type so the consumer can decode
                    msg.headers["content-type"] = content_type

                # 2) append to event store & publish
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
                    logger.exception("Error forwarding batch on '%s'", stream)
                    await self.publisher.publish(self.dead_stream, *batch)
                    await self.outbox.mark_failed(batch, e)
        finally:
            logger.info("OutboxProcessor stopped")
