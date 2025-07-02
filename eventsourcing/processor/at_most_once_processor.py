import asyncio

from eventsourcing.config import DEFAULT_STREAM
from eventsourcing.log_config import logger
from eventsourcing.processor.base import OutboxProcessorProtocol
from eventsourcing.pubsub.base import Publisher
from eventsourcing.store.event_store.base import EventStore
from eventsourcing.store.outbox.base import Outbox


class AtMostOnceProcessor(OutboxProcessorProtocol):
    """
    Polls the outbox and drops any batch that fails,
    guaranteeing at-most-once delivery.
    """

    def __init__(
        self,
        es: EventStore,
        outbox: Outbox,
        publisher: Publisher,
        polling_interval: float = 0.1,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self.es = es
        self.outbox = outbox
        self.publisher = publisher
        self.polling_interval = polling_interval
        self.stop_event = stop_event or asyncio.Event()

    async def run(self) -> None:
        logger.info("AtMostOnceProcessor starting")
        try:
            while True:
                batch = await self.outbox.dequeue()
                if not batch:
                    if self.stop_event.is_set():
                        break
                    await asyncio.sleep(self.polling_interval)
                    continue

                stream = batch[0].stream or DEFAULT_STREAM

                try:
                    await self.es.append_to_stream(batch)
                    await self.publisher.publish(stream, *batch)
                    logger.info(
                        "Delivered %d msgs to '%s'", len(batch), stream
                    )
                except Exception:
                    logger.error(
                        "Failed to deliver %d msgs to '%s', dropping",
                        len(batch),
                        stream,
                    )
                    # drop on failure
        finally:
            logger.info("AtMostOnceProcessor stopped")
