import asyncio
from typing import Dict

from eventsourcing.config import DEFAULT_STREAM
from eventsourcing.log_config import logger
from eventsourcing.processor.base import OutboxProcessorProtocol
from eventsourcing.pubsub.base import Publisher
from eventsourcing.store.event_store.base import EventStore
from eventsourcing.store.outbox.base import Outbox


class AtLeastOnceProcessor(OutboxProcessorProtocol):
    """
    Polls the outbox, and on failure:
      - append fails → re-enqueue batch for retry
      - publish fails → route batch to dead_stream
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
        logger.info("AtLeastOnceProcessor starting")
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

                # 1) Try append
                try:
                    await self.es.append_to_stream(
                        batch, expected_version=expected
                    )
                    self._last_version[stream] = expected + len(batch)
                    logger.info("Appended %d msgs to '%s'", len(batch), stream)
                except Exception:
                    logger.exception(
                        "Error appending batch to '%s', re-enqueueing", stream
                    )
                    await self.outbox.enqueue(batch)
                    await asyncio.sleep(self.polling_interval)
                    continue

                # 2) Try publish to main stream
                try:
                    await self.publisher.publish(stream, *batch)
                    logger.info(
                        "Delivered %d msgs to '%s'", len(batch), stream
                    )
                except Exception:
                    logger.exception(
                        "Error publishing batch on '%s', routing to '%s'",
                        stream,
                        self.dead_stream,
                    )
                    # publish to dead stream using the unpatched method
                    try:
                        original_publish = type(self.publisher).publish
                        await original_publish(
                            self.publisher, self.dead_stream, *batch
                        )
                        logger.info(
                            "Rerouted %d msgs to dead stream '%s'",
                            len(batch),
                            self.dead_stream,
                        )
                    except Exception:
                        logger.exception(
                            "Error routing batch to dead stream '%s'",
                            self.dead_stream,
                        )
        finally:
            logger.info("AtLeastOnceProcessor stopped")
