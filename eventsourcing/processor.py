import asyncio

from .log_config import root_logger as logger
from .pubsub.base import Publisher
from .store.event_store.in_memory import InMemoryEventStore
from .store.outbox.in_memory import InMemoryOutbox


class OutboxProcessor:
    def __init__(
        self,
        es: InMemoryEventStore,
        outbox: InMemoryOutbox,
        publisher: Publisher,
        dead_stream: str = "dead",
    ):
        self.es = es
        self.outbox = outbox
        self.publisher = publisher
        self.dead_stream = dead_stream

    async def run(self) -> None:
        logger.info("OutboxProcessor starting")
        try:
            while True:
                batch = await self.outbox.dequeue()
                if not batch:
                    await asyncio.sleep(0.1)
                    continue
                stream = batch[0].stream or "_default"
                logger.debug(
                    "Processing batch of %d on '%s'", len(batch), stream
                )
                existing = await self.es.read_stream(stream)
                try:
                    await self.es.append_to_stream(
                        batch, expected_version=len(existing)
                    )
                    await self.publisher.publish(stream, *batch)
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
