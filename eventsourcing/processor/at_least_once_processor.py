import asyncio
from itertools import groupby
from typing import Dict
from typing import List

from eventsourcing.config import DEFAULT_STREAM
from eventsourcing.interfaces import Message
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
                batch: List[Message] = await self.outbox.dequeue()
                if not batch:
                    if self.stop_event.is_set():
                        break
                    await asyncio.sleep(self.polling_interval)
                    continue

                # Group messages by their target stream
                batch.sort(key=lambda m: m.stream or DEFAULT_STREAM)
                for stream, msgs_iter in groupby(
                    batch, key=lambda m: m.stream or DEFAULT_STREAM
                ):
                    msgs = list(msgs_iter)
                    expected = self._last_version.get(stream, 0)

                    # 1) Try append sub-batch to its stream
                    try:
                        await self.es.append_to_stream(
                            msgs, expected_version=expected
                        )
                        self._last_version[stream] = expected + len(msgs)
                        logger.info(
                            "Appended %d msgs to '%s'", len(msgs), stream
                        )
                    except Exception:
                        logger.exception(
                            "Error appending batch to '%s', re-enqueueing",
                            stream,
                        )
                        # if append failed, put the entire original batch back
                        await self.outbox.enqueue(batch)
                        await asyncio.sleep(self.polling_interval)
                        break  # skip publishing this stream, retry later
                    else:
                        # 2) Try publish to main stream
                        try:
                            await self.publisher.publish(stream, *msgs)
                            logger.info(
                                "Delivered %d msgs to '%s'", len(msgs), stream
                            )
                        except Exception:
                            logger.exception(
                                "Error publishing batch on '%s', routing to '%s'",
                                stream,
                                self.dead_stream,
                            )
                            # Reroute to dead stream
                            try:
                                original_publish = type(self.publisher).publish
                                await original_publish(
                                    self.publisher, self.dead_stream, *msgs
                                )
                                logger.info(
                                    "Rerouted %d msgs to dead stream '%s'",
                                    len(msgs),
                                    self.dead_stream,
                                )
                            except Exception:
                                logger.exception(
                                    "Error routing batch to dead stream '%s'",
                                    self.dead_stream,
                                )
        finally:
            logger.info("AtLeastOnceProcessor stopped")
