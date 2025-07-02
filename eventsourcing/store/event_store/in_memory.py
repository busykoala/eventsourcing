import asyncio
from typing import Dict
from typing import List
from typing import Optional

from eventsourcing.config import DEFAULT_STREAM
from eventsourcing.interfaces import Message
from eventsourcing.log_config import logger
from eventsourcing.store.event_store.base import EventStore


class InMemoryEventStore(EventStore):
    """
    In-memory implementation of EventStore.
    Not persistent—intended for testing or simple demos.
    """

    def __init__(self) -> None:
        self._streams: Dict[str, List[Message]] = {}
        self._seen_ids: set[str] = set()
        self._lock = asyncio.Lock()

    async def append_to_stream(
        self, msgs: List[Message], expected_version: Optional[int] = None
    ) -> None:
        if not msgs:
            return
        async with self._lock:
            stream = msgs[0].stream or DEFAULT_STREAM
            seq = self._streams.setdefault(stream, [])
            if expected_version is not None and len(seq) != expected_version:
                msg = f"Version conflict on stream '{stream}'"
                logger.error(msg)
                raise RuntimeError(msg)
            appended = 0
            for m in msgs:
                if m.id in self._seen_ids:
                    continue
                m.version = len(seq) + 1
                seq.append(m)
                self._seen_ids.add(m.id)
                appended += 1
            logger.info("Appended %d msgs to '%s'", appended, stream)

    async def read_stream(
        self, stream: str, from_version: int = 0
    ) -> List[Message]:
        async with self._lock:
            result = [
                m
                for m in self._streams.get(stream, [])
                if m.version > from_version
            ]
        logger.debug(
            "Read %d msgs from '%s' (from_version=%d)",
            len(result),
            stream,
            from_version,
        )
        return result
