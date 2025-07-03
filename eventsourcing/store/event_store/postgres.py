import json
import logging
from typing import List
from typing import Optional

import asyncpg

from eventsourcing.interfaces import Message
from eventsourcing.store.event_store.base import EventStore

logger = logging.getLogger(__name__)


class PostgresEventStore(EventStore):
    def __init__(
        self,
        dsn: str,
        connection: asyncpg.Connection | None = None,
    ):
        self.dsn = dsn
        self._external_connection = connection
        self._initialized = False

    async def _get_connection(self) -> asyncpg.Connection:
        return self._external_connection or await asyncpg.connect(self.dsn)

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        logger.info("Ensuring database initialization")
        conn = await self._get_connection()
        logger.debug("Connection established: %s", conn)
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                payload JSONB NOT NULL,
                stream TEXT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                correlation_id UUID,
                causation_id UUID,
                version INT NOT NULL
            )
            """
        )
        logger.info("Table 'events' ensured")
        self._initialized = True

    async def append_to_stream(
        self, msgs: List[Message], expected_version: Optional[int] = None
    ) -> None:
        await self._ensure_initialized()
        conn = await self._get_connection()
        logger.info("Appending %d messages to stream", len(msgs))
        async with conn.transaction():
            for msg in msgs:
                logger.debug("Appending message: %s", msg)
                await conn.execute(
                    """
                    INSERT INTO events (id, name, payload, stream, timestamp, correlation_id, causation_id, version)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    msg.id,
                    msg.name,
                    json.dumps(
                        msg.payload.to_dict()
                    ),  # Ensure payload is serialized using to_dict
                    msg.stream,
                    msg.timestamp,
                    msg.correlation_id,
                    msg.causation_id,
                    msg.version,
                )
        logger.info("Successfully appended messages to stream")

    async def read_stream(
        self, stream: str, from_version: int = 0
    ) -> List[Message]:
        await self._ensure_initialized()
        conn = await self._get_connection()
        rows = await conn.fetch(
            """
            SELECT id, name, payload, stream, timestamp, correlation_id, causation_id, version
            FROM events
            WHERE stream = $1 AND version >= $2
            ORDER BY version ASC
            """,
            stream,
            from_version,
        )
        return [
            Message(
                id=row["id"],
                name=row["name"],
                payload=row["payload"],
                stream=row["stream"],
                timestamp=row["timestamp"],
                correlation_id=row["correlation_id"],
                causation_id=row["causation_id"],
                version=row["version"],
            )
            for row in rows
        ]
