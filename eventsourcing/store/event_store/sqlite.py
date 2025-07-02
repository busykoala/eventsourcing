import datetime

import aiosqlite

from eventsourcing.interfaces import Message
from eventsourcing.store.event_store.base import EventStore


class SQLiteEventStore(EventStore):
    def __init__(
        self,
        db_path: str = "events.db",
        connection: aiosqlite.Connection | None = None,
    ):
        self.db_path = db_path
        self._external_connection = connection
        self._initialized = False

    async def _get_connection(self) -> aiosqlite.Connection:
        if self._external_connection:
            return self._external_connection
        return await aiosqlite.connect(self.db_path)

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        db = await self._get_connection()
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                payload BLOB NOT NULL,
                stream TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                correlation_id TEXT,
                causation_id TEXT,
                version INTEGER NOT NULL
            )
            """
        )
        await db.commit()
        self._initialized = True

    async def append(self, stream: str, messages: list[Message]) -> None:
        await self._ensure_initialized()
        db = await self._get_connection()
        await db.execute("BEGIN")
        for msg in messages:
            await db.execute(
                """
                INSERT INTO events (
                    id, name, payload, stream, timestamp,
                    correlation_id, causation_id, version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    msg.id,
                    msg.name,
                    msg.payload,
                    stream,
                    msg.timestamp.isoformat(),
                    msg.correlation_id,
                    msg.causation_id,
                    msg.version,
                ),
            )
        await db.commit()

    async def read(self, stream: str) -> list[Message]:
        await self._ensure_initialized()
        db = await self._get_connection()
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT *
            FROM events
            WHERE stream = ?
            ORDER BY version ASC
            """,
            (stream,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [
            Message(
                id=row["id"],
                name=row["name"],
                payload=row["payload"],
                stream=row["stream"],
                timestamp=datetime.datetime.fromisoformat(row["timestamp"]),
                correlation_id=row["correlation_id"],
                causation_id=row["causation_id"],
                version=row["version"],
            )
            for row in rows
        ]
