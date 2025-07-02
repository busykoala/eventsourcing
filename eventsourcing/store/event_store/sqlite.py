import json
from typing import Any
from typing import cast

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

    async def _get_connection(self) -> aiosqlite.Connection:
        if self._external_connection:
            return self._external_connection
        return await aiosqlite.connect(self.db_path)

    async def _ensure_initialized(self) -> None:
        db = await self._get_connection()
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream TEXT NOT NULL,
                name TEXT NOT NULL,
                payload TEXT NOT NULL,
                headers TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.commit()

    async def append(self, stream: str, messages: list[Message]) -> None:
        await self._ensure_initialized()
        db = await self._get_connection()
        await db.execute("BEGIN")
        for msg in messages:
            await db.execute(
                """
                INSERT INTO events (stream, name, payload, headers)
                VALUES (?, ?, ?, ?)
                """,
                (
                    stream,
                    msg.name,
                    self._serialize(msg.payload),
                    self._serialize(msg.headers),
                ),
            )
        await db.commit()

    async def read(self, stream: str) -> list[Message]:
        await self._ensure_initialized()
        db = await self._get_connection()
        cursor = await db.execute(
            """
            SELECT name, payload, headers
            FROM events
            WHERE stream = ?
            ORDER BY id ASC
            """,
            (stream,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [
            Message(
                name=row[0],
                payload=self._deserialize(row[1]),
                headers=cast(dict[str, str], self._deserialize(row[2])),
            )
            for row in rows
        ]

    @staticmethod
    def _serialize(obj: object) -> str:
        return json.dumps(obj, separators=(",", ":"))

    @staticmethod
    def _deserialize(s: str) -> Any:
        return json.loads(s)
