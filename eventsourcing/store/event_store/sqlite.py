import datetime
import json
from sqlite3 import Row
from typing import Any

import aiosqlite
from pydantic import BaseModel

from eventsourcing.config import DEFAULT_STREAM
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
        return self._external_connection or await aiosqlite.connect(
            self.db_path
        )

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

    async def append(self, stream: str, messages: list[Message[Any]]) -> None:
        await self._ensure_initialized()
        db = await self._get_connection()

        # Determine last version (None → 0)
        cur = await db.execute(
            "SELECT MAX(version) FROM events WHERE stream = ?", (stream,)
        )
        row: Row | None = await cur.fetchone()
        await cur.close()
        last_version = (row[0] or 0) if row else 0

        await db.execute("BEGIN")
        for msg in messages:
            last_version += 1
            msg.version = last_version

            raw = msg.payload
            # 1) raw bytes
            if isinstance(raw, (bytes, bytearray)):
                blob = raw
            # 2) raw string → prefix with \0
            elif isinstance(raw, str):
                blob = b"\0" + raw.encode("utf-8")
            # 3) Pydantic model
            elif isinstance(raw, BaseModel):
                data = raw.model_dump()
                # unwrap single-byte field
                if len(data) == 1 and isinstance(
                    next(iter(data.values())), (bytes, bytearray)
                ):
                    blob = next(iter(data.values()))
                else:
                    blob = json.dumps(data).encode("utf-8")
            # 4) dict or list → JSON
            elif isinstance(raw, (dict, list)):
                blob = json.dumps(raw).encode("utf-8")
            else:
                # fallback: JSON-serialize anything else
                blob = json.dumps(raw).encode("utf-8")

            await db.execute(
                """
                INSERT INTO events (
                    id, name, payload, stream, timestamp,
                    correlation_id, causation_id, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    msg.id,
                    msg.name,
                    blob,
                    stream,
                    msg.timestamp.isoformat(),
                    msg.correlation_id,
                    msg.causation_id,
                    msg.version,
                ),
            )
        await db.commit()

    async def read(self, stream: str) -> list[Message[Any]]:
        await self._ensure_initialized()
        db = await self._get_connection()
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM events WHERE stream = ? ORDER BY version ASC",
            (stream,),
        )
        rows = await cur.fetchall()
        await cur.close()

        out: list[Message[Any]] = []
        for row in rows:
            blob = row["payload"]
            # 1) prefixed string
            if isinstance(blob, (bytes, bytearray)) and blob.startswith(b"\0"):
                payload: Any = blob[1:].decode("utf-8")
            else:
                # 2) try JSON
                try:
                    text = blob.decode("utf-8")
                    payload = json.loads(text)
                except Exception:
                    # 3) fallback raw bytes
                    payload = blob

            out.append(
                Message[Any](
                    id=row["id"],
                    name=row["name"],
                    payload=payload,
                    stream=row["stream"],
                    timestamp=datetime.datetime.fromisoformat(
                        row["timestamp"]
                    ),
                    correlation_id=row["correlation_id"],
                    causation_id=row["causation_id"],
                    version=row["version"],
                )
            )
        return out

    async def append_to_stream(
        self, msgs: list[Message[Any]], expected_version: int | None = None
    ) -> None:
        if not msgs:
            return
        stream = msgs[0].stream or DEFAULT_STREAM
        await self.append(stream, msgs)

    async def read_stream(
        self, stream: str, from_version: int = 0
    ) -> list[Message[Any]]:
        evs = await self.read(stream)
        return [e for e in evs if e.version > from_version]
