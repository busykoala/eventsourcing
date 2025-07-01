import asyncio
from typing import Optional
from typing import Protocol

from eventsourcing.interfaces import Message


class Publisher(Protocol):
    """Protocol for a Pub/Sub publisher."""

    async def publish(self, stream: str, *msgs: Message) -> None: ...


class Subscriber(Protocol):
    """Protocol for a Pub/Sub subscriber."""

    async def subscribe(
        self, stream: str
    ) -> asyncio.Queue[Optional[Message]]: ...
    async def close(self) -> None: ...
