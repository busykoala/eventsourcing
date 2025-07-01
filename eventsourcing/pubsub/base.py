import asyncio
from typing import Protocol

from ..interfaces import Message


class Publisher(Protocol):
    async def publish(self, stream: str, *msgs: Message) -> None: ...
    async def close(self) -> None: ...


class Subscriber(Protocol):
    async def subscribe(self, stream: str) -> asyncio.Queue[Message]: ...
    async def close(self) -> None: ...
