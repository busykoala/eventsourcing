from typing import List
from typing import Protocol

from ...interfaces import Message


class Outbox(Protocol):
    async def enqueue(self, msgs: List[Message]) -> None: ...
    async def dequeue(self, batch_size: int = 50) -> List[Message]: ...
    async def mark_failed(
        self, msgs: List[Message], error: Exception
    ) -> None: ...
