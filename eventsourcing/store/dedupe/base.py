from typing import Protocol


class DedupeStore(Protocol):
    """Protocol for a dedupe store."""

    async def seen(self, msg_id: str) -> bool: ...
    async def mark(self, msg_id: str) -> None: ...
