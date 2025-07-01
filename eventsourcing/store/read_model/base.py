from typing import Any
from typing import Protocol


class ReadModelStore(Protocol):
    """Protocol for a Read-Model store."""

    async def save(self, name: str, data: Any) -> None: ...
    async def load(self, name: str) -> Any: ...
