from typing import Any
from typing import Protocol


class ReadModelStore(Protocol):
    async def save(self, name: str, data: Any) -> None: ...
    async def load(self, name: str) -> Any: ...
