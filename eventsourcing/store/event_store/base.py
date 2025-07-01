from typing import List
from typing import Optional
from typing import Protocol

from ...interfaces import Message


class EventStore(Protocol):
    async def append_to_stream(
        self, msgs: List[Message], expected_version: Optional[int] = None
    ) -> None: ...
    async def read_stream(
        self, stream: str, from_version: int = 0
    ) -> List[Message]: ...
