import datetime
import uuid
from datetime import timezone
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Protocol

from pydantic import BaseModel
from pydantic import Field


class Message(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    payload: Dict[str, Any]
    headers: Dict[str, str] = {}
    timestamp: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(timezone.utc)
    )
    version: int = 0
    stream: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


class HandlerProtocol(Protocol):
    async def handle(self, message: Message) -> List[Message]: ...


Middleware = Callable[
    [Message, Callable[[Message], Awaitable[List[Message]]]],
    Awaitable[List[Message]],
]
