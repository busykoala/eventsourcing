import datetime
import uuid
from datetime import timezone
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Generic
from typing import List
from typing import Optional
from typing import Protocol
from typing import Set
from typing import TypeVar

from pydantic import BaseModel
from pydantic import Field

# Type variable for the payload type
P = TypeVar("P")


class Message(BaseModel, Generic[P]):
    name: str
    payload: P
    stream: Optional[str] = None
    correlation_id: Optional[str] = None
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(timezone.utc)
    )
    version: int = 0
    causation_id: Optional[str] = None

    def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """Shallow dump to a dict (delegates to Pydantic)."""
        return super().model_dump(*args, **kwargs)

    @classmethod
    def model_construct(
        cls,
        _fields_set: Set[str] | None = None,
        **data: Any,
    ) -> "Message[P]":
        # Bypass validation by constructing directly
        return cls(**data)

    def __hash__(self) -> int:
        # Allow putting Message in sets/maps by its unique ID
        return hash(self.id)


# For handlers that work on any Message[P]
class HandlerProtocol(Protocol, Generic[P]):
    """Handles messages carrying payload of type P."""

    async def handle(self, message: Message[P]) -> List[Message[Any]]: ...


# Middleware signature stays the same but operates on the generic Message[Any]
Middleware = Callable[
    [Message[Any], Callable[[Message[Any]], Awaitable[List[Message[Any]]]]],
    Awaitable[List[Message[Any]]],
]
Messages = List[Message[Any]]
