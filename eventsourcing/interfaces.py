import datetime
import uuid
from dataclasses import dataclass
from dataclasses import field
from datetime import timezone
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Protocol
from typing import TypeVar

M = TypeVar("M", bound="Message")


@dataclass
class Message:
    name: str
    payload: Any
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    headers: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(timezone.utc)
    )
    version: int = 0
    stream: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None

    def model_dump(self) -> Dict[str, Any]:
        """
        Shallow dump to a dict (faster than asdict deep-copy).
        """
        return self.__dict__

    @classmethod
    def model_construct(cls, **data: Any) -> "Message":
        return cls(**data)


class HandlerProtocol(Protocol[M]):
    """Handles messages of type M, returns a list of M."""

    async def handle(self, message: M) -> List[M]: ...


# Middleware takes a Message + next handler, returns a list of Message
Middleware = Callable[
    [Message, Callable[[Message], Awaitable[List[Message]]]],
    Awaitable[List[Message]],
]

# Alias
Messages = List[Message]
