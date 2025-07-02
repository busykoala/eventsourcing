from typing import Awaitable
from typing import Callable
from typing import List

from eventsourcing.interfaces import Message
from eventsourcing.log_config import logger


class CommandHandler:
    """
    Handles commands by producing a corresponding event
    and enqueueing it to the outbox.
    """

    def __init__(
        self, enqueue_outbox: Callable[[List[Message]], Awaitable[None]]
    ) -> None:
        self._enqueue = enqueue_outbox

    async def handle(self, cmd: Message) -> None:
        ev = Message(
            name=f"{cmd.name}Executed",
            payload=cmd.payload,
            stream=cmd.stream,
            correlation_id=cmd.correlation_id or cmd.id,
            causation_id=cmd.id,
        )
        await self._enqueue([ev])
        logger.info("Command %s executed → event %s", cmd.name, ev.name)
