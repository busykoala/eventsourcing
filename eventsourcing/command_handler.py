from typing import Awaitable
from typing import Callable
from typing import List

from .interfaces import Message
from .log_config import root_logger as logger


class CommandHandler:
    def __init__(
        self, enqueue_outbox: Callable[[List[Message]], Awaitable[None]]
    ) -> None:
        self._enqueue: Callable[[List[Message]], Awaitable[None]] = (
            enqueue_outbox
        )

    async def handle(self, cmd: Message) -> None:
        try:
            ev = Message(
                name=f"{cmd.name}Executed",
                payload=cmd.payload,
                stream=cmd.stream,
                correlation_id=cmd.correlation_id or cmd.id,
                causation_id=cmd.id,
            )
            await self._enqueue([ev])
            logger.info("Command %s executed → event %s", cmd.name, ev.name)
        except Exception:
            logger.exception("Error in CommandHandler.handle for %s", cmd.name)
            raise
