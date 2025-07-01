import asyncio
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import List

from .interfaces import HandlerProtocol
from .interfaces import Message
from .interfaces import Middleware
from .log_config import root_logger as logger
from .pubsub.base import Subscriber


class Router:
    def __init__(self, subscriber: Subscriber):
        self._subscriber = subscriber
        self._routes: Dict[str, HandlerProtocol] = {}
        self._middleware: List[Middleware] = []

    def add_route(self, stream: str, handler: HandlerProtocol) -> None:
        self._routes[stream] = handler
        logger.debug("Route added for '%s'", stream)

    def add_middleware(self, mw: Middleware) -> None:
        self._middleware.append(mw)
        logger.debug("Middleware added: %s", getattr(mw, "__name__", repr(mw)))

    async def run(self) -> None:
        logger.info("Router starting")
        queues = {
            stream: await self._subscriber.subscribe(stream)
            for stream in self._routes
        }
        try:
            while True:
                for stream, q in queues.items():
                    msg: Message = await q.get()
                    if msg is None:
                        logger.info("Shutdown sentinel on '%s'", stream)
                        return

                    async def final(m: Message) -> list[Message]:
                        return await self._routes[stream].handle(m)

                    pipeline: Callable[[Message], Awaitable[List[Message]]] = (
                        final
                    )
                    for mw in reversed(self._middleware):
                        pipeline = self._wrap_middleware(mw, pipeline)

                    try:
                        await pipeline(msg)
                    except Exception:
                        logger.exception(
                            "Error processing msg %s on '%s'", msg.id, stream
                        )
                await asyncio.sleep(0)
        finally:
            logger.info("Router stopped")

    @staticmethod
    def _wrap_middleware(
        mw: Middleware,
        next_handler: Callable[[Message], Awaitable[List[Message]]],
    ) -> Callable[[Message], Awaitable[List[Message]]]:
        async def wrapped(msg: Message) -> List[Message]:
            return await mw(msg, next_handler)

        return wrapped
