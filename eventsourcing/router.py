import asyncio
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from eventsourcing.interfaces import HandlerProtocol
from eventsourcing.interfaces import Message
from eventsourcing.interfaces import Middleware
from eventsourcing.log_config import root_logger as logger
from eventsourcing.pubsub.base import Subscriber


class Router:
    """
    Routes messages from pub/sub to registered handlers,
    applying middleware in user-defined order, with clean shutdown.
    """

    def __init__(
        self,
        subscriber: Subscriber,
        stop_event: Optional[asyncio.Event] = None,
    ):
        self._subscriber = subscriber
        self.stop_event = stop_event or asyncio.Event()
        self._routes: Dict[str, HandlerProtocol] = {}
        self._middleware: List[Middleware] = []

    def add_route(self, stream: str, handler: HandlerProtocol) -> None:
        self._routes[stream] = handler
        logger.debug("Route added for '%s'", stream)

    def add_middleware(self, mw: Middleware) -> None:
        self._middleware.append(mw)
        logger.debug("Middleware added: %s", getattr(mw, "__name__", repr(mw)))

    def insert_middleware_before(
        self, existing: Middleware, new_mw: Middleware
    ) -> None:
        """
        Insert `new_mw` immediately before `existing` in the middleware pipeline.
        """
        idx = self._middleware.index(existing)
        self._middleware.insert(idx, new_mw)

    async def run(self) -> None:
        logger.info("Router starting")
        queues: Dict[str, asyncio.Queue[Optional[Message]]] = {
            stream: await self._subscriber.subscribe(stream)
            for stream in self._routes
        }
        try:
            while not self.stop_event.is_set():
                for stream, q in queues.items():
                    msg: Optional[Message] = await q.get()
                    if msg is None:
                        logger.info("Shutdown sentinel on '%s'", stream)
                        return

                    async def final(m: Message) -> list[Message]:
                        return await self._routes[stream].handle(m)

                    pipeline: Callable[[Message], Awaitable[list[Message]]] = (
                        final
                    )
                    for mw in reversed(self._middleware):
                        pipeline = self._wrap_middleware(mw, pipeline)

                    await pipeline(msg)
                await asyncio.sleep(0)
        finally:
            logger.info("Router stopped")

    @staticmethod
    def _wrap_middleware(
        mw: Middleware,
        next_handler: Callable[[Message], Awaitable[list[Message]]],
    ) -> Callable[[Message], Awaitable[list[Message]]]:
        async def wrapped(msg: Message) -> list[Message]:
            return await mw(msg, next_handler)

        return wrapped
