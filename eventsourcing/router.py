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
        self._routes: Dict[str, HandlerProtocol[Message]] = {}
        self._middleware: List[Middleware] = []

    def add_route(
        self, stream: str, handler: HandlerProtocol[Message]
    ) -> None:
        self._routes[stream] = handler
        logger.debug("Route added for '%s'", stream)

    def add_middleware(self, mw: Middleware) -> None:
        self._middleware.append(mw)
        logger.debug("Middleware added: %s", getattr(mw, "__name__", repr(mw)))

    async def subscribe_to_streams(self, streams: List[str]) -> None:
        """
        Public helper to subscribe to multiple streams.
        """
        for s in streams:
            await self._subscriber.subscribe(s)

    async def run(self) -> None:
        logger.info("Router starting")
        # subscribe to exactly the configured routes
        queues = {
            stream: await self._subscriber.subscribe(stream)
            for stream in self._routes
        }
        tasks = [
            asyncio.create_task(self._consume_stream(stream, queue))
            for stream, queue in queues.items()
        ]
        await asyncio.wait(tasks)
        logger.info("Router stopped")

    async def _consume_stream(
        self, stream: str, queue: asyncio.Queue[Optional[Message]]
    ) -> None:
        while not self.stop_event.is_set():
            msg = await queue.get()
            if msg is None:
                logger.info("Shutdown sentinel on '%s'", stream)
                return
            await self._dispatch(stream, msg)

    async def _dispatch(self, stream: str, msg: Message) -> None:
        async def final(m: Message) -> List[Message]:
            return await self._routes[stream].handle(m)

        pipeline: Callable[[Message], Awaitable[List[Message]]] = final
        for mw in reversed(self._middleware):
            pipeline = self._wrap_middleware(mw, pipeline)
        await pipeline(msg)

    @staticmethod
    def _wrap_middleware(
        mw: Middleware,
        next_handler: Callable[[Message], Awaitable[List[Message]]],
    ) -> Callable[[Message], Awaitable[List[Message]]]:
        async def wrapped(msg: Message) -> List[Message]:
            return await mw(msg, next_handler)

        return wrapped
