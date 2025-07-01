from typing import Any
from typing import Protocol

from .interfaces import Message
from .log_config import root_logger as logger


class ReadModelStore(Protocol):
    async def load(self, name: str) -> Any: ...


class QueryHandler:
    def __init__(self, read_model_store: ReadModelStore) -> None:
        self._store: ReadModelStore = read_model_store

    async def handle(self, qry: Message) -> Any:
        try:
            result = await self._store.load(qry.name)
            logger.info("Query %s returned %s", qry.name, result)
            return result
        except Exception:
            logger.exception("Error in QueryHandler.handle for %s", qry.name)
            raise
