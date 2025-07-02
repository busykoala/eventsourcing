from typing import Any

from eventsourcing.interfaces import Message
from eventsourcing.log_config import logger
from eventsourcing.store.read_model.base import ReadModelStore


class QueryHandler:
    """
    Handles query messages by loading data from the read-model store.
    """

    def __init__(self, read_model_store: ReadModelStore) -> None:
        self._store = read_model_store

    async def handle(self, qry: Message) -> Any:
        result = await self._store.load(qry.name)
        logger.info("Query %s returned %s", qry.name, result)
        return result
