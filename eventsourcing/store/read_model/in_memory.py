from typing import Any
from typing import Dict

from eventsourcing.log_config import root_logger as logger
from eventsourcing.store.read_model.base import ReadModelStore


class InMemoryReadModelStore(ReadModelStore):
    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}

    async def save(self, name: str, data: Any) -> None:
        self._data[name] = data
        logger.info("Saved read-model '%s'", name)

    async def load(self, name: str) -> Any:
        result = self._data.get(name)
        logger.debug("Loaded read-model '%s': %s", name, result)
        return result
