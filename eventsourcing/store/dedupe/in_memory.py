from eventsourcing.store.dedupe.base import DedupeStore


class InMemoryDedupeStore(DedupeStore):
    """
    In-memory dedupe store. Uses a Python set for seen IDs.
    """

    def __init__(self) -> None:
        self._seen: set[str] = set()

    async def seen(self, msg_id: str) -> bool:
        return msg_id in self._seen

    async def mark(self, msg_id: str) -> None:
        self._seen.add(msg_id)
