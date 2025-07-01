from collections import OrderedDict

from eventsourcing.store.dedupe.base import DedupeStore


class InMemoryDedupeStore(DedupeStore):
    """
    In-memory dedupe store with max-size eviction.
    """

    def __init__(self, max_size: int = 10000) -> None:
        self._seen: OrderedDict = OrderedDict()
        self._max_size = max_size

    async def seen(self, msg_id: str) -> bool:
        return msg_id in self._seen

    async def mark(self, msg_id: str) -> None:
        if msg_id in self._seen:
            # Move to end if re-seen
            self._seen.move_to_end(msg_id)
        else:
            self._seen[msg_id] = None
        # Evict oldest if over limit
        while len(self._seen) > self._max_size:
            self._seen.popitem(last=False)
