from typing import Protocol


class OutboxProcessorProtocol(Protocol):
    """Protocol for outbox processors with a run loop."""

    async def run(self) -> None: ...
