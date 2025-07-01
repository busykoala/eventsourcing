from typing import Any

from pydantic import BaseModel

DEFAULT_STREAM = "_default"


class ESConfig(BaseModel):
    """
    Configuration for the event-sourcing system.
    """

    publisher: Any = None
    subscriber: Any = None
    namespace: str = "default"
    dead_stream: str = "dead"
    metrics_enabled: bool = False
    polling_interval: float = 0.1
    json_logging: bool = False

    model_config = {"arbitrary_types_allowed": True}
