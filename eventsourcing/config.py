from typing import Any

from pydantic import BaseModel


class ESConfig(BaseModel):
    publisher: Any
    subscriber: Any
    namespace: str = "default"
    dead_stream: str = "dead"
    metrics_enabled: bool = False

    model_config = {"arbitrary_types_allowed": True}
