import json
from typing import Any

from eventsourcing.serializers.base import Serializer


class JSONSerializer(Serializer):
    """
    Simple JSON ↔ bytes serializer.
    """

    def serialize(self, obj: Any) -> bytes:
        # Uses utf-8 encoding
        return json.dumps(obj).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))
