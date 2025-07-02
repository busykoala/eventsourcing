from typing import Any
from typing import Dict
from typing import Protocol
from typing import runtime_checkable


@runtime_checkable
class Serializer(Protocol):
    """
    Serializer protocol: must implement serialize() and deserialize().
    """

    def serialize(self, obj: Any) -> bytes:
        """
        Convert a Python object into bytes.
        """
        ...

    def deserialize(self, data: bytes) -> Any:
        """
        Convert bytes back into a Python object.
        """
        ...


# internal registry of serializers by content-type
_serializers: Dict[str, Serializer] = {}


def register_serializer(content_type: str, serializer: Serializer) -> None:
    """
    Register a Serializer under the given MIME type.
    """
    _serializers[content_type] = serializer


def get_serializer(content_type: str) -> Serializer:
    """
    Lookup a Serializer by MIME type; fall back to JSON.
    """
    return _serializers.get(content_type, _serializers["application/json"])
