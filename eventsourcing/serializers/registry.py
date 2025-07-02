from typing import Dict

from .base import Serializer
from .json_serializer import JSONSerializer


class SerializerRegistry:
    """
    A registry of serializers, keyed by content-type.
    Falls back to JSON if the requested content-type isn't registered.
    """

    def __init__(self) -> None:
        self._map: Dict[str, Serializer] = {}

    def supported_types(self) -> list[str]:
        return list(self._map)

    def is_registered(self, content_type: str) -> bool:
        return content_type.split(";", 1)[0] in self._map

    def register(self, content_type: str, serializer: Serializer) -> None:
        self._map[content_type] = serializer

    def get(self, content_type: str) -> Serializer:
        # strip any charset params etc.
        key = content_type.split(";", 1)[0].strip()
        return self._map.get(key, self._map["application/json"])


registry = SerializerRegistry()
# always have JSON available
registry.register("application/json", JSONSerializer())
