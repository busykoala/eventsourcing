from typing import Type

from google.protobuf.message import Message as PBMessage

from eventsourcing.serializers.base import Serializer


class ProtobufSerializer(Serializer):
    """
    Protobuf ↔ bytes serializer.
    You must pass in the generated protobuf Message class to the constructor.
    """

    def __init__(self, message_type: Type[PBMessage]):
        self._message_type = message_type

    def serialize(self, obj: PBMessage) -> bytes:
        if not isinstance(obj, self._message_type):
            raise TypeError(f"Expected {self._message_type}, got {type(obj)}")
        return obj.SerializeToString()

    def deserialize(self, data: bytes) -> PBMessage:
        msg = self._message_type()
        msg.ParseFromString(data)
        return msg
