import pytest
from google.protobuf.struct_pb2 import Struct

from eventsourcing.serializers.avro_serializer import AvroSerializer
from eventsourcing.serializers.json_serializer import JSONSerializer
from eventsourcing.serializers.protobuf_serializer import ProtobufSerializer
from eventsourcing.serializers.registry import registry


@pytest.fixture(autouse=True)
def setup_registry():
    # Reset registry and register JSON as the default
    registry._map.clear()
    registry.register("application/json", JSONSerializer())
    yield
    registry._map.clear()


def test_registry_fallback_to_json():
    # unknown content type → JSONSerializer
    ser = registry.get("application/does-not-exist")
    assert isinstance(ser, JSONSerializer)


def test_json_serializer_roundtrip():
    obj = {"a": 1, "b": "two", "c": [3, 4, 5]}
    ser = registry.get("application/json")
    data = ser.serialize(obj)
    assert isinstance(data, bytes)
    out = ser.deserialize(data)
    assert out == obj


def test_avro_serializer_roundtrip():
    # a simple record schema
    schema = {
        "name": "TestRecord",
        "type": "record",
        "fields": [
            {"name": "x", "type": "int"},
            {"name": "y", "type": "string"},
        ],
    }
    avro_ser = AvroSerializer(schema)
    registry.register("application/avro", avro_ser)

    obj = {"x": 42, "y": "hello"}
    ser = registry.get("application/avro")
    data = ser.serialize(obj)
    assert isinstance(data, bytes)

    out = ser.deserialize(data)
    assert out == obj


def test_protobuf_serializer_roundtrip():
    # use Struct as a stand-in protobuf message
    proto_ser = ProtobufSerializer(Struct)
    registry.register("application/x-protobuf", proto_ser)

    s = Struct()
    s.update({"foo": "bar", "num": 7})
    ser = registry.get("application/x-protobuf")

    data = ser.serialize(s)
    assert isinstance(data, bytes)

    out = ser.deserialize(data)
    # Struct supports == comparison on fields
    assert out == s
