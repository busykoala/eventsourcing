import asyncio

import pytest
from google.protobuf.struct_pb2 import Struct

from eventsourcing.config import ESConfig
from eventsourcing.interfaces import Message
from eventsourcing.middleware import deserialization_middleware
from eventsourcing.processor import OutboxProcessor
from eventsourcing.pubsub.in_memory import InMemoryPubSub
from eventsourcing.router import Router
from eventsourcing.serializers.avro_serializer import AvroSerializer
from eventsourcing.serializers.json_serializer import JSONSerializer
from eventsourcing.serializers.protobuf_serializer import ProtobufSerializer
from eventsourcing.serializers.registry import registry
from eventsourcing.store.event_store.in_memory import InMemoryEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox


@pytest.fixture(autouse=True)
def setup_registry():
    # start fresh
    registry._map.clear()
    # register JSON
    registry.register("application/json", JSONSerializer())
    # register Avro under our simple schema
    schema = {
        "name": "TestRecord",
        "type": "record",
        "fields": [
            {"name": "x", "type": "int"},
            {"name": "y", "type": "string"},
        ],
    }
    registry.register("application/avro", AvroSerializer(schema))
    # register protobuf using Struct as stand-in
    registry.register("application/x-protobuf", ProtobufSerializer(Struct))


async def _roundtrip(content_type, payload):
    pubsub = InMemoryPubSub()
    es = InMemoryEventStore()
    outbox = InMemoryOutbox()
    stop = asyncio.Event()
    cfg = ESConfig(publisher=pubsub, subscriber=pubsub, dead_stream="dead")
    proc = OutboxProcessor(es, outbox, pubsub, cfg, stop)

    # build a router that first deserializes then calls our handler
    router = Router(pubsub)
    router.add_middleware(deserialization_middleware)

    class H:
        def __init__(self):
            self.seen = []

        async def handle(self, msg: Message):
            self.seen.append(msg.payload)
            return []

    handler = H()
    router.add_route("test.stream", handler)

    # start the router, give it a moment to subscribe
    t_router = asyncio.create_task(router.run())
    await asyncio.sleep(0.01)

    # now start the outbox processor
    t_proc = asyncio.create_task(proc.run())

    # enqueue our test message
    msg = Message(name="Evt", payload=payload, stream="test.stream")
    msg.headers["content-type"] = content_type
    await outbox.enqueue([msg])

    # wait end-to-end
    await asyncio.sleep(0.2)
    stop.set()
    await t_proc
    await pubsub.close()
    await t_router

    assert handler.seen, "no message arrived"
    return handler.seen[0]


@pytest.mark.asyncio
async def test_json_pipeline():
    obj = {"foo": "bar", "n": 123}
    assert await _roundtrip("application/json", obj) == obj


@pytest.mark.asyncio
async def test_avro_pipeline():
    obj = {"x": 42, "y": "hello"}
    assert await _roundtrip("application/avro", obj) == obj


@pytest.mark.asyncio
async def test_protobuf_pipeline():
    s = Struct()
    s.update({"a": 1, "b": "two"})
    assert await _roundtrip("application/x-protobuf", s) == s
