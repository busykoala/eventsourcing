import io
from builtins import memoryview
from typing import Any
from typing import Dict

from fastavro import parse_schema
from fastavro import schemaless_reader
from fastavro import schemaless_writer

from eventsourcing.serializers.base import Serializer


class AvroSerializer(Serializer):
    """
    Avro ↔ bytes serializer using a fastavro schema.
    """

    def __init__(self, schema_dict: Dict[str, Any]):
        # schema_dict should be a Python dict representing your Avro schema
        # (e.g. loaded from JSON).
        self._parsed_schema = parse_schema(schema_dict)

    def serialize(self, obj: Any) -> bytes:
        """
        Serialize `obj` (a dict matching the schema) to Avro binary.
        """
        buf = io.BytesIO()
        schemaless_writer(buf, self._parsed_schema, obj)
        return buf.getvalue()

    def deserialize(self, data: bytes) -> Any:
        """
        Deserialize Avro binary back into a Python dict.
        """
        # directly seed the buffer with a memoryview
        buf = io.BytesIO(memoryview(data))
        # fastavro schemaless_reader wants both writer_schema and reader_schema
        return schemaless_reader(buf, self._parsed_schema, self._parsed_schema)
