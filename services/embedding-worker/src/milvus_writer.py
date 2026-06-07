import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


class MilvusWriter:
    """Writes embedding rows to Milvus, creating the collection + IVF_FLAT index if absent.

    Uses chunk_id as the VARCHAR primary key so upsert() is truly idempotent:
    re-delivering the same chunk overwrites the existing row rather than creating a duplicate.
    """

    def __init__(
        self,
        host: str,
        port: int,
        collection: str,
        dim: int,
        uri: Optional[str] = None,
    ):
        self._host = host
        self._port = port
        self._collection_name = collection
        self._dim = dim
        self._uri = uri  # If set, overrides host/port (supports Milvus Lite file path)
        self._col = None

    def connect(self) -> None:
        from pymilvus import connections
        if self._uri:
            connections.connect(alias="default", uri=self._uri)
        else:
            connections.connect(alias="default", host=self._host, port=str(self._port))
        self._ensure_collection()

    def _ensure_collection(self) -> None:
        from pymilvus import Collection, CollectionSchema, FieldSchema, DataType, utility

        if utility.has_collection(self._collection_name):
            self._col = Collection(self._collection_name)
            self._col.load()
            return

        fields = [
            # chunk_id is the primary key so upsert() deduplicates by chunk identity.
            FieldSchema("chunk_id", DataType.VARCHAR, max_length=256, is_primary=True),
            FieldSchema("doc_id", DataType.VARCHAR, max_length=256),
            FieldSchema("source_type", DataType.VARCHAR, max_length=64),
            FieldSchema("text", DataType.VARCHAR, max_length=65535),
            FieldSchema("embedding", DataType.FLOAT_VECTOR, dim=self._dim),
            FieldSchema("created_at", DataType.INT64),
            FieldSchema("metadata", DataType.JSON),
            FieldSchema("tenant_id", DataType.VARCHAR, max_length=256),
        ]
        schema = CollectionSchema(fields, description="Pipeline document chunks")
        self._col = Collection(self._collection_name, schema=schema)
        self._col.create_index(
            "embedding",
            {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}},
        )
        self._col.load()

    def upsert(self, rows: List[dict]) -> int:
        """Upsert rows by chunk_id; returns number of rows written."""
        if not rows:
            return 0
        data = [
            [r["chunk_id"] for r in rows],
            [r["doc_id"] for r in rows],
            [r["source_type"] for r in rows],
            [r["text"] for r in rows],
            [r["embedding"] for r in rows],
            [r["created_at"] for r in rows],
            [r["metadata"] for r in rows],
            [r.get("tenant_id", "") for r in rows],
        ]
        mr = self._col.upsert(data)
        self._col.flush()
        return mr.upsert_count

    def query(self, expr: str, output_fields: Optional[List[str]] = None) -> List[dict]:
        """Query entities by expression; used in tests to verify writes."""
        if self._col is None:
            return []
        return self._col.query(expr=expr, output_fields=output_fields or [])

    def count(self) -> int:
        """Return number of entities in the collection."""
        if self._col is None:
            return 0
        return self._col.num_entities

    def close(self) -> None:
        try:
            from pymilvus import connections
            connections.disconnect("default")
        except Exception:
            pass
