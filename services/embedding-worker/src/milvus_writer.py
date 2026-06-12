import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class MilvusWriter:
    """Writes embedding rows to Milvus, creating the collection + IVF_FLAT index if absent.

    Uses chunk_id as the VARCHAR primary key so upsert() is truly idempotent:
    re-delivering the same chunk overwrites the existing row rather than creating a duplicate.

    Supports per-tenant collections: pass collection='{tenant_id}_docs' to upsert() to route
    rows to the correct tenant-scoped collection. Collections are created on demand.
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
        self._col_cache: Dict[str, object] = {}

    def connect(self) -> None:
        from pymilvus import connections
        if self._uri:
            connections.connect(alias="default", uri=self._uri)
        else:
            connections.connect(alias="default", host=self._host, port=str(self._port))
        self._ensure_collection_by_name(self._collection_name)
        self._col = self._col_cache[self._collection_name]

    def _ensure_collection_by_name(self, name: str) -> object:
        if name in self._col_cache:
            return self._col_cache[name]

        from pymilvus import Collection, CollectionSchema, FieldSchema, DataType, utility

        if utility.has_collection(name):
            col = Collection(name)
            col.load()
            self._col_cache[name] = col
            return col

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
        col = Collection(name, schema=schema)
        col.create_index(
            "embedding",
            {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}},
        )
        col.load()
        self._col_cache[name] = col
        return col

    def _ensure_collection(self) -> None:
        self._col = self._ensure_collection_by_name(self._collection_name)

    def upsert(self, rows: List[dict], collection: Optional[str] = None) -> int:
        """Upsert rows by chunk_id into the given collection (defaults to configured collection).

        Pass collection='{tenant_id}_docs' to route rows to the correct tenant-scoped collection.
        The target collection is created on demand if it does not exist.
        """
        if not rows:
            return 0
        target_name = collection or self._collection_name
        col = self._col_cache.get(target_name)
        if col is None:
            col = self._ensure_collection_by_name(target_name)
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
        mr = col.upsert(data)
        col.flush()
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
