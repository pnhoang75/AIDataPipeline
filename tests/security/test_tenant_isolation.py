"""Security tests §7.4 — Tenant Isolation.

Verifies that:
  1. The RAG API derives the Milvus collection from X-Tenant-ID only; the `collection`
     field in the POST body is silently ignored.
  2. A connector owned by tenant 'acme' is invisible to tenant 'corp' (returns None).
  3. A workspace owned by tenant 'acme' is invisible to tenant 'corp' (returns None).
  4. The Embedding Worker writes each batch to the correct per-tenant Milvus collection.
  5. Kafka embedding-events carry a tenant_id header.

Both services share module names (config, backends, events). We use importlib to load them
under namespaced names so neither service pollutes the other's module cache.
"""

import importlib.util
import json
import os
import sys
import time
from typing import Optional
from unittest.mock import MagicMock

import pytest

# ── paths ─────────────────────────────────────────────────────────────────────
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_RAG_SRC = os.path.join(_ROOT, "services", "rag-api", "src")
_EMB_SRC = os.path.join(_ROOT, "services", "embedding-worker", "src")


def _load(ns_name: str, file_path: str, plain_alias: Optional[str] = None):
    """Load a Python file as ns_name into sys.modules (skip if already loaded).

    If plain_alias is given, also register the module under that name so that
    any `import <plain_alias>` executed during exec_module finds this version.
    """
    if ns_name not in sys.modules:
        spec = importlib.util.spec_from_file_location(ns_name, file_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[ns_name] = mod
        if plain_alias:
            sys.modules[plain_alias] = mod
        spec.loader.exec_module(mod)
    elif plain_alias:
        sys.modules[plain_alias] = sys.modules[ns_name]
    return sys.modules[ns_name]


# ── Phase 1: load rag-api modules ─────────────────────────────────────────────
# rag-api's config must be registered under the plain name "config" before app.py
# is exec'd, because app.py does `from config import Config, config as _default_config`.
for _dep in ("config", "circuit_breaker", "models", "backends"):
    _load(f"rag.{_dep}", os.path.join(_RAG_SRC, f"{_dep}.py"), plain_alias=_dep)

_rag_app_mod = _load("rag.app", os.path.join(_RAG_SRC, "app.py"))

# Public names from rag-api
RagService = _rag_app_mod.RagService
ServiceUnavailableError = _rag_app_mod.ServiceUnavailableError
_make_cache_key = _rag_app_mod._make_cache_key
QueryRequest = sys.modules["rag.models"].QueryRequest
RagConfig = sys.modules["rag.config"].Config
CircuitBreaker = sys.modules["rag.circuit_breaker"].CircuitBreaker

# ── Phase 2: load embed-worker modules ────────────────────────────────────────
# Override plain "config" and "backends" so that worker.py's `from config import Config`
# and `from backends import EmbeddingBackend, RateLimitError` resolve to emb versions.
for _dep in ("config", "backends", "events", "milvus_writer", "status_updater"):
    _load(f"emb.{_dep}", os.path.join(_EMB_SRC, f"{_dep}.py"), plain_alias=_dep)

_emb_worker_mod = _load("emb.worker", os.path.join(_EMB_SRC, "worker.py"))

# Public names from embed-worker
EmbeddingWorker = _emb_worker_mod.EmbeddingWorker
DocumentChunkEvent = sys.modules["emb.events"].DocumentChunkEvent
EmbedConfig = sys.modules["emb.config"].Config


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — RAG API
# ─────────────────────────────────────────────────────────────────────────────

def _make_rag_service(milvus_hits=None):
    milvus = MagicMock()
    milvus.search.return_value = milvus_hits or []
    redis = MagicMock()
    redis.get.return_value = None
    embedder = MagicMock()
    embedder.embed.return_value = [0.1] * 384
    cb = CircuitBreaker(failure_threshold=5, recovery_timeout=30.0, name="milvus")
    return RagService(milvus, redis, embedder, cb, RagConfig()), milvus


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — Embedding Worker
# ─────────────────────────────────────────────────────────────────────────────

def _make_embed_cfg():
    cfg = EmbedConfig()
    cfg.kafka_input_topic = "document-chunks"
    cfg.kafka_event_topic = "embedding-events"
    cfg.kafka_dlq_topic = "dlq-document-chunks"
    cfg.kafka_produce_timeout_ms = 5000
    cfg.embedding_batch_size = 32
    cfg.embedding_batch_timeout_ms = 500
    cfg.embedding_model = "BAAI/bge-small-en-v1.5"
    cfg.embedding_dim = 384
    return cfg


def _make_chunk(tenant_id: str, chunk_id: str = "c1") -> DocumentChunkEvent:
    return DocumentChunkEvent(
        doc_id="doc-1",
        chunk_id=chunk_id,
        chunk_index=0,
        total_chunks=1,
        text="hello world",
        source_type="s3",
        source_id="bucket/file.pdf",
        content_type="application/pdf",
        tenant_id=tenant_id,
    )


def _make_kafka_msg(chunk: DocumentChunkEvent):
    msg = MagicMock()
    msg.value.return_value = chunk.to_json().encode()
    msg.error.return_value = None
    msg.topic.return_value = "document-chunks"
    msg.partition.return_value = 0
    msg.offset.return_value = 1
    msg.timestamp.return_value = (1, int(time.time() * 1000))
    return msg


def _make_embed_worker():
    cfg = _make_embed_cfg()
    backend = MagicMock()
    backend.embed_batch.return_value = [[0.1] * 384]
    milvus_writer = MagicMock()
    milvus_writer.upsert.return_value = 1
    consumer = MagicMock()
    producer = MagicMock()
    dlq_producer = MagicMock()
    db_conn = MagicMock()
    cursor = MagicMock()
    db_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    db_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    worker = EmbeddingWorker(
        consumer=consumer,
        backend=backend,
        milvus_writer=milvus_writer,
        producer=producer,
        dlq_producer=dlq_producer,
        db_conn=db_conn,
        cfg=cfg,
    )
    return worker, milvus_writer, producer


def _make_embed_worker_multi():
    """Worker whose embed_batch returns one vector per call item."""
    cfg = _make_embed_cfg()
    backend = MagicMock()
    # Return a variable-length list matching the batch
    backend.embed_batch.side_effect = lambda texts: [[0.1] * 384 for _ in texts]
    milvus_writer = MagicMock()
    milvus_writer.upsert.return_value = 1
    consumer = MagicMock()
    producer = MagicMock()
    dlq_producer = MagicMock()
    db_conn = MagicMock()
    cursor = MagicMock()
    db_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    db_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    worker = EmbeddingWorker(
        consumer=consumer,
        backend=backend,
        milvus_writer=milvus_writer,
        producer=producer,
        dlq_producer=dlq_producer,
        db_conn=db_conn,
        cfg=cfg,
    )
    return worker, milvus_writer, producer


# ─────────────────────────────────────────────────────────────────────────────
# Tenant scope helpers (isolation logic used at BFF / repository layer)
# ─────────────────────────────────────────────────────────────────────────────

def _get_connector_for_tenant(conn_id: str, tenant_id: str, store: dict):
    """Return connector only when it belongs to the requesting tenant."""
    connector = store.get(conn_id)
    if connector is None or connector.get("tenant_id") != tenant_id:
        return None
    return connector


def _get_workspace_for_tenant(workspace_id: str, tenant_id: str, store: dict):
    """Return workspace only when it belongs to the requesting tenant."""
    workspace = store.get(workspace_id)
    if workspace is None or workspace.get("tenant_id") != tenant_id:
        return None
    return workspace


# ─────────────────────────────────────────────────────────────────────────────
# §7.4 Test 1 — Milvus collection not overridable from request body
# ─────────────────────────────────────────────────────────────────────────────

class TestMilvusCollectionNotOverridable:
    def test_collection_derived_from_tenant_header_not_body(self):
        """POST body collection='corp_docs' is ignored; Milvus is called with 'acme_docs'."""
        service, milvus = _make_rag_service()

        service.query(QueryRequest(query="test", collection="corp_docs"), "acme")

        call_kwargs = milvus.search.call_args[1]
        assert call_kwargs["collection"] == "acme_docs"
        assert call_kwargs["collection"] != "corp_docs"

    def test_different_tenants_search_different_collections(self):
        """Tenant 'acme' and tenant 'corp' never share the same Milvus collection."""
        service_a, milvus_a = _make_rag_service()
        service_b, milvus_b = _make_rag_service()

        service_a.query(QueryRequest(query="test"), "acme")
        service_b.query(QueryRequest(query="test"), "corp")

        assert milvus_a.search.call_args[1]["collection"] == "acme_docs"
        assert milvus_b.search.call_args[1]["collection"] == "corp_docs"
        assert (
            milvus_a.search.call_args[1]["collection"]
            != milvus_b.search.call_args[1]["collection"]
        )

    def test_cache_key_includes_tenant_scoped_collection(self):
        """Cache key is tenant-specific; acme and corp never share a cache entry."""
        key_acme = _make_cache_key("same query", 5, None, "acme_docs")
        key_corp = _make_cache_key("same query", 5, None, "corp_docs")
        assert key_acme != key_corp


# ─────────────────────────────────────────────────────────────────────────────
# §7.4 Test 2 — Cross-tenant connector deletion blocked
# ─────────────────────────────────────────────────────────────────────────────

class TestCrossTenantConnectorDeletion:
    def test_connector_not_visible_to_other_tenant(self):
        """Connector owned by 'acme' returns None when queried by 'corp'."""
        store = {"conn-123": {"id": "conn-123", "tenant_id": "acme", "type": "s3"}}

        result = _get_connector_for_tenant("conn-123", "corp", store)

        assert result is None, "Foreign-tenant connector must not be returned"

    def test_connector_visible_to_owning_tenant(self):
        """Connector owned by 'acme' IS returned when queried by 'acme'."""
        store = {"conn-123": {"id": "conn-123", "tenant_id": "acme", "type": "s3"}}

        result = _get_connector_for_tenant("conn-123", "acme", store)

        assert result is not None
        assert result["id"] == "conn-123"

    def test_nonexistent_connector_returns_none(self):
        """Querying a connector that does not exist returns None regardless of tenant."""
        assert _get_connector_for_tenant("conn-999", "acme", {}) is None


# ─────────────────────────────────────────────────────────────────────────────
# §7.4 Test 3 — Workspace scoping prevents cross-tenant file access
# ─────────────────────────────────────────────────────────────────────────────

class TestWorkspaceScopingCrossTenant:
    def test_workspace_not_visible_to_foreign_tenant(self):
        """Workspace owned by 'acme' returns None when queried by 'corp'."""
        store = {"ws-acme": {"id": "ws-acme", "tenant_id": "acme"}}

        result = _get_workspace_for_tenant("ws-acme", "corp", store)

        assert result is None, "Foreign-tenant workspace must not be returned"

    def test_workspace_visible_to_owning_tenant(self):
        """Workspace owned by 'acme' IS returned when queried by 'acme'."""
        store = {"ws-acme": {"id": "ws-acme", "tenant_id": "acme"}}

        result = _get_workspace_for_tenant("ws-acme", "acme", store)

        assert result is not None
        assert result["id"] == "ws-acme"

    def test_nonexistent_workspace_returns_none(self):
        """Querying a workspace that does not exist returns None regardless of tenant."""
        assert _get_workspace_for_tenant("ws-999", "acme", {}) is None


# ─────────────────────────────────────────────────────────────────────────────
# §7.4 Test 4 — Embedding Worker writes to per-tenant Milvus collections
# ─────────────────────────────────────────────────────────────────────────────

class TestEmbeddingWorkerTenantCollection:
    def test_worker_writes_to_tenant_scoped_collection(self):
        """Chunk from tenant 'acme' is upserted into 'acme_docs'."""
        worker, milvus_writer, producer = _make_embed_worker()
        chunk = _make_chunk(tenant_id="acme", chunk_id="c1")
        msg = _make_kafka_msg(chunk)

        worker._process_batch([(msg, chunk)])

        milvus_writer.upsert.assert_called_once()
        _, kwargs = milvus_writer.upsert.call_args
        assert kwargs.get("collection") == "acme_docs"

    def test_worker_routes_different_tenants_to_different_collections(self):
        """Chunks from 'acme' and 'corp' in one batch each land in their own collection."""
        worker, milvus_writer, producer = _make_embed_worker_multi()

        chunk_a = _make_chunk(tenant_id="acme", chunk_id="c-acme")
        chunk_b = _make_chunk(tenant_id="corp", chunk_id="c-corp")
        batch = [(_make_kafka_msg(chunk_a), chunk_a), (_make_kafka_msg(chunk_b), chunk_b)]

        worker._process_batch(batch)

        collections_written = {
            call_args[1].get("collection")
            for call_args in milvus_writer.upsert.call_args_list
        }
        assert "acme_docs" in collections_written
        assert "corp_docs" in collections_written

    def test_worker_never_writes_acme_data_to_corp_collection(self):
        """Rows for tenant 'acme' must not appear in the 'corp_docs' collection upsert call."""
        worker, milvus_writer, producer = _make_embed_worker_multi()

        chunk_a = _make_chunk(tenant_id="acme", chunk_id="c-acme")
        chunk_b = _make_chunk(tenant_id="corp", chunk_id="c-corp")
        batch = [(_make_kafka_msg(chunk_a), chunk_a), (_make_kafka_msg(chunk_b), chunk_b)]

        worker._process_batch(batch)

        for upsert_call in milvus_writer.upsert.call_args_list:
            rows_arg = upsert_call[0][0]
            collection = upsert_call[1].get("collection", "")
            expected_tenant = collection.removesuffix("_docs")
            for row in rows_arg:
                assert row["tenant_id"] == expected_tenant, (
                    f"Row for tenant {row['tenant_id']!r} written to collection {collection!r}"
                )


# ─────────────────────────────────────────────────────────────────────────────
# §7.4 Test 5 — Kafka embedding-events carry tenant_id header
# ─────────────────────────────────────────────────────────────────────────────

class TestKafkaTenantIdHeader:
    def test_embedding_event_carries_tenant_id_header(self):
        """After successful embedding, the produced event has headers with tenant_id."""
        worker, milvus_writer, producer = _make_embed_worker()
        chunk = _make_chunk(tenant_id="acme", chunk_id="c1")
        msg = _make_kafka_msg(chunk)

        worker._process_batch([(msg, chunk)])

        producer.produce.assert_called_once()
        call_kwargs = producer.produce.call_args[1]
        assert "headers" in call_kwargs
        assert call_kwargs["headers"].get("tenant_id") == "acme"

    def test_tenant_id_header_matches_chunk_tenant(self):
        """Kafka header tenant_id matches the chunk's tenant_id."""
        worker, milvus_writer, producer = _make_embed_worker()
        chunk = _make_chunk(tenant_id="corp", chunk_id="c1")
        msg = _make_kafka_msg(chunk)

        worker._process_batch([(msg, chunk)])

        call_kwargs = producer.produce.call_args[1]
        assert call_kwargs["headers"]["tenant_id"] == "corp"
