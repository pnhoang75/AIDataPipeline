"""
Unit tests for metadata.entity.created CloudEvent publishing in embedding-worker.
Covers:
  - build_cloudevent() structure
  - _l2_norm() computation
  - MetadataEventPublisher.publish_embedding() — payload, upstream edges, quality checks
  - EmbeddingWorker publishes Embedding metadata events after successful Milvus upsert
  - Metadata publish failure does not stop offset commit
  - Metadata events not published on Milvus error
"""

import json
import math
import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Path setup — must evict any same-named modules that a prior test file may have
# cached (e.g. doc-processor's metadata_event.py), then place embedding-worker
# src at position 0 so its versions take priority.
# ---------------------------------------------------------------------------
_SRC = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "services", "embedding-worker", "src")
)

_EVICT = {"metadata_event", "config", "events", "worker", "backends",
          "milvus_writer", "status_updater"}
for _m in _EVICT:
    sys.modules.pop(_m, None)

if _SRC in sys.path:
    sys.path.remove(_SRC)
sys.path.insert(0, _SRC)

from config import Config
from events import DocumentChunkEvent
from metadata_event import MetadataEventPublisher, _l2_norm, build_cloudevent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg(**overrides) -> Config:
    cfg = Config()
    cfg.kafka_input_topic = "document-chunks"
    cfg.kafka_event_topic = "embedding-events"
    cfg.kafka_dlq_topic = "dlq-document-chunks"
    cfg.kafka_produce_timeout_ms = 5000
    cfg.embedding_batch_size = 32
    cfg.embedding_batch_timeout_ms = 500
    cfg.embedding_model = "BAAI/bge-small-en-v1.5"
    cfg.embedding_dim = 384
    cfg.embedding_backend = "local-cpu"
    cfg.metadata_events_topic = "metadata-events"
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _make_chunk_event(
    doc_id="doc-1",
    chunk_id="doc-1:0",
    chunk_index=0,
    total_chunks=1,
    text="hello world",
    source_id="bucket/file.pdf",
    tenant_id="tenant-1",
) -> DocumentChunkEvent:
    return DocumentChunkEvent(
        doc_id=doc_id,
        chunk_id=chunk_id,
        chunk_index=chunk_index,
        total_chunks=total_chunks,
        text=text,
        source_type="s3",
        source_id=source_id,
        content_type="application/pdf",
        tenant_id=tenant_id,
    )


def _make_kafka_msg(chunk_event: DocumentChunkEvent):
    import time
    msg = MagicMock()
    msg.value.return_value = chunk_event.to_json().encode()
    msg.error.return_value = None
    msg.topic.return_value = "document-chunks"
    msg.partition.return_value = 0
    msg.offset.return_value = 1
    msg.timestamp.return_value = (1, int(time.time() * 1000))
    return msg


def _make_worker(metadata_producer=None, backend=None, cfg=None):
    # Evict shared module names so worker.py finds embedding-worker's versions,
    # not doc-processor's versions that may have been loaded by the processor
    # test helper earlier in the same pytest session.
    _shared = ("events", "config", "metadata_event", "worker",
               "backends", "milvus_writer", "status_updater")
    for _m in _shared:
        sys.modules.pop(_m, None)
    _proc_src = _SRC.replace("embedding-worker", "doc-processor")
    for _p in (_proc_src, _SRC):
        try:
            sys.path.remove(_p)
        except ValueError:
            pass
    sys.path.insert(0, _SRC)

    from worker import EmbeddingWorker

    cfg = cfg or _make_cfg()
    if backend is None:
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
        metadata_producer=metadata_producer,
    )
    return worker, consumer, backend, milvus_writer, producer, dlq_producer


def _make_batch(n=1, tenant_id="tenant-1"):
    batch = []
    for i in range(n):
        chunk = _make_chunk_event(
            doc_id="doc-1",
            chunk_id=f"doc-1:{i}",
            chunk_index=i,
            total_chunks=n,
            tenant_id=tenant_id,
        )
        msg = _make_kafka_msg(chunk)
        batch.append((msg, chunk))
    return batch


# ===========================================================================
# Section 1 — build_cloudevent() structure
# ===========================================================================
class TestBuildCloudevent:
    def test_required_fields_present(self):
        evt = build_cloudevent(
            event_type="metadata.entity.created",
            source="embedding-worker",
            subject="Embedding/doc-1:0:BAAI/bge-small-en-v1.5",
            data={"entity_type": "Embedding"},
        )
        assert evt["specversion"] == "1.0"
        assert evt["type"] == "metadata.entity.created"
        assert evt["source"] == "embedding-worker"
        assert evt["datacontenttype"] == "application/json"
        assert "id" in evt
        assert "time" in evt

    def test_id_is_unique_uuid(self):
        e1 = build_cloudevent("t", "s", "sub", {})
        e2 = build_cloudevent("t", "s", "sub", {})
        assert e1["id"] != e2["id"]
        assert len(e1["id"]) == 36

    def test_time_has_timezone(self):
        evt = build_cloudevent("t", "s", "sub", {})
        dt = datetime.fromisoformat(evt["time"])
        assert dt.tzinfo is not None


# ===========================================================================
# Section 2 — _l2_norm()
# ===========================================================================
class TestL2Norm:
    def test_unit_vector_norm_is_one(self):
        vec = [1.0, 0.0, 0.0]
        assert abs(_l2_norm(vec) - 1.0) < 1e-9

    def test_known_vector(self):
        vec = [3.0, 4.0]
        assert abs(_l2_norm(vec) - 5.0) < 1e-9

    def test_all_zeros_is_zero(self):
        assert _l2_norm([0.0, 0.0, 0.0]) == 0.0

    def test_high_dim_vector(self):
        vec = [0.1] * 384
        expected = math.sqrt(sum(0.01 for _ in range(384)))
        assert abs(_l2_norm(vec) - expected) < 1e-6


# ===========================================================================
# Section 3 — MetadataEventPublisher — Embedding events
# ===========================================================================
class TestPublishEmbedding:
    def _pub(self, producer=None, model="BAAI/bge-small-en-v1.5", dim=384, backend="local-cpu"):
        p = producer or MagicMock()
        pub = MetadataEventPublisher(
            producer=p,
            topic="metadata-events",
            model_name=model,
            embedding_dim=dim,
            backend=backend,
        )
        return pub, p

    def _embedding(self, norm_target=0.9):
        vec = [0.0] * 384
        vec[0] = norm_target
        return vec

    def _publish(self, pub, chunk_id="doc-1:0", tenant_id="tenant-1", embedding=None, collection="tenant-1_docs"):
        if embedding is None:
            embedding = self._embedding(0.9)
        pub.publish_embedding(
            chunk_id=chunk_id,
            tenant_id=tenant_id,
            embedding=embedding,
            collection_name=collection,
        )

    def test_produce_called_once(self):
        pub, producer = self._pub()
        self._publish(pub)
        assert producer.produce.call_count == 1
        assert producer.flush.call_count == 1

    def test_topic_is_metadata_events(self):
        pub, producer = self._pub()
        self._publish(pub)
        assert producer.produce.call_args[0][0] == "metadata-events"

    def test_entity_type_is_embedding(self):
        pub, producer = self._pub()
        self._publish(pub)
        data = json.loads(producer.produce.call_args[1]["value"])["data"]
        assert data["entity_type"] == "Embedding"

    def test_entity_key_contains_chunk_id(self):
        pub, producer = self._pub()
        self._publish(pub, chunk_id="doc-1:3")
        data = json.loads(producer.produce.call_args[1]["value"])["data"]
        assert "doc-1:3" in data["entity_key"]

    def test_subject_format(self):
        pub, producer = self._pub()
        self._publish(pub, chunk_id="doc-1:0")
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["subject"].startswith("Embedding/doc-1:0")

    def test_source_is_embedding_worker(self):
        pub, producer = self._pub()
        self._publish(pub)
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["source"] == "embedding-worker"

    def test_tenant_id_in_data(self):
        pub, producer = self._pub()
        self._publish(pub, tenant_id="acme")
        data = json.loads(producer.produce.call_args[1]["value"])["data"]
        assert data["tenant_id"] == "acme"

    def test_attributes_contain_model_and_dimension(self):
        pub, producer = self._pub(model="BAAI/bge-small-en-v1.5", dim=384, backend="local-cpu")
        self._publish(pub, collection="acme_docs")
        attrs = json.loads(producer.produce.call_args[1]["value"])["data"]["attributes"]
        assert attrs["model_name"] == "BAAI/bge-small-en-v1.5"
        assert attrs["dimension"] == 384
        assert attrs["backend"] == "local-cpu"
        assert attrs["collection_name"] == "acme_docs"

    def test_embedding_norm_in_attributes(self):
        pub, producer = self._pub()
        vec = [3.0, 4.0] + [0.0] * 382
        self._publish(pub, embedding=vec)
        attrs = json.loads(producer.produce.call_args[1]["value"])["data"]["attributes"]
        assert abs(attrs["embedding_norm"] - 5.0) < 0.001

    def test_upstream_has_embedded_by_edge(self):
        pub, producer = self._pub()
        self._publish(pub, chunk_id="doc-1:0")
        upstream = json.loads(producer.produce.call_args[1]["value"])["data"]["upstream"]
        embedded_by = next(u for u in upstream if u["relationship"] == "embedded_by")
        assert embedded_by["entity_type"] == "DocumentChunk"
        assert embedded_by["entity_key"] == "doc-1:0"

    def test_upstream_has_stored_in_edge(self):
        pub, producer = self._pub()
        self._publish(pub, collection="acme_docs")
        upstream = json.loads(producer.produce.call_args[1]["value"])["data"]["upstream"]
        stored_in = next(u for u in upstream if u["relationship"] == "stored_in")
        assert stored_in["entity_type"] == "VectorCollection"
        assert stored_in["entity_key"] == "acme_docs"

    def test_quality_check_embedding_norm_passes(self):
        pub, producer = self._pub()
        vec = [0.6] + [0.0] * 383
        self._publish(pub, embedding=vec)
        checks = json.loads(producer.produce.call_args[1]["value"])["data"]["quality_checks"]
        norm_check = next(c for c in checks if c["check_name"] == "embedding_norm")
        assert norm_check["status"] == "passed"
        assert norm_check["threshold"] == 0.5

    def test_quality_check_embedding_norm_fails_below_threshold(self):
        pub, producer = self._pub()
        vec = [0.3] + [0.0] * 383
        self._publish(pub, embedding=vec)
        checks = json.loads(producer.produce.call_args[1]["value"])["data"]["quality_checks"]
        norm_check = next(c for c in checks if c["check_name"] == "embedding_norm")
        assert norm_check["status"] == "failed"
        assert norm_check["value"] < 0.5

    def test_publish_failure_is_swallowed(self):
        producer = MagicMock()
        producer.produce.side_effect = Exception("broker down")
        pub, _ = self._pub(producer)
        self._publish(pub)  # must not raise

    def test_flush_failure_is_swallowed(self):
        producer = MagicMock()
        producer.flush.side_effect = Exception("timeout")
        pub = MetadataEventPublisher(producer, "metadata-events", "model", 384, "local-cpu")
        pub.publish_embedding("id", "t", [0.1] * 384, "col")  # must not raise


# ===========================================================================
# Section 4 — EmbeddingWorker metadata events integration
# ===========================================================================
class TestEmbeddingWorkerMetadataEvents:
    def test_no_metadata_publisher_when_producer_is_none(self):
        worker, *_ = _make_worker(metadata_producer=None)
        assert worker._metadata_publisher is None

    def test_metadata_publisher_created_when_producer_provided(self):
        meta = MagicMock()
        worker, *_ = _make_worker(metadata_producer=meta)
        assert worker._metadata_publisher is not None

    def test_metadata_event_published_per_chunk_on_success(self):
        meta = MagicMock()
        embedding = [0.1] * 384
        backend = MagicMock()
        backend.embed_batch.return_value = [embedding]
        worker, consumer, _, milvus, producer, dlq = _make_worker(
            metadata_producer=meta, backend=backend
        )
        batch = _make_batch(n=1)
        worker._process_batch(batch)

        assert meta.produce.call_count == 1
        payload = json.loads(meta.produce.call_args[1]["value"])
        assert payload["data"]["entity_type"] == "Embedding"

    def test_metadata_entity_key_contains_chunk_id_and_model(self):
        meta = MagicMock()
        embedding = [0.1] * 384
        backend = MagicMock()
        backend.embed_batch.return_value = [embedding]
        worker, *_ = _make_worker(metadata_producer=meta, backend=backend)
        batch = _make_batch(n=1)
        worker._process_batch(batch)

        data = json.loads(meta.produce.call_args[1]["value"])["data"]
        assert "doc-1:0" in data["entity_key"]
        assert "BAAI/bge-small-en-v1.5" in data["entity_key"]

    def test_collection_name_uses_tenant_id(self):
        meta = MagicMock()
        backend = MagicMock()
        backend.embed_batch.return_value = [[0.1] * 384]
        worker, *_ = _make_worker(metadata_producer=meta, backend=backend)
        batch = _make_batch(n=1, tenant_id="acme")
        worker._process_batch(batch)

        attrs = json.loads(meta.produce.call_args[1]["value"])["data"]["attributes"]
        assert attrs["collection_name"] == "acme_docs"

    def test_metadata_events_published_for_each_chunk_in_batch(self):
        meta = MagicMock()
        backend = MagicMock()
        backend.embed_batch.return_value = [[0.1] * 384, [0.2] * 384, [0.3] * 384]
        worker, consumer, _, milvus, producer, dlq = _make_worker(
            metadata_producer=meta, backend=backend
        )
        batch = _make_batch(n=3)
        worker._process_batch(batch)

        assert meta.produce.call_count == 3

    def test_metadata_not_published_on_milvus_error(self):
        meta = MagicMock()
        backend = MagicMock()
        backend.embed_batch.return_value = [[0.1] * 384]
        worker, consumer, _, milvus, producer, dlq = _make_worker(
            metadata_producer=meta, backend=backend
        )
        milvus.upsert.side_effect = Exception("Milvus unavailable")
        batch = _make_batch(n=1)
        worker._process_batch(batch)

        assert meta.produce.call_count == 0

    def test_metadata_not_published_on_embedding_error(self):
        meta = MagicMock()
        backend = MagicMock()
        backend.embed_batch.side_effect = Exception("backend error")
        worker, consumer, _, milvus, producer, dlq = _make_worker(
            metadata_producer=meta, backend=backend
        )
        batch = _make_batch(n=1)
        worker._process_batch(batch)

        assert meta.produce.call_count == 0

    def test_metadata_failure_does_not_stop_offset_commit(self):
        meta = MagicMock()
        meta.produce.side_effect = Exception("broker down")
        backend = MagicMock()
        backend.embed_batch.return_value = [[0.1] * 384]
        worker, consumer, _, milvus, producer, dlq = _make_worker(
            metadata_producer=meta, backend=backend
        )
        batch = _make_batch(n=1)
        worker._process_batch(batch)

        consumer.commit.assert_called_once()

    def test_existing_embedding_event_still_published_when_metadata_fails(self):
        meta = MagicMock()
        meta.produce.side_effect = Exception("broker down")
        backend = MagicMock()
        backend.embed_batch.return_value = [[0.1] * 384]
        worker, consumer, _, milvus, producer, dlq = _make_worker(
            metadata_producer=meta, backend=backend
        )
        batch = _make_batch(n=1)
        worker._process_batch(batch)

        # Main embedding event still published
        assert producer.produce.call_count == 1
