import json
import os
import sys
import time
from unittest.mock import MagicMock, call, patch

import pytest

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "services", "embedding-worker", "src"
    ),
)

from backends import LocalCPUBackend, OpenAIBackend, RateLimitError, make_backend
from config import Config
from events import DocumentChunkEvent


# ──────────────────── helpers ────────────────────

def _make_cfg(**overrides) -> Config:
    cfg = Config()
    cfg.kafka_input_topic = "document-chunks"
    cfg.kafka_event_topic = "embedding-events"
    cfg.kafka_dlq_topic = "dlq-document-chunks"
    cfg.kafka_produce_timeout_ms = 5000
    cfg.embedding_batch_size = overrides.get("embedding_batch_size", 32)
    cfg.embedding_batch_timeout_ms = overrides.get("embedding_batch_timeout_ms", 500)
    cfg.embedding_model = "BAAI/bge-small-en-v1.5"
    cfg.embedding_dim = 384
    cfg.openai_embedding_model = "text-embedding-3-small"
    cfg.openai_embedding_dim = 1536
    return cfg


def _make_chunk_event(
    doc_id="doc-1",
    chunk_id="doc-1:0",
    chunk_index=0,
    total_chunks=1,
    text="hello world",
    source_id="bucket/file.pdf",
    source_type="s3",
    tenant_id="tenant-1",
    **overrides,
) -> DocumentChunkEvent:
    return DocumentChunkEvent(
        doc_id=doc_id,
        chunk_id=chunk_id,
        chunk_index=chunk_index,
        total_chunks=total_chunks,
        text=text,
        source_type=source_type,
        source_id=source_id,
        content_type="application/pdf",
        tenant_id=tenant_id,
        **overrides,
    )


def _make_kafka_msg(chunk_event: DocumentChunkEvent, topic="document-chunks", partition=0, offset=1):
    msg = MagicMock()
    msg.value.return_value = chunk_event.to_json().encode()
    msg.error.return_value = None
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.timestamp.return_value = (1, int(time.time() * 1000))
    return msg


def _make_worker(backend=None, cfg=None, **overrides):
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
    )
    return worker, consumer, backend, milvus_writer, producer, dlq_producer, db_conn


def _make_batch(n=1, base_chunk_id="doc-1"):
    batch = []
    for i in range(n):
        chunk = _make_chunk_event(
            doc_id="doc-1",
            chunk_id=f"{base_chunk_id}:{i}",
            chunk_index=i,
            total_chunks=n,
        )
        msg = _make_kafka_msg(chunk)
        batch.append((msg, chunk))
    return batch


# ──────────────────── backend tests ────────────────────

class TestLocalCPUBackend:
    def test_local_cpu_backend_returns_correct_dimension(self):
        backend = LocalCPUBackend()
        mock_model = MagicMock()
        mock_encoded = MagicMock()
        mock_encoded.tolist.return_value = [[0.0] * 384]
        mock_model.encode.return_value = mock_encoded
        backend._model = mock_model

        result = backend.embed_batch(["hello"])

        assert len(result) == 1
        assert len(result[0]) == 384

    def test_local_cpu_backend_dim_property(self):
        backend = LocalCPUBackend()
        assert backend.dim == 384

    def test_local_cpu_backend_lazy_loads_model(self):
        backend = LocalCPUBackend()
        assert backend._model is None


class TestBackendSelection:
    def test_backend_selected_by_env_var_openai(self):
        cfg = _make_cfg()
        backend = make_backend("openai", cfg)
        assert isinstance(backend, OpenAIBackend)

    def test_backend_selected_by_env_var_local_cpu(self):
        cfg = _make_cfg()
        backend = make_backend("local-cpu", cfg)
        assert isinstance(backend, LocalCPUBackend)

    def test_make_backend_raises_on_unknown(self):
        cfg = _make_cfg()
        with pytest.raises(ValueError, match="Unknown embedding backend"):
            make_backend("unknown-backend", cfg)


# ──────────────────── batch accumulation tests ────────────────────

class TestBatchAccumulation:
    def test_batch_accumulates_up_to_32_chunks(self):
        """_collect_batch stops at batch_size=32 before timeout."""
        worker, consumer, backend, milvus, producer, dlq, db = _make_worker(
            cfg=_make_cfg(embedding_batch_size=32, embedding_batch_timeout_ms=5000)
        )

        chunks = [_make_chunk_event(chunk_id=f"doc-1:{i}", chunk_index=i) for i in range(32)]
        msgs = [_make_kafka_msg(c) for c in chunks]

        call_count = 0

        def poll_side_effect(timeout=None):
            nonlocal call_count
            if call_count < 32:
                result = msgs[call_count]
                call_count += 1
                return result
            return None

        consumer.poll.side_effect = poll_side_effect

        batch = worker._collect_batch()

        assert len(batch) == 32

    def test_batch_flushes_after_timeout(self):
        """_collect_batch returns after timeout with fewer than batch_size messages."""
        worker, consumer, backend, milvus, producer, dlq, db = _make_worker(
            cfg=_make_cfg(embedding_batch_size=32, embedding_batch_timeout_ms=100)
        )

        chunks = [_make_chunk_event(chunk_id=f"doc-1:{i}", chunk_index=i) for i in range(5)]
        msgs = [_make_kafka_msg(c) for c in chunks]

        call_count = 0

        def poll_side_effect(timeout=None):
            nonlocal call_count
            if call_count < 5:
                result = msgs[call_count]
                call_count += 1
                return result
            return None

        consumer.poll.side_effect = poll_side_effect

        start = time.time()
        batch = worker._collect_batch()
        elapsed = time.time() - start

        assert len(batch) == 5
        assert elapsed >= 0.08  # fired after ~100 ms


# ──────────────────── process_batch: happy path ────────────────────

class TestMilvusUpsert:
    def test_milvus_upsert_called_with_correct_fields(self):
        """embed_batch result lands in milvus.upsert rows with required fields."""
        embedding = [0.42] * 384
        backend = MagicMock()
        backend.embed_batch.return_value = [embedding]

        worker, consumer, _, milvus, producer, dlq, db = _make_worker(backend=backend)
        batch = _make_batch(n=1)

        worker._process_batch(batch)

        milvus.upsert.assert_called_once()
        rows = milvus.upsert.call_args[0][0]
        assert len(rows) == 1
        row = rows[0]
        assert row["chunk_id"] == "doc-1:0"
        assert row["text"] == "hello world"
        assert row["embedding"] == embedding
        assert row["tenant_id"] == "tenant-1"

    def test_duplicate_chunk_id_uses_upsert_not_insert(self):
        """Replaying same chunk_id calls upsert each time, not insert."""
        chunk = _make_chunk_event(chunk_id="doc-1:0")
        msg = _make_kafka_msg(chunk)

        worker, consumer, _, milvus, producer, dlq, db = _make_worker()

        worker._process_batch([(msg, chunk)])
        worker._process_batch([(msg, chunk)])

        assert milvus.upsert.call_count == 2
        assert not hasattr(milvus, "insert") or milvus.insert.call_count == 0


# ──────────────────── process_batch: DLQ routing ────────────────────

class TestDLQRouting:
    def test_embedding_timeout_routes_chunk_to_dlq(self):
        """TimeoutError on embed_batch → DLQ after 1 retry; offset not committed."""
        backend = MagicMock()
        backend.embed_batch.side_effect = TimeoutError("inference timeout")

        worker, consumer, _, milvus, producer, dlq, db = _make_worker(backend=backend)
        batch = _make_batch(n=1)

        worker._process_batch(batch)

        dlq.produce.assert_called_once()
        dlq_payload = json.loads(dlq.produce.call_args[1]["value"].decode())
        assert dlq_payload["failure_reason"] == "embedding_error"
        assert dlq_payload["original_topic"] == "document-chunks"

        consumer.commit.assert_not_called()
        milvus.upsert.assert_not_called()

    def test_milvus_error_routes_to_dlq(self):
        """Milvus upsert failure routes chunks to DLQ; offset not committed."""
        backend = MagicMock()
        backend.embed_batch.return_value = [[0.1] * 384]

        worker, consumer, _, milvus, producer, dlq, db = _make_worker(backend=backend)
        milvus.upsert.side_effect = Exception("Milvus unavailable")

        batch = _make_batch(n=1)
        worker._process_batch(batch)

        dlq.produce.assert_called_once()
        dlq_payload = json.loads(dlq.produce.call_args[1]["value"].decode())
        assert dlq_payload["failure_reason"] == "milvus_error"
        consumer.commit.assert_not_called()

    def test_embedding_retry_succeeds_on_second_attempt(self):
        """First embed_batch fails; second attempt succeeds — no DLQ, offset committed."""
        embedding = [0.5] * 384
        backend = MagicMock()
        backend.embed_batch.side_effect = [Exception("transient"), [embedding]]

        worker, consumer, _, milvus, producer, dlq, db = _make_worker(backend=backend)
        batch = _make_batch(n=1)

        worker._process_batch(batch)

        dlq.produce.assert_not_called()
        milvus.upsert.assert_called_once()
        consumer.commit.assert_called_once()


# ──────────────────── process_batch: rate limiting ────────────────────

class TestRateLimiting:
    def test_openai_backend_respects_retry_after_header(self):
        """RateLimitError(retry_after=2) → worker sleeps ≥2 s then retries."""
        embedding = [0.1] * 384
        backend = MagicMock()
        backend.embed_batch.side_effect = [
            RateLimitError("429 Too Many Requests", retry_after=2.0),
            [embedding],
        ]

        worker, consumer, _, milvus, producer, dlq, db = _make_worker(backend=backend)
        batch = _make_batch(n=1)

        with patch("worker.time.sleep") as mock_sleep:
            worker._process_batch(batch)

        mock_sleep.assert_called_once_with(2.0)
        milvus.upsert.assert_called_once()
        consumer.commit.assert_called_once()
        dlq.produce.assert_not_called()

    def test_persistent_rate_limit_routes_to_dlq(self):
        """Two consecutive RateLimitErrors → DLQ; offset not committed."""
        backend = MagicMock()
        backend.embed_batch.side_effect = [
            RateLimitError("429", retry_after=0.0),
            RateLimitError("429", retry_after=0.0),
        ]

        worker, consumer, _, milvus, producer, dlq, db = _make_worker(backend=backend)
        batch = _make_batch(n=1)

        with patch("worker.time.sleep"):
            worker._process_batch(batch)

        dlq.produce.assert_called_once()
        consumer.commit.assert_not_called()


# ──────────────────── process_batch: success path ────────────────────

class TestSuccessPath:
    def test_offset_committed_after_successful_batch(self):
        worker, consumer, backend, milvus, producer, dlq, db = _make_worker()
        batch = _make_batch(n=3)

        worker._process_batch(batch)

        assert consumer.commit.call_count == 3
        dlq.produce.assert_not_called()

    def test_embedding_event_published_per_chunk(self):
        worker, consumer, backend, milvus, producer, dlq, db = _make_worker()
        batch = _make_batch(n=2)

        worker._process_batch(batch)

        assert producer.produce.call_count == 2
        call_args = producer.produce.call_args_list[0]
        evt = json.loads(call_args[1]["value"].decode())
        assert evt["doc_id"] == "doc-1"
        assert evt["tenant_id"] == "tenant-1"
        assert evt["chunk_count"] == 2

    def test_postgres_status_updated_to_indexed(self):
        worker, consumer, backend, milvus, producer, dlq, db = _make_worker()
        batch = _make_batch(n=1)

        with patch("worker.update_source_file_status") as mock_status:
            worker._process_batch(batch)

        mock_status.assert_called_once_with(db, "bucket/file.pdf", "indexed", 1)

    def test_empty_batch_is_noop(self):
        worker, consumer, backend, milvus, producer, dlq, db = _make_worker()

        worker._process_batch([])

        backend.embed_batch.assert_not_called()
        milvus.upsert.assert_not_called()
        consumer.commit.assert_not_called()


# ──────────────────── usage event publishing ────────────────────

class TestUsageEventPublishing:
    def test_usage_event_published_per_tenant_on_success(self):
        """After a successful batch, usage_producer.produce() is called once per tenant."""
        usage_producer = MagicMock()
        worker, consumer, backend, milvus, producer, dlq, db = _make_worker()
        worker._usage_producer = usage_producer
        worker._cfg.kafka_usage_topic = "usage-events"

        batch = _make_batch(n=3)
        worker._process_batch(batch)

        # All 3 chunks belong to tenant-1 → one usage event
        usage_producer.produce.assert_called_once()
        call_kwargs = usage_producer.produce.call_args[1]
        assert call_kwargs["topic"] == "usage-events"
        assert call_kwargs["key"] == b"tenant-1"
        payload = json.loads(call_kwargs["value"].decode())
        assert payload["type"] == "pipeline.embedding.batch"
        assert payload["subject"] == "tenant-1"
        assert payload["data"]["batch_size"] == 3
        assert payload["data"]["gpu_seconds"] >= 0

    def test_usage_event_not_published_on_dlq_failure(self):
        """DLQ routing on embedding error means no usage event is published."""
        backend = MagicMock()
        backend.embed_batch.side_effect = Exception("backend error")

        usage_producer = MagicMock()
        worker, consumer, _, milvus, producer, dlq, db = _make_worker(backend=backend)
        worker._usage_producer = usage_producer

        batch = _make_batch(n=1)
        worker._process_batch(batch)

        usage_producer.produce.assert_not_called()

    def test_no_usage_producer_does_not_raise(self):
        """Worker without usage_producer completes batch normally."""
        worker, consumer, backend, milvus, producer, dlq, db = _make_worker()
        worker._usage_producer = None

        batch = _make_batch(n=1)
        worker._process_batch(batch)

        consumer.commit.assert_called_once()

    def test_usage_event_failure_does_not_abort_batch(self):
        """usage_producer.produce() raising does not stop offset commit."""
        usage_producer = MagicMock()
        usage_producer.produce.side_effect = Exception("Kafka unavailable")

        worker, consumer, backend, milvus, producer, dlq, db = _make_worker()
        worker._usage_producer = usage_producer

        batch = _make_batch(n=1)
        worker._process_batch(batch)

        consumer.commit.assert_called_once()
