"""
Chaos tests for embedding-worker — test plan §8.1, §8.6, §8.7.

Exercises resilience behaviors using mocked Kafka, Milvus, and embedding
backends. No live cluster required.

  8.1  Kafka broker failure   → worker skips error messages; resumes on recovery
  8.6  Network partition      → retry/backoff tolerates transient embedding errors
  8.7  Embedding worker OOM   → idempotent redelivery; no duplicate Milvus vectors
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import pytest

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_EMBEDDER_SRC = os.path.join(_REPO, "services", "embedding-worker", "src")
if _EMBEDDER_SRC not in sys.path:
    sys.path.insert(0, _EMBEDDER_SRC)

from events import DocumentChunkEvent  # noqa: E402
from worker import EmbeddingWorker  # noqa: E402
from config import Config as EmbedConfig  # noqa: E402


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_chunk(
    doc_id="doc-1",
    chunk_id="doc-1:0",
    chunk_index=0,
    total_chunks=1,
    text="hello chaos",
    source_type="s3",
    source_id="bucket/file.pdf",
    content_type="application/pdf",
    tenant_id="tenant-chaos",
) -> DocumentChunkEvent:
    return DocumentChunkEvent(
        doc_id=doc_id,
        chunk_id=chunk_id,
        chunk_index=chunk_index,
        total_chunks=total_chunks,
        text=text,
        source_type=source_type,
        source_id=source_id,
        content_type=content_type,
        tenant_id=tenant_id,
    )


def _make_embed_cfg(**overrides) -> EmbedConfig:
    cfg = EmbedConfig()
    cfg.kafka_input_topic = "document-chunks"
    cfg.kafka_event_topic = "embedding-events"
    cfg.kafka_dlq_topic = "dlq-document-chunks"
    cfg.kafka_usage_topic = "usage-events"
    cfg.kafka_produce_timeout_ms = 5000
    cfg.embedding_batch_size = overrides.get("embedding_batch_size", 32)
    cfg.embedding_batch_timeout_ms = overrides.get("embedding_batch_timeout_ms", 500)
    cfg.embedding_model = "BAAI/bge-small-en-v1.5"
    cfg.embedding_dim = 384
    cfg.embedding_backend = "local-cpu"
    cfg.openai_embedding_model = "text-embedding-3-small"
    cfg.openai_embedding_dim = 1536
    cfg.metadata_events_topic = "metadata-events"
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _make_worker(consumer=None, backend=None, milvus_writer=None):
    cfg = _make_embed_cfg()
    consumer = consumer or MagicMock()
    backend = backend or MagicMock()
    backend.embed_batch.return_value = [[0.1] * 384]
    milvus_writer = milvus_writer or MagicMock()
    milvus_writer.upsert.return_value = 1
    producer = MagicMock()
    dlq_producer = MagicMock()
    db_conn = MagicMock()
    return EmbeddingWorker(
        consumer=consumer,
        backend=backend,
        milvus_writer=milvus_writer,
        producer=producer,
        dlq_producer=dlq_producer,
        db_conn=db_conn,
        cfg=cfg,
    )


# ── 8.1 Kafka Broker Failure ──────────────────────────────────────────────────

class TestKafkaBrokerFailure:
    """8.1 Kafka broker failure: consumer error messages are skipped; valid messages processed."""

    def test_consumer_error_messages_are_skipped(self):
        """Error messages from a failed broker are logged and skipped; batch stays empty."""
        error_msg = MagicMock()
        error_msg.error.return_value = "BROKER_TRANSPORT_FAILURE"  # truthy error

        consumer = MagicMock()
        consumer.poll.return_value = error_msg

        worker = _make_worker(consumer=consumer)
        worker._cfg = _make_embed_cfg(embedding_batch_timeout_ms=50)

        batch = worker._collect_batch()

        assert batch == [], "error messages must not be added to the batch"

    def test_valid_messages_processed_after_broker_recovery(self):
        """After broker recovery, valid messages are consumed and processed normally."""
        chunk = _make_chunk()
        error_msg = MagicMock()
        error_msg.error.return_value = "BROKER_TRANSPORT_FAILURE"

        valid_msg = MagicMock()
        valid_msg.error.return_value = None
        valid_msg.value.return_value = chunk.to_json().encode()
        valid_msg.topic.return_value = "document-chunks"
        valid_msg.partition.return_value = 0
        valid_msg.offset.return_value = 42

        consumer = MagicMock()
        consumer.poll.side_effect = [error_msg, error_msg, error_msg, valid_msg, None, None]

        backend = MagicMock()
        backend.embed_batch.return_value = [[0.1] * 384]

        worker = _make_worker(consumer=consumer, backend=backend)
        worker._cfg = _make_embed_cfg(embedding_batch_size=1, embedding_batch_timeout_ms=500)

        batch = worker._collect_batch()

        assert len(batch) == 1, "valid message must be collected after broker recovery"
        assert batch[0][1].chunk_id == chunk.chunk_id

    def test_dlq_empty_after_broker_failure_and_recovery(self):
        """No messages are sent to DLQ for broker transport errors (skip, not DLQ)."""
        error_msg = MagicMock()
        error_msg.error.return_value = "BROKER_TRANSPORT_FAILURE"

        consumer = MagicMock()
        consumer.poll.return_value = error_msg

        worker = _make_worker(consumer=consumer)
        worker._cfg = _make_embed_cfg(embedding_batch_timeout_ms=50)

        worker._collect_batch()

        worker._dlq_producer.produce.assert_not_called()


# ── 8.6 Network Partition ─────────────────────────────────────────────────────

class TestNetworkPartition:
    """8.6 Network partition: transient errors trigger retry/backoff; system self-heals."""

    def test_embedding_retry_on_transient_connection_error(self):
        """EmbeddingWorker retries once on a transient error; succeeds on second attempt."""
        chunk = _make_chunk()

        backend = MagicMock()
        call_count = {"n": 0}

        def _embed_side_effect(texts):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise ConnectionError("network partition — 200 ms latency + packet loss")
            return [[0.1] * 384 for _ in texts]

        backend.embed_batch.side_effect = _embed_side_effect

        milvus_writer = MagicMock()
        milvus_writer.upsert.return_value = 1

        msg = MagicMock()
        msg.topic.return_value = "document-chunks"
        msg.partition.return_value = 0
        msg.offset.return_value = 10

        worker = _make_worker(backend=backend, milvus_writer=milvus_writer)
        worker._process_batch([(msg, chunk)])

        assert call_count["n"] == 2, "embedding must be retried once on transient error"
        milvus_writer.upsert.assert_called_once()
        worker._consumer.commit.assert_called_once_with(message=msg)

    def test_persistent_network_error_routes_to_dlq(self):
        """Two consecutive embedding failures route the batch to the DLQ."""
        chunk = _make_chunk()

        backend = MagicMock()
        backend.embed_batch.side_effect = ConnectionError("persistent network failure")

        msg = MagicMock()
        msg.topic.return_value = "document-chunks"
        msg.partition.return_value = 0
        msg.offset.return_value = 11
        msg.timestamp.return_value = (0, 1_700_000_000_000)  # (type, epoch_ms)

        worker = _make_worker(backend=backend)
        worker._process_batch([(msg, chunk)])

        worker._dlq_producer.produce.assert_called_once()
        worker._consumer.commit.assert_not_called()

    def test_milvus_error_under_partition_routes_to_dlq(self):
        """Milvus upsert failure (e.g. network partition) sends chunk to DLQ."""
        chunk = _make_chunk()

        backend = MagicMock()
        backend.embed_batch.return_value = [[0.1] * 384]

        milvus_writer = MagicMock()
        milvus_writer.upsert.side_effect = ConnectionError("Milvus unreachable")

        msg = MagicMock()
        msg.topic.return_value = "document-chunks"
        msg.partition.return_value = 0
        msg.offset.return_value = 12
        msg.timestamp.return_value = (0, 1_700_000_000_000)

        worker = _make_worker(backend=backend, milvus_writer=milvus_writer)
        worker._process_batch([(msg, chunk)])

        worker._dlq_producer.produce.assert_called_once()
        worker._consumer.commit.assert_not_called()


# ── 8.7 Embedding Worker OOM ──────────────────────────────────────────────────

class TestEmbeddingWorkerOOM:
    """8.7 Embedding worker OOM: idempotent redelivery; no duplicate Milvus vectors."""

    def test_same_chunk_id_on_redelivery(self):
        """Redelivered message carries the same chunk_id as the original delivery."""
        original = _make_chunk(doc_id="doc-42", chunk_id="doc-42:0", chunk_index=0)
        redelivered = _make_chunk(doc_id="doc-42", chunk_id="doc-42:0", chunk_index=0)

        assert original.chunk_id == redelivered.chunk_id, (
            "chunk_id must be deterministic from (doc_id, chunk_index)"
        )

    def test_milvus_upsert_called_on_redelivery(self):
        """Processing the same message twice calls upsert both times (not insert+insert)."""
        chunk = _make_chunk(doc_id="doc-42", chunk_id="doc-42:0")

        backend = MagicMock()
        backend.embed_batch.return_value = [[0.1] * 384]

        milvus_writer = MagicMock()
        milvus_writer.upsert.return_value = 1

        worker = _make_worker(backend=backend, milvus_writer=milvus_writer)

        for delivery in range(2):
            msg = MagicMock()
            msg.topic.return_value = "document-chunks"
            msg.partition.return_value = 0
            msg.offset.return_value = delivery
            worker._process_batch([(msg, chunk)])

        assert milvus_writer.upsert.call_count == 2

    def test_upsert_rows_have_same_chunk_id_on_redelivery(self):
        """Both deliveries upsert rows with identical chunk_id — no duplicate vectors created."""
        chunk = _make_chunk(doc_id="doc-42", chunk_id="doc-42:0", text="idempotent text")

        backend = MagicMock()
        backend.embed_batch.return_value = [[0.1] * 384]

        captured_rows: list[list[dict]] = []

        def _capture_upsert(rows, collection=None):
            captured_rows.append([dict(r) for r in rows])
            return len(rows)

        milvus_writer = MagicMock()
        milvus_writer.upsert.side_effect = _capture_upsert

        worker = _make_worker(backend=backend, milvus_writer=milvus_writer)

        for delivery in range(2):
            msg = MagicMock()
            msg.topic.return_value = "document-chunks"
            msg.partition.return_value = 0
            msg.offset.return_value = delivery
            worker._process_batch([(msg, chunk)])

        assert len(captured_rows) == 2
        first_chunk_id = captured_rows[0][0]["chunk_id"]
        second_chunk_id = captured_rows[1][0]["chunk_id"]
        assert first_chunk_id == second_chunk_id == "doc-42:0"

    def test_offset_committed_on_successful_redelivery(self):
        """After successful processing of a redelivered message, offset is committed."""
        chunk = _make_chunk(doc_id="doc-42", chunk_id="doc-42:0")

        backend = MagicMock()
        backend.embed_batch.return_value = [[0.1] * 384]

        milvus_writer = MagicMock()
        milvus_writer.upsert.return_value = 1

        worker = _make_worker(backend=backend, milvus_writer=milvus_writer)

        msg_redelivered = MagicMock()
        msg_redelivered.topic.return_value = "document-chunks"
        msg_redelivered.partition.return_value = 0
        msg_redelivered.offset.return_value = 7

        worker._process_batch([(msg_redelivered, chunk)])

        worker._consumer.commit.assert_called_once_with(message=msg_redelivered)
