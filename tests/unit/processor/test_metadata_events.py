"""
Unit tests for metadata.entity.created CloudEvent publishing in doc-processor.
Covers:
  - build_cloudevent() structure
  - MetadataEventPublisher.publish_document_chunk() — payload, quality checks
  - DocumentProcessor publishes DocumentChunk metadata events after successful chunk publish
  - Metadata publish failure does not stop chunk processing
  - Metadata events not published when chunk publish fails
"""

import json
import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_SRC = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "services", "doc-processor", "src")
)
sys.path.insert(0, _SRC)

from chunker import Chunk
from config import Config
from events import RawDocumentEvent
from metadata_event import MetadataEventPublisher, build_cloudevent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg() -> Config:
    cfg = Config()
    cfg.kafka_input_topic = "raw-documents"
    cfg.kafka_output_topic = "document-chunks"
    cfg.kafka_dlq_topic = "dlq-raw-documents"
    cfg.kafka_produce_timeout_ms = 5000
    cfg.chunk_size_tokens = 512
    cfg.chunk_overlap_tokens = 64
    cfg.metadata_events_topic = "metadata-events"
    return cfg


def _make_raw_event(**overrides) -> RawDocumentEvent:
    defaults = dict(
        source_type="s3",
        source_id="bucket/doc.pdf",
        content_ref="s3://bucket/doc.pdf",
        content_type="text/plain",
        tenant_id="tenant-1",
        metadata={},
    )
    defaults.update(overrides)
    return RawDocumentEvent(**defaults)


def _make_kafka_msg(event: RawDocumentEvent):
    import time
    msg = MagicMock()
    msg.value.return_value = event.to_json().encode()
    msg.error.return_value = None
    msg.topic.return_value = "raw-documents"
    msg.partition.return_value = 0
    msg.offset.return_value = 1
    msg.timestamp.return_value = (1, int(time.time() * 1000))
    return msg


def _make_mock_chunk(doc_id="doc-abc", index=0, text="chunk text", token_count=100):
    c = MagicMock(spec=Chunk)
    c.doc_id = doc_id
    c.chunk_id = f"{doc_id}:{index}"
    c.index = index
    c.text = text
    c.token_count = token_count
    return c


def _make_processor(metadata_producer=None):
    # Evict shared module names so processor.py finds doc-processor's versions,
    # not the embedding-worker versions that the embedder test module imports at
    # collection time.
    _shared = ("events", "config", "metadata_event", "processor",
               "chunker", "parsers", "status")
    for _m in _shared:
        sys.modules.pop(_m, None)
    _emb_src = _SRC.replace("doc-processor", "embedding-worker")
    for _p in (_emb_src, _SRC):
        try:
            sys.path.remove(_p)
        except ValueError:
            pass
    sys.path.insert(0, _SRC)

    from processor import DocumentProcessor

    consumer = MagicMock()
    producer = MagicMock()
    dlq_producer = MagicMock()
    db_conn = MagicMock()
    cursor = MagicMock()
    db_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    db_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    proc = DocumentProcessor(
        consumer=consumer,
        producer=producer,
        dlq_producer=dlq_producer,
        content_fetcher=MagicMock(return_value=b"sample text"),
        db_conn=db_conn,
        cfg=_make_cfg(),
        metadata_producer=metadata_producer,
    )
    return proc, consumer, producer, dlq_producer


# ===========================================================================
# Section 1 — build_cloudevent() structure
# ===========================================================================
class TestBuildCloudevent:
    def test_required_fields_present(self):
        evt = build_cloudevent(
            event_type="metadata.entity.created",
            source="doc-processor",
            subject="DocumentChunk/doc-abc:0",
            data={"entity_type": "DocumentChunk"},
        )
        assert evt["specversion"] == "1.0"
        assert evt["type"] == "metadata.entity.created"
        assert evt["source"] == "doc-processor"
        assert evt["subject"] == "DocumentChunk/doc-abc:0"
        assert evt["datacontenttype"] == "application/json"
        assert "id" in evt
        assert "time" in evt
        assert isinstance(evt["data"], dict)

    def test_id_is_unique_uuid(self):
        e1 = build_cloudevent("t", "s", "sub", {})
        e2 = build_cloudevent("t", "s", "sub", {})
        assert e1["id"] != e2["id"]
        assert len(e1["id"]) == 36

    def test_time_is_iso8601_with_timezone(self):
        evt = build_cloudevent("t", "s", "sub", {})
        dt = datetime.fromisoformat(evt["time"])
        assert dt.tzinfo is not None


# ===========================================================================
# Section 2 — MetadataEventPublisher — DocumentChunk events
# ===========================================================================
class TestPublishDocumentChunk:
    def _pub(self, producer=None):
        p = producer or MagicMock()
        return MetadataEventPublisher(producer=p, topic="metadata-events"), p

    def _publish(self, pub, token_count=100, text_preview="hello world", chunk_id="doc-abc:0"):
        pub.publish_document_chunk(
            chunk_id=chunk_id,
            doc_id="doc-abc",
            chunk_index=0,
            total_chunks=3,
            token_count=token_count,
            text_preview=text_preview,
            tenant_id="tenant-1",
        )

    def test_produce_called_once(self):
        pub, producer = self._pub()
        self._publish(pub)
        assert producer.produce.call_count == 1
        assert producer.flush.call_count == 1

    def test_topic_is_metadata_events(self):
        pub, producer = self._pub()
        self._publish(pub)
        topic = producer.produce.call_args[0][0]
        assert topic == "metadata-events"

    def test_entity_type_is_document_chunk(self):
        pub, producer = self._pub()
        self._publish(pub)
        data = json.loads(producer.produce.call_args[1]["value"])["data"]
        assert data["entity_type"] == "DocumentChunk"

    def test_entity_key_matches_chunk_id(self):
        pub, producer = self._pub()
        self._publish(pub, chunk_id="doc-abc:2")
        data = json.loads(producer.produce.call_args[1]["value"])["data"]
        assert data["entity_key"] == "doc-abc:2"

    def test_subject_format(self):
        pub, producer = self._pub()
        self._publish(pub, chunk_id="doc-abc:0")
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["subject"] == "DocumentChunk/doc-abc:0"

    def test_source_is_doc_processor(self):
        pub, producer = self._pub()
        self._publish(pub)
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["source"] == "doc-processor"

    def test_tenant_id_in_data(self):
        pub, producer = self._pub()
        self._publish(pub)
        data = json.loads(producer.produce.call_args[1]["value"])["data"]
        assert data["tenant_id"] == "tenant-1"

    def test_attributes_contain_doc_id_and_indexes(self):
        pub, producer = self._pub()
        self._publish(pub, token_count=200)
        attrs = json.loads(producer.produce.call_args[1]["value"])["data"]["attributes"]
        assert attrs["doc_id"] == "doc-abc"
        assert attrs["chunk_index"] == 0
        assert attrs["total_chunks"] == 3
        assert attrs["token_count"] == 200

    def test_text_preview_in_attributes(self):
        pub, producer = self._pub()
        self._publish(pub, text_preview="Revenue grew 24% YoY")
        attrs = json.loads(producer.produce.call_args[1]["value"])["data"]["attributes"]
        assert attrs["text_preview"] == "Revenue grew 24% YoY"

    def test_upstream_has_chunked_into_edge(self):
        pub, producer = self._pub()
        self._publish(pub)
        upstream = json.loads(producer.produce.call_args[1]["value"])["data"]["upstream"]
        assert len(upstream) == 1
        assert upstream[0]["entity_type"] == "RawDocument"
        assert upstream[0]["entity_key"] == "doc-abc"
        assert upstream[0]["relationship"] == "chunked_into"

    def test_quality_check_not_empty_passes(self):
        pub, producer = self._pub()
        self._publish(pub, token_count=100)
        checks = json.loads(producer.produce.call_args[1]["value"])["data"]["quality_checks"]
        not_empty = next(c for c in checks if c["check_name"] == "not_empty")
        assert not_empty["status"] == "passed"

    def test_quality_check_not_empty_fails_for_zero_tokens(self):
        pub, producer = self._pub()
        self._publish(pub, token_count=0)
        checks = json.loads(producer.produce.call_args[1]["value"])["data"]["quality_checks"]
        not_empty = next(c for c in checks if c["check_name"] == "not_empty")
        assert not_empty["status"] == "failed"

    def test_quality_check_min_token_count_passes(self):
        pub, producer = self._pub()
        self._publish(pub, token_count=50)
        checks = json.loads(producer.produce.call_args[1]["value"])["data"]["quality_checks"]
        min_tok = next(c for c in checks if c["check_name"] == "min_token_count")
        assert min_tok["status"] == "passed"
        assert min_tok["value"] == 50
        assert min_tok["threshold"] == 50

    def test_quality_check_min_token_count_fails_below_threshold(self):
        pub, producer = self._pub()
        self._publish(pub, token_count=30)
        checks = json.loads(producer.produce.call_args[1]["value"])["data"]["quality_checks"]
        min_tok = next(c for c in checks if c["check_name"] == "min_token_count")
        assert min_tok["status"] == "failed"
        assert min_tok["value"] == 30

    def test_publish_failure_is_swallowed(self):
        producer = MagicMock()
        producer.produce.side_effect = Exception("broker down")
        pub, _ = self._pub(producer)
        self._publish(pub)  # must not raise

    def test_flush_failure_is_swallowed(self):
        producer = MagicMock()
        producer.flush.side_effect = Exception("timeout")
        pub = MetadataEventPublisher(producer=producer, topic="metadata-events")
        pub.publish_document_chunk("id", "doc", 0, 1, 100, "preview", "t")  # must not raise


# ===========================================================================
# Section 3 — DocumentProcessor metadata events integration
# ===========================================================================
class TestDocumentProcessorMetadataEvents:
    def test_no_metadata_publisher_when_metadata_producer_is_none(self):
        proc, _, _, _ = _make_processor(metadata_producer=None)
        assert proc._metadata_publisher is None

    def test_metadata_publisher_created_when_producer_provided(self):
        meta = MagicMock()
        proc, _, _, _ = _make_processor(metadata_producer=meta)
        assert proc._metadata_publisher is not None

    def test_metadata_event_published_per_chunk_on_success(self):
        meta = MagicMock()
        proc, consumer, producer, _ = _make_processor(metadata_producer=meta)
        event = _make_raw_event()
        msg = _make_kafka_msg(event)
        chunk = _make_mock_chunk(doc_id="doc-1", index=0, text="chunk text", token_count=80)

        with patch.object(proc._chunker, "chunk", return_value=[chunk]):
            proc._process_message(msg)

        assert meta.produce.call_count == 1
        payload = json.loads(meta.produce.call_args[1]["value"])
        assert payload["data"]["entity_type"] == "DocumentChunk"

    def test_metadata_entity_key_matches_chunk_id(self):
        meta = MagicMock()
        proc, _, _, _ = _make_processor(metadata_producer=meta)
        event = _make_raw_event()
        msg = _make_kafka_msg(event)
        chunk = _make_mock_chunk(doc_id="doc-xyz", index=2, token_count=60)

        with patch.object(proc._chunker, "chunk", return_value=[chunk]):
            proc._process_message(msg)

        data = json.loads(meta.produce.call_args[1]["value"])["data"]
        assert data["entity_key"] == "doc-xyz:2"

    def test_metadata_upstream_links_to_rawdocument(self):
        meta = MagicMock()
        proc, _, _, _ = _make_processor(metadata_producer=meta)
        event = _make_raw_event()
        msg = _make_kafka_msg(event)
        chunk = _make_mock_chunk(doc_id="doc-abc", token_count=100)

        with patch.object(proc._chunker, "chunk", return_value=[chunk]):
            proc._process_message(msg)

        upstream = json.loads(meta.produce.call_args[1]["value"])["data"]["upstream"]
        assert upstream[0]["entity_type"] == "RawDocument"
        assert upstream[0]["entity_key"] == "doc-abc"
        assert upstream[0]["relationship"] == "chunked_into"

    def test_metadata_published_for_each_chunk_in_batch(self):
        meta = MagicMock()
        proc, _, _, _ = _make_processor(metadata_producer=meta)
        event = _make_raw_event()
        msg = _make_kafka_msg(event)
        chunks = [
            _make_mock_chunk(doc_id="doc-1", index=0, token_count=100),
            _make_mock_chunk(doc_id="doc-1", index=1, token_count=90),
            _make_mock_chunk(doc_id="doc-1", index=2, token_count=85),
        ]

        with patch.object(proc._chunker, "chunk", return_value=chunks):
            proc._process_message(msg)

        assert meta.produce.call_count == 3

    def test_metadata_not_published_when_chunk_publish_fails(self):
        meta = MagicMock()
        proc, _, producer, _ = _make_processor(metadata_producer=meta)
        event = _make_raw_event()
        msg = _make_kafka_msg(event)
        chunk = _make_mock_chunk(token_count=100)

        producer.produce.side_effect = Exception("Kafka unavailable")

        with patch.object(proc._chunker, "chunk", return_value=[chunk]), \
             patch("processor.time.sleep"):
            proc._process_message(msg)

        assert meta.produce.call_count == 0

    def test_metadata_failure_does_not_stop_offset_commit(self):
        meta = MagicMock()
        meta.produce.side_effect = Exception("broker down")
        proc, consumer, _, _ = _make_processor(metadata_producer=meta)
        event = _make_raw_event()
        msg = _make_kafka_msg(event)
        chunk = _make_mock_chunk(token_count=100)

        with patch.object(proc._chunker, "chunk", return_value=[chunk]):
            proc._process_message(msg)

        consumer.commit.assert_called_once_with(message=msg)

    def test_total_chunks_reflects_full_batch_size(self):
        meta = MagicMock()
        proc, _, _, _ = _make_processor(metadata_producer=meta)
        event = _make_raw_event()
        msg = _make_kafka_msg(event)
        chunks = [
            _make_mock_chunk(doc_id="d", index=0, token_count=100),
            _make_mock_chunk(doc_id="d", index=1, token_count=100),
        ]

        with patch.object(proc._chunker, "chunk", return_value=chunks):
            proc._process_message(msg)

        # First call: total_chunks should be 2
        first_data = json.loads(meta.produce.call_args_list[0][1]["value"])["data"]
        assert first_data["attributes"]["total_chunks"] == 2
