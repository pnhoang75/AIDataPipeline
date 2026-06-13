"""
Unit tests for the Metadata Service consumer (session 5-D).
Covers:
  - MetadataConsumer.process_event(): entity upsert, lineage edge insert,
    quality check insert, DataQualityFailed event publishing on failure
  - DataQualityFailedPublisher: event shape and Kafka produce call
  - Ignored event types pass through without DB calls
"""

import json
import os
import sys
import uuid
from unittest.mock import MagicMock, call, patch

import pytest

_SRC = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "services", "metadata-service", "src")
)
_EVICT = {"config", "consumer", "db", "events", "app"}
for _m in _EVICT:
    sys.modules.pop(_m, None)

if _SRC in sys.path:
    sys.path.remove(_SRC)
sys.path.insert(0, _SRC)

from config import Config
from events import DataQualityFailedPublisher


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg(**overrides) -> Config:
    cfg = Config()
    cfg.database_url = "postgresql://test/test"
    cfg.kafka_bootstrap = "localhost:9092"
    cfg.metadata_events_topic = "metadata-events"
    cfg.kafka_consumer_group = "metadata-service"
    cfg.data_quality_failed_topic = "data-quality-failed"
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _make_consumer(producer=None):
    """Create a MetadataConsumer with mocked DB and producer."""
    from consumer import MetadataConsumer

    db_conn = MagicMock()
    producer = producer or MagicMock()
    cfg = _make_cfg()
    return MetadataConsumer(db_conn=db_conn, producer=producer, cfg=cfg), db_conn, producer


def _entity_event(
    entity_type="DocumentChunk",
    entity_key="doc-1:0",
    tenant_id="tenant-1",
    attributes=None,
    upstream=None,
    quality_checks=None,
    pipeline_run_id=None,
):
    return {
        "specversion": "1.0",
        "type": "metadata.entity.created",
        "source": "doc-processor",
        "subject": f"{entity_type}/{entity_key}",
        "id": str(uuid.uuid4()),
        "data": {
            "entity_type": entity_type,
            "entity_key": entity_key,
            "tenant_id": tenant_id,
            "attributes": attributes or {},
            "upstream": upstream or [],
            "quality_checks": quality_checks or [],
            "pipeline_run_id": pipeline_run_id,
        },
    }


# ---------------------------------------------------------------------------
# Section 1 — entity upsert
# ---------------------------------------------------------------------------

class TestEntityUpsert:
    def test_upsert_called_with_correct_entity_type(self):
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="eid") as mock_upsert, \
             patch("consumer.insert_quality_checks", return_value=[]), \
             patch("consumer.insert_lineage_edge"):
            consumer.process_event(_entity_event(entity_type="RAGQuery", entity_key="q-1"))
            assert mock_upsert.call_args[1]["entity_type"] == "RAGQuery"

    def test_upsert_called_with_entity_key(self):
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="eid") as mock_upsert, \
             patch("consumer.insert_quality_checks", return_value=[]), \
             patch("consumer.insert_lineage_edge"):
            consumer.process_event(_entity_event(entity_key="sha256:abc123"))
            assert mock_upsert.call_args[1]["entity_key"] == "sha256:abc123"

    def test_upsert_called_with_tenant_id(self):
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="eid") as mock_upsert, \
             patch("consumer.insert_quality_checks", return_value=[]), \
             patch("consumer.insert_lineage_edge"):
            consumer.process_event(_entity_event(tenant_id="acme"))
            assert mock_upsert.call_args[1]["tenant_id"] == "acme"

    def test_upsert_called_with_attributes(self):
        attrs = {"chunk_index": 3, "token_count": 498}
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="eid") as mock_upsert, \
             patch("consumer.insert_quality_checks", return_value=[]), \
             patch("consumer.insert_lineage_edge"):
            consumer.process_event(_entity_event(attributes=attrs))
            assert mock_upsert.call_args[1]["attributes"] == attrs

    def test_upsert_called_with_pipeline_run_id(self):
        run_id = str(uuid.uuid4())
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="eid") as mock_upsert, \
             patch("consumer.insert_quality_checks", return_value=[]), \
             patch("consumer.insert_lineage_edge"):
            consumer.process_event(_entity_event(pipeline_run_id=run_id))
            assert mock_upsert.call_args[1]["pipeline_run_id"] == run_id

    def test_db_commit_called_after_processing(self):
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="eid"), \
             patch("consumer.insert_quality_checks", return_value=[]), \
             patch("consumer.insert_lineage_edge"):
            consumer.process_event(_entity_event())
            db.commit.assert_called_once()


# ---------------------------------------------------------------------------
# Section 2 — lineage edge insert
# ---------------------------------------------------------------------------

class TestLineageInsert:
    def test_lineage_edge_inserted_for_each_upstream_ref(self):
        upstream = [
            {"entity_type": "RawDocument", "entity_key": "doc-1", "relationship": "chunked_into"},
        ]
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="eid"), \
             patch("consumer.insert_quality_checks", return_value=[]), \
             patch("consumer.insert_lineage_edge") as mock_lineage:
            consumer.process_event(_entity_event(upstream=upstream))
            assert mock_lineage.call_count == 1
            kw = mock_lineage.call_args[1]
            assert kw["upstream_type"] == "RawDocument"
            assert kw["upstream_key"] == "doc-1"
            assert kw["relationship"] == "chunked_into"
            assert kw["downstream_id"] == "eid"

    def test_multiple_upstream_refs_each_inserted(self):
        upstream = [
            {"entity_type": "DocumentChunk", "entity_key": "doc-1:0", "relationship": "embedded_by"},
            {"entity_type": "VectorCollection", "entity_key": "acme_docs", "relationship": "stored_in"},
        ]
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="eid"), \
             patch("consumer.insert_quality_checks", return_value=[]), \
             patch("consumer.insert_lineage_edge") as mock_lineage:
            consumer.process_event(_entity_event(upstream=upstream))
            assert mock_lineage.call_count == 2
            relationships = {c[1]["relationship"] for c in mock_lineage.call_args_list}
            assert relationships == {"embedded_by", "stored_in"}

    def test_no_upstream_means_no_lineage_call(self):
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="eid"), \
             patch("consumer.insert_quality_checks", return_value=[]), \
             patch("consumer.insert_lineage_edge") as mock_lineage:
            consumer.process_event(_entity_event(upstream=[]))
            mock_lineage.assert_not_called()

    def test_lineage_failure_does_not_abort_processing(self):
        upstream = [
            {"entity_type": "RawDocument", "entity_key": "missing", "relationship": "chunked_into"},
        ]
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="eid"), \
             patch("consumer.insert_quality_checks", return_value=[]), \
             patch("consumer.insert_lineage_edge", side_effect=Exception("upstream not found")):
            # Should not raise
            consumer.process_event(_entity_event(upstream=upstream))
            db.commit.assert_called_once()


# ---------------------------------------------------------------------------
# Section 3 — quality check insert
# ---------------------------------------------------------------------------

class TestQualityCheckInsert:
    def test_quality_checks_inserted_with_entity_id(self):
        checks = [{"check_name": "not_empty", "status": "passed"}]
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="entity-uuid"), \
             patch("consumer.insert_quality_checks", return_value=[]) as mock_qc, \
             patch("consumer.insert_lineage_edge"):
            consumer.process_event(_entity_event(quality_checks=checks))
            assert mock_qc.call_args[1]["entity_id"] == "entity-uuid"
            assert mock_qc.call_args[1]["quality_checks"] == checks

    def test_no_quality_checks_means_empty_insert(self):
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="eid"), \
             patch("consumer.insert_quality_checks", return_value=[]) as mock_qc, \
             patch("consumer.insert_lineage_edge"):
            consumer.process_event(_entity_event(quality_checks=[]))
            mock_qc.assert_called_once()
            assert mock_qc.call_args[1]["quality_checks"] == []


# ---------------------------------------------------------------------------
# Section 4 — DataQualityFailed event publishing
# ---------------------------------------------------------------------------

class TestDataQualityFailedEvent:
    def test_no_event_published_when_all_checks_pass(self):
        checks = [
            {"check_name": "not_empty", "status": "passed"},
            {"check_name": "min_token_count", "status": "passed", "value": 100, "threshold": 50},
        ]
        producer = MagicMock()
        consumer, db, _ = _make_consumer(producer=producer)
        with patch("consumer.upsert_entity", return_value="eid"), \
             patch("consumer.insert_quality_checks", return_value=[]), \
             patch("consumer.insert_lineage_edge"), \
             patch("consumer.update_entity_quality_failed") as mock_update:
            consumer.process_event(_entity_event(quality_checks=checks))
            producer.produce.assert_not_called()
            mock_update.assert_not_called()

    def test_event_published_when_a_check_fails(self):
        failed_check = {"check_name": "min_token_count", "status": "failed", "value": 10, "threshold": 50}
        producer = MagicMock()
        consumer, db, _ = _make_consumer(producer=producer)
        with patch("consumer.upsert_entity", return_value="eid"), \
             patch("consumer.insert_quality_checks", return_value=[failed_check]), \
             patch("consumer.insert_lineage_edge"), \
             patch("consumer.update_entity_quality_failed"):
            consumer.process_event(_entity_event(quality_checks=[failed_check]))
            producer.produce.assert_called_once()
            payload = json.loads(producer.produce.call_args[1]["value"])
            assert payload["type"] == "data.quality.failed"
            assert payload["data"]["failed_checks"] == [failed_check]

    def test_event_published_to_data_quality_failed_topic(self):
        failed_check = {"check_name": "embedding_norm", "status": "failed", "value": 0.1, "threshold": 0.5}
        producer = MagicMock()
        consumer, db, _ = _make_consumer(producer=producer)
        with patch("consumer.upsert_entity", return_value="eid"), \
             patch("consumer.insert_quality_checks", return_value=[failed_check]), \
             patch("consumer.insert_lineage_edge"), \
             patch("consumer.update_entity_quality_failed"):
            consumer.process_event(_entity_event())
            assert producer.produce.call_args[0][0] == "data-quality-failed"

    def test_entity_quality_failed_updated_in_db(self):
        failed_check = {"check_name": "not_empty", "status": "failed"}
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity", return_value="entity-123"), \
             patch("consumer.insert_quality_checks", return_value=[failed_check]), \
             patch("consumer.insert_lineage_edge"), \
             patch("consumer.update_entity_quality_failed") as mock_update:
            consumer.process_event(_entity_event(quality_checks=[failed_check]))
            mock_update.assert_called_once_with(db, "entity-123")

    def test_dq_event_contains_entity_id_type_key_tenant(self):
        failed_check = {"check_name": "not_empty", "status": "failed"}
        producer = MagicMock()
        consumer, db, _ = _make_consumer(producer=producer)
        with patch("consumer.upsert_entity", return_value="entity-xyz"), \
             patch("consumer.insert_quality_checks", return_value=[failed_check]), \
             patch("consumer.insert_lineage_edge"), \
             patch("consumer.update_entity_quality_failed"):
            consumer.process_event(_entity_event(
                entity_type="DocumentChunk", entity_key="doc-1:3", tenant_id="acme",
                quality_checks=[failed_check],
            ))
            payload = json.loads(producer.produce.call_args[1]["value"])
            d = payload["data"]
            assert d["entity_id"] == "entity-xyz"
            assert d["entity_type"] == "DocumentChunk"
            assert d["entity_key"] == "doc-1:3"
            assert d["tenant_id"] == "acme"

    def test_dq_event_key_is_entity_key(self):
        failed_check = {"check_name": "not_empty", "status": "failed"}
        producer = MagicMock()
        consumer, db, _ = _make_consumer(producer=producer)
        with patch("consumer.upsert_entity", return_value="eid"), \
             patch("consumer.insert_quality_checks", return_value=[failed_check]), \
             patch("consumer.insert_lineage_edge"), \
             patch("consumer.update_entity_quality_failed"):
            consumer.process_event(_entity_event(entity_key="sha256:abc", quality_checks=[failed_check]))
            assert producer.produce.call_args[1]["key"] == b"sha256:abc"

    def test_dq_event_is_cloudevent(self):
        failed_check = {"check_name": "not_empty", "status": "failed"}
        producer = MagicMock()
        consumer, db, _ = _make_consumer(producer=producer)
        with patch("consumer.upsert_entity", return_value="eid"), \
             patch("consumer.insert_quality_checks", return_value=[failed_check]), \
             patch("consumer.insert_lineage_edge"), \
             patch("consumer.update_entity_quality_failed"):
            consumer.process_event(_entity_event())
            payload = json.loads(producer.produce.call_args[1]["value"])
            assert payload["specversion"] == "1.0"
            assert "id" in payload
            assert "time" in payload
            assert payload["source"] == "metadata-service"


# ---------------------------------------------------------------------------
# Section 5 — ignored event types
# ---------------------------------------------------------------------------

class TestIgnoredEvents:
    def test_unknown_event_type_not_processed(self):
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity") as mock_upsert:
            consumer.process_event({"type": "some.other.event", "data": {}})
            mock_upsert.assert_not_called()
            db.commit.assert_not_called()

    def test_missing_type_field_not_processed(self):
        consumer, db, producer = _make_consumer()
        with patch("consumer.upsert_entity") as mock_upsert:
            consumer.process_event({"data": {"entity_type": "DocumentChunk"}})
            mock_upsert.assert_not_called()


# ---------------------------------------------------------------------------
# Section 6 — DataQualityFailedPublisher unit tests
# ---------------------------------------------------------------------------

class TestDataQualityFailedPublisher:
    def _pub(self, producer=None):
        p = producer or MagicMock()
        return DataQualityFailedPublisher(producer=p, topic="data-quality-failed"), p

    def test_produce_called_once(self):
        pub, producer = self._pub()
        pub.publish(
            entity_id="eid",
            entity_type="DocumentChunk",
            entity_key="doc-1:0",
            tenant_id="t1",
            failed_checks=[{"check_name": "not_empty", "status": "failed"}],
        )
        assert producer.produce.call_count == 1
        assert producer.flush.call_count == 1

    def test_topic_is_data_quality_failed(self):
        pub, producer = self._pub()
        pub.publish("eid", "DocumentChunk", "doc-1:0", "t1", [{"check_name": "not_empty", "status": "failed"}])
        assert producer.produce.call_args[0][0] == "data-quality-failed"

    def test_event_type_is_data_quality_failed(self):
        pub, producer = self._pub()
        pub.publish("eid", "Embedding", "e-key", "t1", [{"check_name": "embedding_norm", "status": "failed"}])
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["type"] == "data.quality.failed"

    def test_subject_is_entity_type_slash_key(self):
        pub, producer = self._pub()
        pub.publish("eid", "DocumentChunk", "doc-1:3", "t1", [{"check_name": "x", "status": "failed"}])
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["subject"] == "DocumentChunk/doc-1:3"

    def test_producer_failure_is_swallowed(self):
        producer = MagicMock()
        producer.produce.side_effect = Exception("broker down")
        pub = DataQualityFailedPublisher(producer, "data-quality-failed")
        pub.publish("eid", "T", "k", "tenant", [])  # must not raise

    def test_failed_checks_included_in_data(self):
        pub, producer = self._pub()
        checks = [
            {"check_name": "not_empty", "status": "failed"},
            {"check_name": "min_token_count", "status": "failed", "value": 5, "threshold": 50},
        ]
        pub.publish("eid", "DocumentChunk", "doc-1:0", "t1", checks)
        data = json.loads(producer.produce.call_args[1]["value"])["data"]
        assert data["failed_checks"] == checks
        assert len(data["failed_checks"]) == 2


# ---------------------------------------------------------------------------
# Section 7 — RAG API metadata event publisher (metadata_event.py in rag-api)
# ---------------------------------------------------------------------------

class TestRagApiMetadataPublisher:
    """Test MetadataEventPublisher.publish_rag_query in the RAG API service."""

    def _setup(self):
        _rag_src = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "services", "rag-api", "src")
        )
        _rag_evict = {"metadata_event", "config", "events", "app", "circuit_breaker", "models"}
        for _m in _rag_evict:
            sys.modules.pop(_m, None)
        if _rag_src in sys.path:
            sys.path.remove(_rag_src)
        sys.path.insert(0, _rag_src)
        from metadata_event import MetadataEventPublisher
        return MetadataEventPublisher

    def test_produce_called_on_publish(self):
        Pub = self._setup()
        producer = MagicMock()
        pub = Pub(producer, "metadata-events")
        pub.publish_rag_query(
            query_id="q-1",
            tenant_id="acme",
            query_text="what is Paris?",
            top_k=5,
            source_filter=None,
            collection="acme_docs",
            latency_ms=42.0,
            cached=False,
            retrieved_chunks=[{"entity_key": "c1", "rank": 1, "score": 0.9}],
        )
        assert producer.produce.call_count == 1

    def test_topic_is_metadata_events(self):
        Pub = self._setup()
        producer = MagicMock()
        pub = Pub(producer, "metadata-events")
        pub.publish_rag_query(
            query_id="q-1", tenant_id="t", query_text="q", top_k=3,
            source_filter=None, collection="t_docs", latency_ms=10.0,
            cached=False, retrieved_chunks=[],
        )
        assert producer.produce.call_args[0][0] == "metadata-events"

    def test_entity_type_is_ragquery(self):
        Pub = self._setup()
        producer = MagicMock()
        pub = Pub(producer, "metadata-events")
        pub.publish_rag_query(
            query_id="q-1", tenant_id="t", query_text="q", top_k=3,
            source_filter=None, collection="t_docs", latency_ms=10.0,
            cached=False, retrieved_chunks=[],
        )
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["data"]["entity_type"] == "RAGQuery"

    def test_retrieved_chunks_in_data(self):
        Pub = self._setup()
        producer = MagicMock()
        pub = Pub(producer, "metadata-events")
        chunks = [{"entity_key": "c1", "rank": 1, "score": 0.9}]
        pub.publish_rag_query(
            query_id="q-1", tenant_id="t", query_text="q", top_k=3,
            source_filter=None, collection="t_docs", latency_ms=10.0,
            cached=True, retrieved_chunks=chunks,
        )
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["data"]["retrieved_chunks"] == chunks

    def test_cached_attribute_recorded(self):
        Pub = self._setup()
        producer = MagicMock()
        pub = Pub(producer, "metadata-events")
        pub.publish_rag_query(
            query_id="q-1", tenant_id="t", query_text="q", top_k=3,
            source_filter=None, collection="t_docs", latency_ms=10.0,
            cached=True, retrieved_chunks=[],
        )
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["data"]["attributes"]["cached"] is True

    def test_source_is_rag_api(self):
        Pub = self._setup()
        producer = MagicMock()
        pub = Pub(producer, "metadata-events")
        pub.publish_rag_query(
            query_id="q-1", tenant_id="t", query_text="q", top_k=3,
            source_filter=None, collection="t_docs", latency_ms=10.0,
            cached=False, retrieved_chunks=[],
        )
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["source"] == "rag-api"

    def test_subject_format(self):
        Pub = self._setup()
        producer = MagicMock()
        pub = Pub(producer, "metadata-events")
        pub.publish_rag_query(
            query_id="query-uuid-123", tenant_id="t", query_text="q", top_k=3,
            source_filter=None, collection="t_docs", latency_ms=10.0,
            cached=False, retrieved_chunks=[],
        )
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["subject"] == "RAGQuery/query-uuid-123"

    def test_producer_failure_is_swallowed(self):
        Pub = self._setup()
        producer = MagicMock()
        producer.produce.side_effect = Exception("broker down")
        pub = Pub(producer, "metadata-events")
        pub.publish_rag_query(
            query_id="q", tenant_id="t", query_text="q", top_k=3,
            source_filter=None, collection="t_docs", latency_ms=1.0,
            cached=False, retrieved_chunks=[],
        )  # must not raise
