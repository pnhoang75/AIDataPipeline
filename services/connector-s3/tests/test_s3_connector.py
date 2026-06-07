import json
import sys
import os
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from events import RawDocumentEvent
from config import Config
from connector import S3Connector, _KAFKA_MAX_RETRIES


def _make_obj(name: str, last_modified: datetime = None, etag: str = "abc123", size: int = 1024):
    obj = SimpleNamespace(
        object_name=name,
        last_modified=last_modified or datetime(2024, 1, 1, tzinfo=timezone.utc),
        etag=etag,
        size=size,
    )
    return obj


def _make_cfg(**overrides):
    cfg = Config()
    cfg.connector_id = overrides.get("connector_id", "test-connector")
    cfg.tenant_id = overrides.get("tenant_id", "tenant-1")
    cfg.minio_bucket = overrides.get("minio_bucket", "docs")
    cfg.kafka_topic = overrides.get("kafka_topic", "raw-documents")
    cfg.kafka_produce_timeout_ms = overrides.get("kafka_produce_timeout_ms", 5000)
    if "file_types" in overrides:
        os.environ["FILE_TYPES"] = ",".join(overrides["file_types"])
    else:
        os.environ["FILE_TYPES"] = "application/pdf,text/plain"
    return cfg


def _make_connector(objects=None, kafka_side_effect=None, cfg=None, watermark=None):
    minio = MagicMock()
    minio.list_objects.return_value = objects or []

    producer = MagicMock()
    if kafka_side_effect is not None:
        producer.flush.side_effect = kafka_side_effect
    else:
        producer.flush.return_value = None

    redis = MagicMock()
    redis.hget.return_value = watermark.isoformat().encode() if watermark else None

    db_conn = MagicMock()
    cursor = MagicMock()
    db_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    db_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    cfg = cfg or _make_cfg()
    connector = S3Connector(minio, producer, redis, db_conn, cfg)
    return connector, minio, producer, redis, db_conn


class TestWatermarkNotAdvancedOnPublishFailure:
    def test_s3_connector_watermark_not_advanced_on_publish_failure(self):
        from confluent_kafka import KafkaException
        obj = _make_obj("report.pdf", datetime(2024, 6, 1, tzinfo=timezone.utc))
        connector, minio, producer, redis, db = _make_connector(
            objects=[obj],
            kafka_side_effect=KafkaException("timeout"),
        )

        with patch("connector.time.sleep"):
            events = list(connector.poll())

        assert events == []
        redis.hset.assert_not_called()


class TestSkipsInvalidContentType:
    def test_s3_connector_skips_file_on_invalid_content_type(self):
        obj = _make_obj("archive.zip", datetime(2024, 6, 1, tzinfo=timezone.utc))
        cfg = _make_cfg(file_types=["application/pdf", "text/plain"])
        connector, minio, producer, redis, db = _make_connector(objects=[obj], cfg=cfg)

        with patch("connector.time.sleep"):
            events = list(connector.poll())

        assert events == []
        producer.produce.assert_not_called()

        cursor = db.cursor.return_value.__enter__.return_value
        status_call = cursor.execute.call_args
        assert status_call is not None
        sql, args = status_call[0]
        assert "source_file_status" in sql
        assert args[3] == "error"


class TestEventSchemaValid:
    def test_connector_event_schema_valid(self):
        event = RawDocumentEvent(
            source_type="s3",
            source_id="docs/report.pdf",
            content_ref="s3://docs/report.pdf",
            content_type="application/pdf",
            tenant_id="tenant-1",
            metadata={"title": "Report"},
        )
        d = event.to_dict()
        assert d["source_type"] == "s3"
        assert d["source_id"] == "docs/report.pdf"
        assert d["content_ref"] == "s3://docs/report.pdf"
        assert d["content_type"] == "application/pdf"
        assert d["tenant_id"] == "tenant-1"
        assert isinstance(d["event_id"], str) and len(d["event_id"]) == 36
        assert isinstance(d["ingested_at"], float)
        assert isinstance(d["metadata"], dict)

        serialised = event.to_json()
        parsed = json.loads(serialised)
        assert parsed["source_type"] == "s3"
        assert "event_id" in parsed

        roundtrip = RawDocumentEvent.from_json(serialised)
        assert roundtrip.event_id == event.event_id
        assert roundtrip.content_type == event.content_type


class TestRetriesKafkaOnTimeout:
    def test_connector_retries_kafka_on_timeout(self):
        from confluent_kafka import KafkaException
        obj = _make_obj("doc.pdf", datetime(2024, 6, 1, tzinfo=timezone.utc))
        connector, minio, producer, redis, db = _make_connector(objects=[obj])

        call_count = 0

        def flush_side_effect(timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise KafkaException("timeout")

        producer.flush.side_effect = flush_side_effect

        with patch("connector.time.sleep"):
            events = list(connector.poll())

        assert len(events) == 1
        assert producer.flush.call_count == 3


class TestSkipsAfterMaxRetries:
    def test_connector_skips_after_max_retries(self):
        from confluent_kafka import KafkaException
        obj = _make_obj("doc.pdf", datetime(2024, 6, 1, tzinfo=timezone.utc))
        connector, minio, producer, redis, db = _make_connector(
            objects=[obj],
            kafka_side_effect=KafkaException("timeout"),
        )

        with patch("connector.time.sleep"), \
             patch("connector.connector_errors_total") as mock_counter:
            events = list(connector.poll())

        assert events == []
        assert producer.flush.call_count == _KAFKA_MAX_RETRIES
        mock_counter.labels.assert_called_once_with(reason="kafka_timeout")
        mock_counter.labels.return_value.inc.assert_called_once()
        redis.hset.assert_not_called()
