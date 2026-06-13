"""
Unit tests for metadata.entity.created CloudEvent publishing in S3 and NFS connectors.
Covers:
  - build_cloudevent() structure
  - MetadataEventPublisher.publish_datasource()
  - MetadataEventPublisher.publish_rawdocument() + upstream lineage edge
  - S3Connector publishes DataSource on init, RawDocument on file discovery
  - NFSConnector publishes DataSource on init, RawDocument on file discovery
  - Metadata publish failure is swallowed (doesn't stop ingestion)
"""

import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_BASE = os.path.dirname(os.path.abspath(__file__))
S3_SRC = os.path.abspath(os.path.join(_BASE, "..", "..", "..", "services", "connector-s3", "src"))
NFS_SRC = os.path.abspath(os.path.join(_BASE, "..", "..", "..", "services", "connector-nfs", "src"))

sys.path.insert(0, S3_SRC)

# S3 module-level imports — these also cache bare names in sys.modules.
from metadata_event import MetadataEventPublisher, build_cloudevent  # noqa: E402
from connector import S3Connector  # noqa: E402
from config import Config as S3Config  # noqa: E402

# Remove bare module names from sys.modules so that test_nfs_connector.py (collected
# in the same pytest session) can import NFS versions from NFS_SRC without a cache hit.
# The class/function objects above keep the S3 module objects alive via __globals__.
for _m in ("connector", "config", "events", "status", "watermark", "metadata_event"):
    sys.modules.pop(_m, None)
del _m

# ---------------------------------------------------------------------------
# NFS lazy-loader
# ---------------------------------------------------------------------------
_NFS_MOD_NAMES = frozenset(
    {"config", "events", "status", "metadata_event", "connector"}
)

_nfs_connector_class = None
_nfs_connector_module = None


def _load_nfs():
    """Return (NFSConnector class, connector module) loaded from NFS src — once only.

    Patches prometheus_client.Counter during module exec to prevent the duplicate
    metric registration error (both connectors register 'connector_errors_total').
    """
    global _nfs_connector_class, _nfs_connector_module
    if _nfs_connector_class is not None:
        return _nfs_connector_class, _nfs_connector_module

    # Evict any previously loaded bare module names so NFS versions load fresh.
    saved = {k: sys.modules.pop(k) for k in _NFS_MOD_NAMES if k in sys.modules}

    sys.path.insert(0, NFS_SRC)
    try:
        with patch("prometheus_client.Counter"):
            import connector as _nfs_mod  # noqa: PLC0415

        _nfs_connector_class = _nfs_mod.NFSConnector
        _nfs_connector_module = _nfs_mod
    finally:
        sys.path.remove(NFS_SRC)
        for k in _NFS_MOD_NAMES:
            sys.modules.pop(k, None)
        sys.modules.update(saved)

    return _nfs_connector_class, _nfs_connector_module


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_s3_cfg(**overrides):
    cfg = S3Config()
    cfg.connector_id = overrides.get("connector_id", "s3-test")
    cfg.tenant_id = overrides.get("tenant_id", "tenant-1")
    cfg.minio_bucket = overrides.get("minio_bucket", "docs")
    cfg.kafka_topic = overrides.get("kafka_topic", "raw-documents")
    cfg.kafka_produce_timeout_ms = 5000
    cfg.metadata_events_topic = overrides.get("metadata_events_topic", "metadata-events")
    os.environ["FILE_TYPES"] = "application/pdf,text/plain"
    return cfg


def _make_s3_connector(objects=None, cfg=None, metadata_producer=None):
    minio = MagicMock()
    minio.list_objects.return_value = objects or []

    producer = MagicMock()
    producer.flush.return_value = None

    redis = MagicMock()
    redis.hget.return_value = None

    db_conn = MagicMock()
    cursor = MagicMock()
    db_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    db_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    cfg = cfg or _make_s3_cfg()
    connector = S3Connector(minio, producer, redis, db_conn, cfg, metadata_producer=metadata_producer)
    return connector, producer, redis


def _make_obj(name, last_modified=None, etag="etag123", size=2048):
    return SimpleNamespace(
        object_name=name,
        last_modified=last_modified or datetime(2024, 1, 1, tzinfo=timezone.utc),
        etag=etag,
        size=size,
    )


def _make_nfs_cfg(**overrides):
    """Build an NFS Config instance using the NFS Config class from the loaded module."""
    _, nfs_module = _load_nfs()
    # NFSConnector.py binds `Config` in its namespace via `from config import Config`
    cfg = nfs_module.Config()
    cfg.connector_id = overrides.get("connector_id", "nfs-test")
    cfg.tenant_id = overrides.get("tenant_id", "tenant-1")
    cfg.kafka_topic = overrides.get("kafka_topic", "raw-documents")
    cfg.kafka_produce_timeout_ms = 5000
    cfg.metadata_events_topic = overrides.get("metadata_events_topic", "metadata-events")
    os.environ["ALLOWED_EXTENSIONS"] = ".pdf,.txt,.html,.json,.csv"
    return cfg


def _make_nfs_connector(mount_path, cfg=None, metadata_producer=None):
    NFSConnector, nfs_module = _load_nfs()

    producer = MagicMock()
    redis = MagicMock()
    redis.smembers.return_value = set()

    db_conn = MagicMock()
    cursor = MagicMock()
    db_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    db_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    cfg = cfg or _make_nfs_cfg()

    with patch.object(nfs_module, "Observer") as mock_obs_cls:
        mock_observer = MagicMock()
        mock_observer.is_alive.return_value = True
        mock_obs_cls.return_value = mock_observer
        connector = NFSConnector(
            mount_path, producer, redis, db_conn, cfg, metadata_producer=metadata_producer
        )

    return connector, producer, redis


# ===========================================================================
# Section 1 — build_cloudevent() structure
# ===========================================================================
class TestBuildCloudevent:
    def test_required_fields_present(self):
        evt = build_cloudevent(
            event_type="metadata.entity.created",
            source="connector/test",
            subject="DataSource/tenant-1/s3/docs",
            data={"entity_type": "DataSource"},
        )
        assert evt["specversion"] == "1.0"
        assert evt["type"] == "metadata.entity.created"
        assert evt["source"] == "connector/test"
        assert evt["subject"] == "DataSource/tenant-1/s3/docs"
        assert evt["datacontenttype"] == "application/json"
        assert "id" in evt
        assert "time" in evt
        assert isinstance(evt["data"], dict)

    def test_id_is_unique_uuid(self):
        e1 = build_cloudevent("t", "s", "sub", {})
        e2 = build_cloudevent("t", "s", "sub", {})
        assert e1["id"] != e2["id"]
        assert len(e1["id"]) == 36

    def test_time_is_iso8601(self):
        evt = build_cloudevent("t", "s", "sub", {})
        dt = datetime.fromisoformat(evt["time"])
        assert dt.tzinfo is not None


# ===========================================================================
# Section 2 — MetadataEventPublisher — DataSource events
# ===========================================================================
class TestPublishDataSource:
    def _pub(self, producer=None):
        p = producer or MagicMock()
        return MetadataEventPublisher(
            producer=p,
            topic="metadata-events",
            connector_id="conn-1",
            tenant_id="tenant-1",
        ), p

    def test_produce_called_once(self):
        pub, producer = self._pub()
        pub.publish_datasource("tenant-1/s3/docs", {"source_type": "s3"})
        assert producer.produce.call_count == 1
        assert producer.flush.call_count == 1

    def test_topic_is_metadata_events(self):
        pub, producer = self._pub()
        pub.publish_datasource("tenant-1/s3/docs", {})
        topic = producer.produce.call_args[0][0]
        assert topic == "metadata-events"

    def test_payload_entity_type_and_tenant(self):
        pub, producer = self._pub()
        pub.publish_datasource("tenant-1/s3/docs", {"source_type": "s3"})
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["type"] == "metadata.entity.created"
        data = payload["data"]
        assert data["entity_type"] == "DataSource"
        assert data["tenant_id"] == "tenant-1"
        assert data["entity_key"] == "tenant-1/s3/docs"
        assert data["attributes"]["source_type"] == "s3"

    def test_subject_format(self):
        pub, producer = self._pub()
        pub.publish_datasource("tenant-1/s3/docs", {})
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["subject"] == "DataSource/tenant-1/s3/docs"

    def test_source_contains_connector_id(self):
        pub, producer = self._pub()
        pub.publish_datasource("key", {})
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["source"] == "connector/conn-1"

    def test_publish_failure_is_swallowed(self):
        producer = MagicMock()
        producer.produce.side_effect = Exception("broker down")
        pub, _ = self._pub(producer)
        pub.publish_datasource("key", {})  # must not raise


# ===========================================================================
# Section 3 — MetadataEventPublisher — RawDocument + discovered_in edge
# ===========================================================================
class TestPublishRawDocument:
    def _pub(self):
        producer = MagicMock()
        pub = MetadataEventPublisher(
            producer=producer,
            topic="metadata-events",
            connector_id="conn-1",
            tenant_id="tenant-1",
        )
        return pub, producer

    def test_entity_type_is_rawdocument(self):
        pub, producer = self._pub()
        pub.publish_rawdocument("s3://docs/r.pdf", {"content_type": "application/pdf"}, "ds-key")
        data = json.loads(producer.produce.call_args[1]["value"])["data"]
        assert data["entity_type"] == "RawDocument"

    def test_upstream_has_discovered_in_edge(self):
        pub, producer = self._pub()
        pub.publish_rawdocument("s3://docs/r.pdf", {}, "tenant-1/s3/docs")
        upstream = json.loads(producer.produce.call_args[1]["value"])["data"]["upstream"]
        assert len(upstream) == 1
        assert upstream[0]["entity_type"] == "DataSource"
        assert upstream[0]["entity_key"] == "tenant-1/s3/docs"
        assert upstream[0]["relationship"] == "discovered_in"

    def test_subject_format(self):
        pub, producer = self._pub()
        pub.publish_rawdocument("s3://docs/r.pdf", {}, "ds")
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["subject"] == "RawDocument/s3://docs/r.pdf"

    def test_attributes_passed_through(self):
        pub, producer = self._pub()
        pub.publish_rawdocument("key", {"content_type": "application/pdf", "size_bytes": 1024}, "ds")
        attrs = json.loads(producer.produce.call_args[1]["value"])["data"]["attributes"]
        assert attrs["content_type"] == "application/pdf"
        assert attrs["size_bytes"] == 1024

    def test_flush_failure_is_swallowed(self):
        producer = MagicMock()
        producer.flush.side_effect = Exception("timeout")
        pub = MetadataEventPublisher(producer, "metadata-events", "c", "t")
        pub.publish_rawdocument("key", {}, "ds-key")  # must not raise


# ===========================================================================
# Section 4 — S3Connector metadata events
# ===========================================================================
class TestS3ConnectorMetadataEvents:
    def test_datasource_event_published_on_init(self):
        meta = MagicMock()
        _make_s3_connector(metadata_producer=meta)
        assert meta.produce.call_count == 1
        payload = json.loads(meta.produce.call_args[1]["value"])
        assert payload["data"]["entity_type"] == "DataSource"
        assert payload["data"]["attributes"]["source_type"] == "s3"

    def test_datasource_entity_key_format(self):
        meta = MagicMock()
        cfg = _make_s3_cfg(tenant_id="acme", minio_bucket="acme-docs")
        _make_s3_connector(cfg=cfg, metadata_producer=meta)
        payload = json.loads(meta.produce.call_args[1]["value"])
        assert payload["data"]["entity_key"] == "acme/s3/acme-docs"

    def test_no_publisher_when_metadata_producer_is_none(self):
        connector, _, _ = _make_s3_connector(metadata_producer=None)
        assert connector._metadata_publisher is None

    def test_rawdocument_event_published_per_discovered_file(self):
        meta = MagicMock()
        obj = _make_obj("report.pdf", size=4096, etag="etag-abc")
        connector, _, _ = _make_s3_connector(objects=[obj], metadata_producer=meta)
        meta.reset_mock()
        list(connector.poll())

        assert meta.produce.call_count >= 1
        payload = json.loads(meta.produce.call_args[1]["value"])
        assert payload["data"]["entity_type"] == "RawDocument"

    def test_rawdocument_upstream_links_to_datasource(self):
        meta = MagicMock()
        obj = _make_obj("doc.pdf")
        cfg = _make_s3_cfg(tenant_id="t1", minio_bucket="b1")
        connector, _, _ = _make_s3_connector(objects=[obj], cfg=cfg, metadata_producer=meta)
        meta.reset_mock()
        list(connector.poll())

        upstream = json.loads(meta.produce.call_args[1]["value"])["data"]["upstream"]
        assert upstream[0]["relationship"] == "discovered_in"
        assert upstream[0]["entity_key"] == "t1/s3/b1"

    def test_rawdocument_entity_key_is_s3_uri(self):
        meta = MagicMock()
        obj = _make_obj("sub/file.pdf")
        cfg = _make_s3_cfg(minio_bucket="mybucket")
        connector, _, _ = _make_s3_connector(objects=[obj], cfg=cfg, metadata_producer=meta)
        meta.reset_mock()
        list(connector.poll())

        entity_key = json.loads(meta.produce.call_args[1]["value"])["data"]["entity_key"]
        assert entity_key == "s3://mybucket/sub/file.pdf"

    def test_metadata_publish_failure_does_not_stop_ingestion(self):
        meta = MagicMock()
        meta.flush.side_effect = Exception("broker unavailable")
        obj = _make_obj("doc.pdf")
        connector, raw_producer, _ = _make_s3_connector(objects=[obj], metadata_producer=meta)
        raw_producer.flush.side_effect = None

        events = list(connector.poll())
        assert len(events) == 1

    def test_rawdocument_not_published_when_raw_kafka_fails(self):
        """RawDocument metadata event must NOT be published if Kafka ingestion fails."""
        from confluent_kafka import KafkaException

        meta = MagicMock()
        obj = _make_obj("doc.pdf")
        connector, raw_producer, _ = _make_s3_connector(objects=[obj], metadata_producer=meta)
        raw_producer.flush.side_effect = KafkaException("timeout")

        meta.reset_mock()
        with patch("time.sleep"):
            events = list(connector.poll())

        assert events == []
        assert meta.produce.call_count == 0  # no RawDocument event after reset


# ===========================================================================
# Section 5 — NFSConnector metadata events
# ===========================================================================
class TestNFSConnectorMetadataEvents:
    def test_datasource_event_published_on_init(self, tmp_path):
        meta = MagicMock()
        _make_nfs_connector(str(tmp_path), metadata_producer=meta)
        assert meta.produce.call_count == 1
        payload = json.loads(meta.produce.call_args[1]["value"])
        assert payload["data"]["entity_type"] == "DataSource"
        assert payload["data"]["attributes"]["source_type"] == "nfs"

    def test_datasource_entity_key_includes_mount_path(self, tmp_path):
        meta = MagicMock()
        cfg = _make_nfs_cfg(tenant_id="acme")
        _make_nfs_connector(str(tmp_path), cfg=cfg, metadata_producer=meta)
        payload = json.loads(meta.produce.call_args[1]["value"])
        assert payload["data"]["entity_key"] == f"acme/nfs/{tmp_path}"

    def test_no_publisher_when_metadata_producer_is_none(self, tmp_path):
        connector, _, _ = _make_nfs_connector(str(tmp_path), metadata_producer=None)
        assert connector._metadata_publisher is None

    def test_rawdocument_event_published_per_discovered_file(self, tmp_path):
        test_file = tmp_path / "doc.pdf"
        test_file.write_bytes(b"%PDF content")

        meta = MagicMock()
        connector, _, _ = _make_nfs_connector(str(tmp_path), metadata_producer=meta)
        connector._file_queue.put(str(test_file))

        meta.reset_mock()
        events = list(connector.poll())

        assert len(events) == 1
        assert meta.produce.call_count >= 1
        payload = json.loads(meta.produce.call_args[1]["value"])
        assert payload["data"]["entity_type"] == "RawDocument"

    def test_rawdocument_upstream_links_to_datasource(self, tmp_path):
        test_file = tmp_path / "report.txt"
        test_file.write_text("content")

        meta = MagicMock()
        cfg = _make_nfs_cfg(tenant_id="t1")
        connector, _, _ = _make_nfs_connector(str(tmp_path), cfg=cfg, metadata_producer=meta)
        connector._file_queue.put(str(test_file))

        meta.reset_mock()
        list(connector.poll())

        upstream = json.loads(meta.produce.call_args[1]["value"])["data"]["upstream"]
        assert upstream[0]["relationship"] == "discovered_in"
        assert upstream[0]["entity_type"] == "DataSource"

    def test_metadata_publish_failure_does_not_stop_ingestion(self, tmp_path):
        test_file = tmp_path / "doc.pdf"
        test_file.write_bytes(b"%PDF content")

        meta = MagicMock()
        meta.flush.side_effect = Exception("broker down")
        connector, raw_producer, _ = _make_nfs_connector(str(tmp_path), metadata_producer=meta)
        raw_producer.flush.side_effect = None
        connector._file_queue.put(str(test_file))

        events = list(connector.poll())
        assert len(events) == 1
