import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "services", "connector-nfs", "src"
    ),
)

from connector import NFSConnector, _KAFKA_MAX_RETRIES, _known_files_key
from events import RawDocumentEvent
from config import Config


def _make_nfs_cfg(**overrides):
    cfg = Config()
    cfg.connector_id = overrides.get("connector_id", "nfs-test")
    cfg.tenant_id = overrides.get("tenant_id", "tenant-1")
    cfg.kafka_topic = overrides.get("kafka_topic", "raw-documents")
    cfg.kafka_produce_timeout_ms = overrides.get("kafka_produce_timeout_ms", 5000)
    if "allowed_extensions" in overrides:
        os.environ["ALLOWED_EXTENSIONS"] = ",".join(overrides["allowed_extensions"])
    else:
        os.environ["ALLOWED_EXTENSIONS"] = ".pdf,.txt,.html,.json,.csv"
    return cfg


def _make_nfs_connector(
    mount_path,
    observer_alive=True,
    known_files=None,
    kafka_side_effect=None,
    cfg=None,
):
    producer = MagicMock()
    if kafka_side_effect is not None:
        producer.flush.side_effect = kafka_side_effect

    redis = MagicMock()
    redis.smembers.return_value = known_files if known_files is not None else set()

    db_conn = MagicMock()
    cursor = MagicMock()
    db_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    db_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    cfg = cfg or _make_nfs_cfg()

    with patch("connector.Observer") as mock_obs_class:
        mock_observer = MagicMock()
        mock_observer.is_alive.return_value = observer_alive
        mock_obs_class.return_value = mock_observer
        connector = NFSConnector(mount_path, producer, redis, db_conn, cfg)

    return connector, producer, redis, db_conn


class TestNFSInotifyEmitsEvent:
    def test_nfs_connector_inotify_emits_event_on_new_file(self, tmp_path):
        test_file = tmp_path / "doc.pdf"
        test_file.write_bytes(b"%PDF test content")

        connector, producer, redis, db = _make_nfs_connector(
            mount_path=str(tmp_path), observer_alive=True
        )
        connector._file_queue.put(str(test_file))

        events = list(connector.poll())

        assert len(events) == 1
        assert events[0].source_type == "nfs"
        assert "doc.pdf" in events[0].source_id
        assert events[0].tenant_id == "tenant-1"
        producer.produce.assert_called_once()
        redis.sadd.assert_called_once_with(
            _known_files_key("nfs-test"), str(test_file)
        )


class TestNFSTreeDiffFallback:
    def test_nfs_connector_tree_diff_fallback_finds_new_files(self, tmp_path):
        test_file = tmp_path / "report.txt"
        test_file.write_text("report content")

        connector, producer, redis, db = _make_nfs_connector(
            mount_path=str(tmp_path),
            observer_alive=False,
            known_files=set(),
        )

        events = list(connector.poll())

        assert len(events) == 1
        assert events[0].source_type == "nfs"
        redis.sadd.assert_called_once()

    def test_nfs_connector_tree_diff_skips_known_files(self, tmp_path):
        test_file = tmp_path / "report.txt"
        test_file.write_text("already seen")

        known = {str(test_file).encode()}
        connector, producer, redis, db = _make_nfs_connector(
            mount_path=str(tmp_path),
            observer_alive=False,
            known_files=known,
        )

        events = list(connector.poll())

        assert events == []
        producer.produce.assert_not_called()

    def test_nfs_connector_tree_diff_skips_disallowed_extension(self, tmp_path):
        zip_file = tmp_path / "archive.zip"
        zip_file.write_bytes(b"PK zip content")

        connector, producer, redis, db = _make_nfs_connector(
            mount_path=str(tmp_path),
            observer_alive=False,
            known_files=set(),
        )

        events = list(connector.poll())

        assert events == []
        producer.produce.assert_not_called()


class TestNFSSkipsDisallowedExtension:
    def test_nfs_connector_skips_disallowed_extension_from_queue(self, tmp_path):
        cfg = _make_nfs_cfg(allowed_extensions=[".pdf", ".txt"])
        zip_file = tmp_path / "archive.zip"
        zip_file.write_bytes(b"PK zip content")

        connector, producer, redis, db = _make_nfs_connector(
            mount_path=str(tmp_path), observer_alive=True, cfg=cfg
        )
        connector._file_queue.put(str(zip_file))

        events = list(connector.poll())

        assert events == []
        producer.produce.assert_not_called()


class TestNFSKafkaRetry:
    def test_nfs_connector_retries_kafka_on_timeout(self, tmp_path):
        from confluent_kafka import KafkaException

        test_file = tmp_path / "doc.pdf"
        test_file.write_bytes(b"%PDF test")

        call_count = 0

        def flush_side_effect(timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise KafkaException("timeout")

        connector, producer, redis, db = _make_nfs_connector(
            mount_path=str(tmp_path),
            observer_alive=True,
            kafka_side_effect=flush_side_effect,
        )
        connector._file_queue.put(str(test_file))

        with patch("connector.time.sleep"):
            events = list(connector.poll())

        assert len(events) == 1
        assert producer.flush.call_count == 3


class TestNFSSkipsAfterMaxRetries:
    def test_nfs_connector_skips_after_max_retries(self, tmp_path):
        from confluent_kafka import KafkaException

        test_file = tmp_path / "doc.pdf"
        test_file.write_bytes(b"%PDF test")

        connector, producer, redis, db = _make_nfs_connector(
            mount_path=str(tmp_path),
            observer_alive=True,
            kafka_side_effect=KafkaException("timeout"),
        )
        connector._file_queue.put(str(test_file))

        with patch("connector.time.sleep"), patch(
            "connector.connector_errors_total"
        ) as mock_counter:
            events = list(connector.poll())

        assert events == []
        assert producer.flush.call_count == _KAFKA_MAX_RETRIES
        mock_counter.labels.assert_called_once_with(reason="kafka_timeout")
        mock_counter.labels.return_value.inc.assert_called_once()
        redis.sadd.assert_not_called()


class TestNFSEventSchema:
    def test_nfs_connector_event_schema_valid(self, tmp_path):
        test_file = tmp_path / "report.pdf"
        test_file.write_bytes(b"%PDF content")

        connector, producer, redis, db = _make_nfs_connector(
            mount_path=str(tmp_path), observer_alive=True
        )
        connector._file_queue.put(str(test_file))

        events = list(connector.poll())
        assert len(events) == 1
        event = events[0]

        d = event.to_dict()
        assert d["source_type"] == "nfs"
        assert isinstance(d["event_id"], str) and len(d["event_id"]) == 36
        assert isinstance(d["ingested_at"], float)
        assert isinstance(d["metadata"], dict)

        serialised = event.to_json()
        parsed = json.loads(serialised)
        assert parsed["source_type"] == "nfs"
        assert "event_id" in parsed

        roundtrip = RawDocumentEvent.from_json(serialised)
        assert roundtrip.event_id == event.event_id
        assert roundtrip.source_type == "nfs"


class TestNFSMultipleFiles:
    def test_nfs_connector_emits_multiple_events(self, tmp_path):
        files = [tmp_path / f"file{i}.txt" for i in range(3)]
        for f in files:
            f.write_text(f"content of {f.name}")

        connector, producer, redis, db = _make_nfs_connector(
            mount_path=str(tmp_path), observer_alive=True
        )
        for f in files:
            connector._file_queue.put(str(f))

        events = list(connector.poll())

        assert len(events) == 3
        assert producer.produce.call_count == 3
        assert redis.sadd.call_count == 3


class TestNFSOTelSpans:
    @pytest.fixture
    def span_exporter(self):
        """Provide InMemorySpanExporter; patch connector module's _tracer directly."""
        import connector as conn_mod
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        test_tracer = provider.get_tracer("test")
        original_tracer = conn_mod._tracer
        conn_mod._tracer = test_tracer
        yield exporter
        conn_mod._tracer = original_tracer
        exporter.clear()

    def test_publish_with_retry_emits_kafka_produce_span(self, span_exporter, tmp_path):
        """_publish_with_retry emits a kafka.produce span on every successful publish."""
        test_file = tmp_path / "doc.pdf"
        test_file.write_bytes(b"%PDF test")
        connector, producer, redis, db = _make_nfs_connector(
            mount_path=str(tmp_path), observer_alive=True
        )
        connector._file_queue.put(str(test_file))
        list(connector.poll())

        span_names = [s.name for s in span_exporter.get_finished_spans()]
        assert "kafka.produce" in span_names
        span = next(s for s in span_exporter.get_finished_spans() if s.name == "kafka.produce")
        assert span.attributes.get("messaging.system") == "kafka"
        assert span.attributes.get("messaging.destination") == "raw-documents"
        assert span.attributes.get("messaging.operation") == "publish"
