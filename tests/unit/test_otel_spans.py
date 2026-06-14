"""
OTel span coverage tests for session 6-C.

connector-s3: kafka.produce span in _publish_with_retry
connector-nfs: see tests/unit/connectors/test_nfs_connector.py (TestNFSOTelSpans)
doc-processor: see tests/unit/processor/test_doc_processor.py (TestDocProcessorOTelSpans)
"""
import os
import sys

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "services", "connector-s3", "src"),
)

from connector import S3Connector
from events import RawDocumentEvent


@pytest.fixture
def span_exporter():
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


def test_s3_connector_kafka_produce_span(span_exporter):
    """S3Connector._publish_with_retry emits a kafka.produce span on success."""
    from config import Config

    os.environ.update({
        "CONNECTOR_ID": "s3-test",
        "TENANT_ID": "tenant-1",
        "KAFKA_TOPIC": "raw-documents",
        "KAFKA_PRODUCE_TIMEOUT_MS": "5000",
        "MINIO_BUCKET": "test-bucket",
        "FILE_TYPES": "application/pdf",
        "METADATA_EVENTS_TOPIC": "metadata-events",
    })
    cfg = Config()

    from unittest.mock import MagicMock
    producer = MagicMock()
    connector = S3Connector(
        minio_client=MagicMock(),
        kafka_producer=producer,
        redis_client=MagicMock(),
        db_conn=MagicMock(),
        cfg=cfg,
    )

    event = RawDocumentEvent(
        source_type="s3",
        source_id="bucket/doc.pdf",
        content_ref="s3://bucket/doc.pdf",
        content_type="application/pdf",
        tenant_id="tenant-1",
        metadata={},
    )
    connector._publish_with_retry(event)

    spans = span_exporter.get_finished_spans()
    assert any(s.name == "kafka.produce" for s in spans)
    span = next(s for s in spans if s.name == "kafka.produce")
    assert span.attributes.get("messaging.system") == "kafka"
    assert span.attributes.get("messaging.destination") == "raw-documents"
    assert span.attributes.get("messaging.operation") == "publish"
