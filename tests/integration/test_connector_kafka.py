"""Integration tests: S3Connector → Kafka raw-documents → DocumentProcessor pipeline.

Three scenarios:
  1. Happy-path PDF  — valid PDF uploaded to MinIO → connector publishes event →
                        processor produces chunks on document-chunks topic.
  2. Corrupt PDF→DLQ — corrupt bytes with .pdf extension → processor parse error →
                        DLQ entry on dlq-raw-documents topic.
  3. Watermark dedup — same file polled twice → second poll emits no events because
                        the watermark already covers the file's last_modified timestamp.

Requires Docker (testcontainers spins up Kafka, MinIO, Redis, PostgreSQL).
"""

from __future__ import annotations

import io
import json
import os
import sys
import threading
import time
from pathlib import Path

import psycopg2
import pytest

# ── Testcontainers (skip whole module if not installed) ─────────────────────────
testcontainers = pytest.importorskip("testcontainers", reason="testcontainers not installed")

from testcontainers.kafka import KafkaContainer  # noqa: E402
from testcontainers.minio import MinioContainer  # noqa: E402
from testcontainers.postgres import PostgresContainer  # noqa: E402
from testcontainers.redis import RedisContainer  # noqa: E402

from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402
from minio import Minio  # noqa: E402
import redis as redis_lib  # noqa: E402

# ── Service source paths ────────────────────────────────────────────────────────
_ROOT = Path(__file__).parent.parent.parent
_CONN_SRC = str(_ROOT / "services" / "connector-s3" / "src")
_PROC_SRC = str(_ROOT / "services" / "doc-processor" / "src")

# ── Import connector-s3 modules ─────────────────────────────────────────────────
# Add connector-s3/src first so its module-level imports resolve correctly.
sys.path.insert(0, _CONN_SRC)
for _m in ["config", "events", "status", "watermark", "connector"]:
    sys.modules.pop(_m, None)

from connector import S3Connector  # noqa: E402
import config as _conn_cfg_mod  # noqa: E402
import watermark as _watermark_mod  # noqa: E402

ConnConfig = _conn_cfg_mod.Config
_get_watermark = _watermark_mod.get_watermark

# ── Import doc-processor modules ────────────────────────────────────────────────
# Put proc src at the front and clear cached module names so imports reload fresh.
sys.path.insert(0, _PROC_SRC)
for _m in ["config", "events", "status", "chunker", "parsers", "processor", "fetcher"]:
    sys.modules.pop(_m, None)

from processor import DocumentProcessor  # noqa: E402
import config as _proc_cfg_mod  # noqa: E402
import events as _proc_events_mod  # noqa: E402

ProcConfig = _proc_cfg_mod.Config
DLQEnvelope = _proc_events_mod.DLQEnvelope

# Ensure PDF and plain text are accepted by the connector's file-type allowlist.
os.environ.setdefault("FILE_TYPES", "application/pdf,text/plain")

# ── Kafka topic names ───────────────────────────────────────────────────────────
TOPIC_RAW = "raw-documents"
TOPIC_CHUNKS = "document-chunks"
TOPIC_DLQ = "dlq-raw-documents"
TOPICS = [TOPIC_RAW, TOPIC_CHUNKS, TOPIC_DLQ]


# ── PDF generation helper ───────────────────────────────────────────────────────

def _make_pdf_bytes() -> bytes:
    from fpdf import FPDF
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=12)
    pdf.cell(200, 10, text="Integration test document. This text will be chunked by the processor.")
    return bytes(pdf.output())


# ── Kafka helpers ───────────────────────────────────────────────────────────────

def _create_topics(bootstrap: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics(
        [NewTopic(t, num_partitions=1, replication_factor=1) for t in TOPICS]
    )
    for _, fut in futures.items():
        try:
            fut.result(timeout=30)
        except Exception:
            pass  # Already exists is fine


def _get_high_watermark(bootstrap: str, topic: str) -> int:
    """Return the current end offset (high-water mark) for partition 0."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"hwm-probe-{topic}-{time.time()}",
    })
    try:
        tp = TopicPartition(topic, 0)
        lo, hi = consumer.get_watermark_offsets(tp, timeout=10)
        return hi
    finally:
        consumer.close()


def _drain_from_offset(bootstrap: str, topic: str, start_offset: int, n: int, timeout_s: float = 30) -> list:
    """Consume up to n messages from topic starting at start_offset; return decoded JSON list."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"drain-{topic}-{start_offset}-{time.time()}",
        "auto.offset.reset": "earliest",
    })
    tp = TopicPartition(topic, 0, start_offset)
    consumer.assign([tp])
    messages: list = []
    deadline = time.time() + timeout_s
    try:
        while len(messages) < n and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            messages.append(json.loads(msg.value()))
    finally:
        consumer.close()
    return messages


# ── PostgreSQL schema ───────────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS source_file_status (
    connector_id  VARCHAR      NOT NULL,
    source_id     VARCHAR      NOT NULL,
    event_id      VARCHAR      NOT NULL DEFAULT '',
    ingest_status VARCHAR      NOT NULL,
    error_message TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (connector_id, source_id)
);
"""


def _ensure_schema(dsn: str) -> None:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)
    conn.close()


# ── MinIO fetch shim for DocumentProcessor ──────────────────────────────────────

def _make_minio_fetcher(minio_client: Minio):
    """Return a content_fetcher callable for DocumentProcessor that reads from MinIO."""
    def fetch(content_ref: str) -> bytes:
        # content_ref format: "s3://bucket/key"
        path = content_ref[len("s3://"):]
        bucket, key = path.split("/", 1)
        resp = minio_client.get_object(bucket, key)
        return resp.read()
    return fetch


# ── Builder helpers ─────────────────────────────────────────────────────────────

def _conn_cfg(bootstrap: str, bucket: str, connector_id: str) -> ConnConfig:
    cfg = ConnConfig()
    cfg.kafka_bootstrap = bootstrap
    cfg.kafka_topic = TOPIC_RAW
    cfg.kafka_produce_timeout_ms = 10_000
    cfg.minio_bucket = bucket
    cfg.connector_id = connector_id
    cfg.tenant_id = "test-tenant"
    return cfg


def _proc_cfg(bootstrap: str, group_id: str) -> ProcConfig:
    cfg = ProcConfig()
    cfg.kafka_bootstrap = bootstrap
    cfg.kafka_input_topic = TOPIC_RAW
    cfg.kafka_output_topic = TOPIC_CHUNKS
    cfg.kafka_dlq_topic = TOPIC_DLQ
    cfg.kafka_consumer_group = group_id
    cfg.kafka_produce_timeout_ms = 10_000
    cfg.kafka_max_poll_interval_ms = 300_000
    cfg.kafka_session_timeout_ms = 45_000
    cfg.chunk_size_tokens = 512
    cfg.chunk_overlap_tokens = 64
    return cfg


def _build_connector(
    bootstrap: str,
    minio_client: Minio,
    redis_client,
    db_conn,
    bucket: str,
    connector_id: str,
) -> S3Connector:
    cfg = _conn_cfg(bootstrap, bucket, connector_id)
    producer = Producer({"bootstrap.servers": bootstrap})
    return S3Connector(minio_client, producer, redis_client, db_conn, cfg)


def _build_processor(bootstrap: str, minio_client: Minio, group_id: str, db_conn) -> DocumentProcessor:
    cfg = _proc_cfg(bootstrap, group_id)
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": bootstrap})
    dlq_producer = Producer({"bootstrap.servers": bootstrap})
    fetcher = _make_minio_fetcher(minio_client)
    return DocumentProcessor(consumer, producer, dlq_producer, fetcher, db_conn=db_conn, cfg=cfg)


def _run_processor_with_timeout(processor: DocumentProcessor, stop_after_s: float) -> None:
    """Run processor.run() and stop it after stop_after_s seconds."""
    timer = threading.Timer(stop_after_s, processor.stop)
    timer.start()
    try:
        processor.run(poll_timeout_s=1.0)
    finally:
        timer.cancel()


# ── Session-scoped container fixtures ───────────────────────────────────────────

@pytest.fixture(scope="session")
def kafka():
    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as k:
        bootstrap = k.get_bootstrap_server()
        _create_topics(bootstrap)
        yield k


@pytest.fixture(scope="session")
def minio():
    with MinioContainer() as m:
        yield m


@pytest.fixture(scope="session")
def redis_svc():
    with RedisContainer("redis:7-alpine") as r:
        yield r


@pytest.fixture(scope="session")
def postgres():
    with PostgresContainer(
        "postgres:16-alpine",
        username="pipeline",
        password="pipeline",
        dbname="pipeline",
        driver=None,  # returns plain postgresql:// DSN, not +psycopg2
    ) as pg:
        dsn = pg.get_connection_url()
        _ensure_schema(dsn)
        yield pg


# ── Convenience fixture: all connection strings in one dict ─────────────────────

@pytest.fixture(scope="session")
def infra(kafka, minio, redis_svc, postgres):
    bootstrap = kafka.get_bootstrap_server()
    minio_cfg = minio.get_config()
    redis_host = redis_svc.get_container_host_ip()
    redis_port = redis_svc.get_exposed_port(6379)
    pg_dsn = postgres.get_connection_url()
    return {
        "bootstrap": bootstrap,
        "minio_endpoint": minio_cfg["endpoint"],
        "minio_access_key": minio_cfg["access_key"],
        "minio_secret_key": minio_cfg["secret_key"],
        "redis_url": f"redis://{redis_host}:{redis_port}/0",
        "postgres_dsn": pg_dsn,
    }


# ── Test class ──────────────────────────────────────────────────────────────────

class TestConnectorKafka:

    # ── test 1: happy path PDF ──────────────────────────────────────────────────

    def test_happy_path_pdf(self, infra):
        """Valid PDF uploaded → connector publishes event → processor produces chunks."""
        bucket = "happy-path-pdf"
        connector_id = "conn-happy"

        minio_client = Minio(
            infra["minio_endpoint"],
            access_key=infra["minio_access_key"],
            secret_key=infra["minio_secret_key"],
            secure=False,
        )
        minio_client.make_bucket(bucket)

        pdf_bytes = _make_pdf_bytes()
        minio_client.put_object(
            bucket, "document.pdf",
            io.BytesIO(pdf_bytes), length=len(pdf_bytes),
            content_type="application/pdf",
        )

        redis_client = redis_lib.from_url(infra["redis_url"])
        db_conn = psycopg2.connect(infra["postgres_dsn"])

        connector = _build_connector(
            infra["bootstrap"], minio_client, redis_client, db_conn, bucket, connector_id
        )

        # Record chunk-topic offset before producing so we drain only this test's output.
        chunks_start = _get_high_watermark(infra["bootstrap"], TOPIC_CHUNKS)

        events = list(connector.poll())
        assert len(events) == 1, f"Expected 1 raw event, got {len(events)}"

        processor = _build_processor(
            infra["bootstrap"], minio_client,
            group_id="proc-happy-pdf",
            db_conn=db_conn,
        )
        thread = threading.Thread(
            target=_run_processor_with_timeout, args=(processor, 25), daemon=True
        )
        thread.start()

        chunks = _drain_from_offset(
            infra["bootstrap"], TOPIC_CHUNKS, chunks_start, n=1, timeout_s=30
        )
        processor.stop()
        thread.join(timeout=10)
        db_conn.close()

        assert len(chunks) >= 1, "No chunks published to document-chunks topic"
        chunk = chunks[0]
        assert chunk["source_id"] == f"{bucket}/document.pdf"
        assert chunk["content_type"] == "application/pdf"
        assert chunk["tenant_id"] == "test-tenant"
        assert chunk["text"], "Chunk text must not be empty"

    # ── test 2: corrupt PDF → DLQ ───────────────────────────────────────────────

    def test_corrupt_pdf_routed_to_dlq(self, infra):
        """Corrupt PDF bytes → processor raises ParseError → message routed to DLQ."""
        bucket = "corrupt-pdf"
        connector_id = "conn-corrupt"

        minio_client = Minio(
            infra["minio_endpoint"],
            access_key=infra["minio_access_key"],
            secret_key=infra["minio_secret_key"],
            secure=False,
        )
        minio_client.make_bucket(bucket)

        corrupt_bytes = b"NOT_A_PDF: this will fail pdfplumber gracefully"
        minio_client.put_object(
            bucket, "corrupt.pdf",
            io.BytesIO(corrupt_bytes), length=len(corrupt_bytes),
            content_type="application/pdf",
        )

        redis_client = redis_lib.from_url(infra["redis_url"])
        db_conn = psycopg2.connect(infra["postgres_dsn"])

        connector = _build_connector(
            infra["bootstrap"], minio_client, redis_client, db_conn, bucket, connector_id
        )

        # Capture DLQ high-water before producing so drain only sees this test's entry.
        dlq_start = _get_high_watermark(infra["bootstrap"], TOPIC_DLQ)

        events = list(connector.poll())
        assert len(events) == 1, f"Expected 1 raw event, got {len(events)}"

        processor = _build_processor(
            infra["bootstrap"], minio_client,
            group_id="proc-corrupt-pdf",
            db_conn=db_conn,
        )
        thread = threading.Thread(
            target=_run_processor_with_timeout, args=(processor, 25), daemon=True
        )
        thread.start()

        dlq_msgs = _drain_from_offset(
            infra["bootstrap"], TOPIC_DLQ, dlq_start, n=1, timeout_s=30
        )
        processor.stop()
        thread.join(timeout=10)
        db_conn.close()

        assert len(dlq_msgs) >= 1, "No DLQ entry produced for corrupt PDF"
        dlq = dlq_msgs[0]
        assert dlq["failure_reason"] == "parse_error", (
            f"Expected parse_error, got {dlq['failure_reason']!r}"
        )
        assert "corrupt.pdf" in dlq["original_payload"]["source_id"]

    # ── test 3: watermark prevents duplicate events ─────────────────────────────

    def test_watermark_prevents_duplicates(self, infra):
        """Polling the same connector twice yields events only on the first call."""
        bucket = "watermark-test"
        connector_id = "conn-watermark"

        minio_client = Minio(
            infra["minio_endpoint"],
            access_key=infra["minio_access_key"],
            secret_key=infra["minio_secret_key"],
            secure=False,
        )
        minio_client.make_bucket(bucket)

        pdf_bytes = _make_pdf_bytes()
        minio_client.put_object(
            bucket, "doc.pdf",
            io.BytesIO(pdf_bytes), length=len(pdf_bytes),
            content_type="application/pdf",
        )

        redis_client = redis_lib.from_url(infra["redis_url"])
        db_conn = psycopg2.connect(infra["postgres_dsn"])

        connector = _build_connector(
            infra["bootstrap"], minio_client, redis_client, db_conn, bucket, connector_id
        )

        # First poll: file is newer than watermark (no watermark yet) → 1 event.
        first_poll = list(connector.poll())
        assert len(first_poll) == 1, (
            f"First poll expected 1 event, got {len(first_poll)}"
        )

        # Second poll on the same connector (same Redis client, same connector_id).
        # Watermark was advanced to the file's last_modified, so the file is filtered out.
        second_poll = list(connector.poll())
        assert len(second_poll) == 0, (
            f"Second poll expected 0 events (watermark should suppress), got {len(second_poll)}"
        )

        db_conn.close()
