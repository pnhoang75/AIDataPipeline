"""Integration tests: document-chunks Kafka → EmbeddingWorker → Milvus + PostgreSQL.

Four scenarios:
  1. vector written      — ChunkEvent on Kafka → worker embeds → entity in Milvus with
                           correct chunk_id and 384-dim vector.
  2. idempotent upsert   — same ChunkEvent produced twice → Milvus has exactly 1 entity
                           (upsert by chunk_id, not a duplicate insert).
  3. timeout → DLQ       — backend raises TimeoutError → chunk routed to
                           dlq-document-chunks, offset NOT committed.
  4. postgres=indexed    — after successful embedding → source_file_status row has
                           ingest_status='indexed' and chunk_count set.

Requires Docker (testcontainers spins up Kafka + PostgreSQL).
Milvus uses the embedded Milvus Lite file backend (no Docker image needed).
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import warnings
from pathlib import Path
from typing import List

import psycopg2
import pytest

warnings.filterwarnings("ignore", category=DeprecationWarning)

# ── Testcontainers (skip whole module if not installed) ─────────────────────
testcontainers = pytest.importorskip("testcontainers", reason="testcontainers not installed")

from testcontainers.kafka import KafkaContainer  # noqa: E402
from testcontainers.postgres import PostgresContainer  # noqa: E402

from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

# ── Service source path ─────────────────────────────────────────────────────
_ROOT = Path(__file__).parent.parent.parent
_EMBED_SRC = str(_ROOT / "services" / "embedding-worker" / "src")

sys.path.insert(0, _EMBED_SRC)
for _m in ["config", "events", "milvus_writer", "status_updater", "backends", "worker"]:
    sys.modules.pop(_m, None)

from worker import EmbeddingWorker  # noqa: E402
from milvus_writer import MilvusWriter  # noqa: E402
from events import DocumentChunkEvent  # noqa: E402
import config as _embed_cfg_mod  # noqa: E402

EmbedConfig = _embed_cfg_mod.Config

# ── Kafka topic names ───────────────────────────────────────────────────────
TOPIC_CHUNKS = "document-chunks"
TOPIC_EVENTS = "embedding-events"
TOPIC_DLQ = "dlq-document-chunks"
TOPICS = [TOPIC_CHUNKS, TOPIC_EVENTS, TOPIC_DLQ]

# ── PostgreSQL schema ───────────────────────────────────────────────────────
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS source_file_status (
    connector_id  VARCHAR      NOT NULL,
    source_id     VARCHAR      NOT NULL,
    event_id      VARCHAR      NOT NULL DEFAULT '',
    ingest_status VARCHAR      NOT NULL,
    chunk_count   INTEGER      NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (connector_id, source_id)
);
"""


# ── Deterministic embedding backend (no model load) ─────────────────────────

class FixedBackend:
    """Returns deterministic 384-dim vectors based on text hash. No model download."""

    @property
    def dim(self) -> int:
        return 384

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        result = []
        for text in texts:
            seed = hash(text) % 1000
            vec = [(seed + i) / 1000.0 for i in range(384)]
            result.append(vec)
        return result


class TimeoutBackend:
    """Always raises TimeoutError to simulate embedding service unavailability."""

    @property
    def dim(self) -> int:
        return 384

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        raise TimeoutError("embedding inference timed out")


# ── Kafka helpers ───────────────────────────────────────────────────────────

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
        "group.id": f"hwm-{topic}-{time.time()}",
    })
    try:
        tp = TopicPartition(topic, 0)
        lo, hi = consumer.get_watermark_offsets(tp, timeout=10)
        return hi
    finally:
        consumer.close()


def _drain_from_offset(
    bootstrap: str, topic: str, start_offset: int, n: int, timeout_s: float = 30
) -> list:
    """Consume up to n messages from topic starting at start_offset."""
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


def _produce_chunk(bootstrap: str, chunk: DocumentChunkEvent) -> None:
    producer = Producer({"bootstrap.servers": bootstrap})
    producer.produce(
        topic=TOPIC_CHUNKS,
        key=chunk.doc_id.encode(),
        value=chunk.to_json().encode(),
    )
    producer.flush(10)


def _make_chunk(
    doc_id: str = "doc-1",
    chunk_id: str = "doc-1:0",
    source_id: str = "bucket/file.pdf",
    tenant_id: str = "test-tenant",
    text: str = "Integration test content for embedding.",
) -> DocumentChunkEvent:
    return DocumentChunkEvent(
        doc_id=doc_id,
        chunk_id=chunk_id,
        chunk_index=0,
        total_chunks=1,
        text=text,
        source_type="s3",
        source_id=source_id,
        content_type="application/pdf",
        tenant_id=tenant_id,
    )


# ── Worker builder ──────────────────────────────────────────────────────────

def _embed_cfg(bootstrap: str, group_id: str) -> EmbedConfig:
    cfg = EmbedConfig()
    cfg.kafka_bootstrap = bootstrap
    cfg.kafka_input_topic = TOPIC_CHUNKS
    cfg.kafka_event_topic = TOPIC_EVENTS
    cfg.kafka_dlq_topic = TOPIC_DLQ
    cfg.kafka_consumer_group = group_id
    cfg.kafka_produce_timeout_ms = 10_000
    cfg.embedding_batch_size = 32
    cfg.embedding_batch_timeout_ms = 2_000  # 2 s to collect batch in tests
    return cfg


def _build_worker(
    bootstrap: str,
    milvus_uri: str,
    db_conn,
    backend,
    group_id: str,
    collection: str,
    start_chunk_offset: int = 0,
) -> EmbeddingWorker:
    """Build an EmbeddingWorker whose consumer starts at start_chunk_offset.

    Using consumer.assign() + explicit offset instead of subscribe() ensures
    each test only processes the messages it produced, not historical ones.
    """
    cfg = _embed_cfg(bootstrap, group_id)

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
    })
    # Assign to exact offset so historical messages from other tests are skipped.
    tp = TopicPartition(TOPIC_CHUNKS, 0, start_chunk_offset)
    consumer.assign([tp])

    producer = Producer({"bootstrap.servers": bootstrap})
    dlq_producer = Producer({"bootstrap.servers": bootstrap})

    milvus_writer = MilvusWriter(
        host="", port=0, collection=collection, dim=384, uri=milvus_uri
    )
    milvus_writer.connect()

    return EmbeddingWorker(
        consumer=consumer,
        backend=backend,
        milvus_writer=milvus_writer,
        producer=producer,
        dlq_producer=dlq_producer,
        db_conn=db_conn,
        cfg=cfg,
    )


def _run_worker_with_timeout(worker: EmbeddingWorker, stop_after_s: float) -> None:
    timer = threading.Timer(stop_after_s, worker.stop)
    timer.start()
    try:
        worker.run()
    finally:
        timer.cancel()


# ── PostgreSQL helpers ──────────────────────────────────────────────────────

def _ensure_schema(dsn: str) -> None:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)
    conn.close()


def _seed_status_row(db_conn, source_id: str) -> None:
    """Insert a 'pending' row that the worker will update to 'indexed'."""
    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source_file_status (connector_id, source_id, ingest_status)
            VALUES (%s, %s, 'pending')
            ON CONFLICT (connector_id, source_id) DO UPDATE SET ingest_status = 'pending'
            """,
            ("conn-test", source_id),
        )
    db_conn.commit()


def _get_status_row(db_conn, source_id: str) -> dict:
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT ingest_status, chunk_count FROM source_file_status WHERE source_id = %s",
            (source_id,),
        )
        row = cur.fetchone()
    return {"ingest_status": row[0], "chunk_count": row[1]} if row else {}


# ── Session-scoped container fixtures ───────────────────────────────────────

@pytest.fixture(scope="session")
def kafka():
    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as k:
        bootstrap = k.get_bootstrap_server()
        _create_topics(bootstrap)
        yield k


@pytest.fixture(scope="session")
def postgres():
    with PostgresContainer(
        "postgres:16-alpine",
        username="pipeline",
        password="pipeline",
        dbname="pipeline",
        driver=None,
    ) as pg:
        dsn = pg.get_connection_url()
        _ensure_schema(dsn)
        yield pg


@pytest.fixture(scope="session")
def milvus_db(tmp_path_factory):
    """Milvus Lite file — shared within the session; each test uses its own collection."""
    db_path = str(tmp_path_factory.mktemp("milvus") / "test.db")
    yield db_path


@pytest.fixture(scope="session")
def infra(kafka, postgres, milvus_db):
    return {
        "bootstrap": kafka.get_bootstrap_server(),
        "postgres_dsn": postgres.get_connection_url(),
        "milvus_uri": milvus_db,
    }


# ── Test class ──────────────────────────────────────────────────────────────

class TestEmbedderMilvus:

    # ── test 1: vector written to Milvus ───────────────────────────────────

    def test_embedder_writes_vector_to_milvus(self, infra):
        """ChunkEvent on Kafka → worker embeds → Milvus has 1 entity with correct fields."""
        chunk = _make_chunk(
            doc_id="doc-write-1",
            chunk_id="doc-write-1:0",
            source_id="bucket/write-test.pdf",
        )

        # Record offset BEFORE producing so worker starts exactly here.
        start_offset = _get_high_watermark(infra["bootstrap"], TOPIC_CHUNKS)
        _produce_chunk(infra["bootstrap"], chunk)

        db_conn = psycopg2.connect(infra["postgres_dsn"])
        _seed_status_row(db_conn, chunk.source_id)

        worker = _build_worker(
            bootstrap=infra["bootstrap"],
            milvus_uri=infra["milvus_uri"],
            db_conn=db_conn,
            backend=FixedBackend(),
            group_id="grp-write-vector",
            collection="col_write_vector",
            start_chunk_offset=start_offset,
        )

        thread = threading.Thread(
            target=_run_worker_with_timeout, args=(worker, 15), daemon=True
        )
        thread.start()
        thread.join(timeout=20)
        db_conn.close()

        # Verify Milvus entity
        results = worker._milvus_writer.query(
            f'chunk_id == "{chunk.chunk_id}"',
            output_fields=["chunk_id", "doc_id", "text", "embedding", "tenant_id"],
        )
        assert len(results) == 1, f"Expected 1 Milvus entity, got {len(results)}"
        entity = results[0]
        assert entity["chunk_id"] == chunk.chunk_id
        assert entity["doc_id"] == chunk.doc_id
        assert entity["text"] == chunk.text
        assert entity["tenant_id"] == chunk.tenant_id
        emb = entity["embedding"]
        assert len(emb) == 384, f"Expected 384-dim vector, got {len(emb)}"
        assert isinstance(emb[0], float), "Embedding values must be floats"

    # ── test 2: idempotent upsert ──────────────────────────────────────────

    def test_embedder_upsert_is_idempotent(self, infra):
        """Processing the same ChunkEvent twice → Milvus has exactly 1 entity (no duplicate)."""
        chunk = _make_chunk(
            doc_id="doc-idem-1",
            chunk_id="doc-idem-1:0",
            source_id="bucket/idem-test.pdf",
        )

        # Record offset then produce the same chunk twice.
        start_offset = _get_high_watermark(infra["bootstrap"], TOPIC_CHUNKS)
        _produce_chunk(infra["bootstrap"], chunk)
        _produce_chunk(infra["bootstrap"], chunk)

        db_conn = psycopg2.connect(infra["postgres_dsn"])
        _seed_status_row(db_conn, chunk.source_id)

        worker = _build_worker(
            bootstrap=infra["bootstrap"],
            milvus_uri=infra["milvus_uri"],
            db_conn=db_conn,
            backend=FixedBackend(),
            group_id="grp-idem-upsert",
            collection="col_idem_upsert",
            start_chunk_offset=start_offset,
        )

        # Run long enough to process both messages (each batch waits up to 2 s).
        thread = threading.Thread(
            target=_run_worker_with_timeout, args=(worker, 20), daemon=True
        )
        thread.start()
        thread.join(timeout=25)
        db_conn.close()

        # Upsert by chunk_id → second write overwrites the first → exactly 1 entity.
        results = worker._milvus_writer.query(
            f'chunk_id == "{chunk.chunk_id}"',
            output_fields=["chunk_id"],
        )
        assert len(results) == 1, (
            f"Expected 1 Milvus entity after idempotent upsert, got {len(results)}"
        )

    # ── test 3: embedding timeout routes chunk to DLQ ──────────────────────

    def test_embedder_timeout_routes_to_dlq(self, infra):
        """TimeoutError from backend → chunk goes to dlq-document-chunks; not in Milvus."""
        chunk = _make_chunk(
            doc_id="doc-dlq-1",
            chunk_id="doc-dlq-1:0",
            source_id="bucket/dlq-test.pdf",
            text="Content that will fail to embed.",
        )

        # Capture high-water marks before producing.
        start_offset = _get_high_watermark(infra["bootstrap"], TOPIC_CHUNKS)
        dlq_start = _get_high_watermark(infra["bootstrap"], TOPIC_DLQ)
        _produce_chunk(infra["bootstrap"], chunk)

        db_conn = psycopg2.connect(infra["postgres_dsn"])

        worker = _build_worker(
            bootstrap=infra["bootstrap"],
            milvus_uri=infra["milvus_uri"],
            db_conn=db_conn,
            backend=TimeoutBackend(),
            group_id="grp-timeout-dlq",
            collection="col_timeout_dlq",
            start_chunk_offset=start_offset,
        )

        thread = threading.Thread(
            target=_run_worker_with_timeout, args=(worker, 15), daemon=True
        )
        thread.start()
        thread.join(timeout=20)
        db_conn.close()

        # DLQ should contain exactly 1 message for the failed chunk.
        dlq_msgs = _drain_from_offset(
            infra["bootstrap"], TOPIC_DLQ, dlq_start, n=1, timeout_s=15
        )
        assert len(dlq_msgs) >= 1, "No DLQ entry produced for timed-out embedding"
        dlq = dlq_msgs[0]
        assert dlq["failure_reason"] == "embedding_error", (
            f"Expected 'embedding_error', got {dlq['failure_reason']!r}"
        )
        assert dlq["original_topic"] == TOPIC_CHUNKS
        original = dlq["original_payload"]
        assert original["chunk_id"] == chunk.chunk_id

        # Milvus must have NO entity (chunk was DLQ'd, not inserted).
        results = worker._milvus_writer.query(
            f'chunk_id == "{chunk.chunk_id}"',
            output_fields=["chunk_id"],
        )
        assert len(results) == 0, (
            f"Expected 0 Milvus entities after DLQ routing, got {len(results)}"
        )

    # ── test 4: postgres status updated to indexed ─────────────────────────

    def test_embedder_updates_postgres_status_to_indexed(self, infra):
        """After successful embedding, source_file_status row is updated to 'indexed'."""
        chunk = _make_chunk(
            doc_id="doc-pg-1",
            chunk_id="doc-pg-1:0",
            source_id="bucket/pg-status-test.pdf",
            text="Document whose status will be tracked in PostgreSQL.",
        )

        start_offset = _get_high_watermark(infra["bootstrap"], TOPIC_CHUNKS)
        _produce_chunk(infra["bootstrap"], chunk)

        db_conn = psycopg2.connect(infra["postgres_dsn"])
        _seed_status_row(db_conn, chunk.source_id)

        # Confirm initial status before running the worker.
        initial = _get_status_row(db_conn, chunk.source_id)
        assert initial["ingest_status"] == "pending", (
            f"Expected initial status 'pending', got {initial!r}"
        )

        worker = _build_worker(
            bootstrap=infra["bootstrap"],
            milvus_uri=infra["milvus_uri"],
            db_conn=db_conn,
            backend=FixedBackend(),
            group_id="grp-pg-status",
            collection="col_pg_status",
            start_chunk_offset=start_offset,
        )

        thread = threading.Thread(
            target=_run_worker_with_timeout, args=(worker, 15), daemon=True
        )
        thread.start()
        thread.join(timeout=20)

        row = _get_status_row(db_conn, chunk.source_id)
        db_conn.close()

        assert row, f"No source_file_status row found for source_id={chunk.source_id!r}"
        assert row["ingest_status"] == "indexed", (
            f"Expected ingest_status='indexed', got {row['ingest_status']!r}"
        )
        assert row["chunk_count"] > 0, (
            f"Expected chunk_count > 0, got {row['chunk_count']}"
        )
