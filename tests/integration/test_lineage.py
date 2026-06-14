"""Integration tests: Metadata Service REST endpoints.

Spins up PostgreSQL (testcontainers) to test all six lineage/quality endpoints.
One test also uses a Kafka container to verify the MetadataConsumer processes
DataSource events and stores them in the DB.

Run with:
    pytest tests/integration/test_lineage.py -x --tb=short -q
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import quote

import psycopg2
import psycopg2.extras
import pytest

testcontainers = pytest.importorskip("testcontainers", reason="testcontainers not installed")

from testcontainers.kafka import KafkaContainer  # noqa: E402
from testcontainers.postgres import PostgresContainer  # noqa: E402

from confluent_kafka import Consumer, Producer  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

_ROOT = Path(__file__).parent.parent.parent
_META_SRC = str(_ROOT / "services" / "metadata-service" / "src")

TOPIC_META_EVENTS = "metadata-events"
TOPIC_DQ_FAILED = "data-quality-failed"


# ── Schema DDL ──────────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE metadata.schema_versions (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id             UUID NOT NULL,
    version_number        INTEGER NOT NULL,
    chunk_size            INTEGER NOT NULL DEFAULT 512,
    chunk_overlap         INTEGER NOT NULL DEFAULT 64,
    chunking_strategy     TEXT NOT NULL DEFAULT 'fixed',
    embedding_model       TEXT NOT NULL,
    embedding_model_version TEXT,
    embedding_dimension   INTEGER NOT NULL,
    embedding_backend     TEXT NOT NULL,
    index_type            TEXT NOT NULL DEFAULT 'IVF_FLAT',
    is_current            BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ DEFAULT now(),
    created_by            TEXT,
    UNIQUE (tenant_id, version_number)
);

CREATE TABLE metadata.pipeline_runs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL,
    pipeline_type       TEXT NOT NULL,
    connector_id        TEXT,
    schema_version_id   UUID REFERENCES metadata.schema_versions,
    config_snapshot     JSONB NOT NULL DEFAULT '{}',
    status              TEXT NOT NULL DEFAULT 'running',
    started_at          TIMESTAMPTZ DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    duration_ms         INTEGER GENERATED ALWAYS AS (
                            EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000
                        ) STORED,
    entities_processed  INTEGER NOT NULL DEFAULT 0,
    entities_failed     INTEGER NOT NULL DEFAULT 0,
    bytes_processed     BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE metadata.entities (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type       TEXT NOT NULL,
    entity_key        TEXT NOT NULL,
    tenant_id         UUID NOT NULL,
    pipeline_run_id   UUID REFERENCES metadata.pipeline_runs,
    schema_version_id UUID REFERENCES metadata.schema_versions,
    attributes        JSONB NOT NULL DEFAULT '{}',
    is_current        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ DEFAULT now(),
    updated_at        TIMESTAMPTZ DEFAULT now(),
    UNIQUE (tenant_id, entity_type, entity_key)
);

CREATE INDEX ON metadata.entities USING GIN (attributes);

CREATE TABLE metadata.lineage (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    upstream_id     UUID NOT NULL REFERENCES metadata.entities,
    downstream_id   UUID NOT NULL REFERENCES metadata.entities,
    relationship    TEXT NOT NULL,
    pipeline_run_id UUID REFERENCES metadata.pipeline_runs,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (upstream_id, downstream_id, relationship)
);

CREATE TABLE metadata.processing_steps (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id       UUID NOT NULL REFERENCES metadata.entities,
    pipeline_run_id UUID NOT NULL REFERENCES metadata.pipeline_runs,
    step_type       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    started_at      TIMESTAMPTZ DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    duration_ms     INTEGER GENERATED ALWAYS AS (
                        EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000
                    ) STORED,
    input_bytes     BIGINT,
    output_count    INTEGER,
    config_used     JSONB,
    error_message   TEXT,
    error_code      TEXT
);

CREATE TABLE metadata.data_quality (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id   UUID NOT NULL REFERENCES metadata.entities,
    run_id      UUID REFERENCES metadata.pipeline_runs,
    check_name  TEXT NOT NULL,
    status      TEXT NOT NULL,
    value       NUMERIC,
    threshold   NUMERIC,
    message     TEXT,
    checked_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE metadata.query_results (
    query_id        UUID NOT NULL,
    chunk_entity_id UUID NOT NULL REFERENCES metadata.entities,
    rank            INTEGER NOT NULL,
    score           NUMERIC NOT NULL,
    cached          BOOLEAN NOT NULL DEFAULT FALSE,
    feedback_score  INTEGER,
    PRIMARY KEY (query_id, chunk_entity_id)
);
"""


def _apply_schema(dsn: str) -> None:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)
    conn.close()


def _insert_entity(conn, entity_type, entity_key, tenant_id, attributes=None,
                   pipeline_run_id=None, schema_version_id=None) -> str:
    attrs = json.dumps(attributes or {})
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.entities
                (entity_type, entity_key, tenant_id, attributes,
                 pipeline_run_id, schema_version_id)
            VALUES (%s, %s, %s::uuid, %s::jsonb, %s, %s)
            ON CONFLICT (tenant_id, entity_type, entity_key)
            DO UPDATE SET attributes = EXCLUDED.attributes
            RETURNING id
            """,
            (entity_type, entity_key, tenant_id, attrs, pipeline_run_id, schema_version_id),
        )
        return str(cur.fetchone()[0])


def _insert_lineage(conn, upstream_id, downstream_id, relationship) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.lineage (upstream_id, downstream_id, relationship)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (upstream_id, downstream_id, relationship),
        )


def _create_kafka_topics(bootstrap: str, *topics: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics(
        [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics]
    )
    for _, fut in futures.items():
        try:
            fut.result(timeout=30)
        except Exception:
            pass


# ── Fixtures ────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def _pg():
    with PostgresContainer(
        "postgres:16-alpine",
        username="meta",
        password="meta",
        dbname="meta",
        driver=None,
    ) as pg:
        _apply_schema(pg.get_connection_url())
        yield pg


@pytest.fixture(scope="module")
def _kafka():
    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as k:
        _create_kafka_topics(k.get_bootstrap_server(), TOPIC_META_EVENTS, TOPIC_DQ_FAILED)
        yield k


@pytest.fixture(scope="module")
def api_client(_pg):
    """FastAPI TestClient wired to the test PostgreSQL instance."""
    from fastapi.testclient import TestClient

    # Insert src on sys.path and clear stale module cache before importing app
    if _META_SRC not in sys.path:
        sys.path.insert(0, _META_SRC)
    for _m in ["config", "app", "db", "consumer", "events"]:
        sys.modules.pop(_m, None)

    os.environ["DATABASE_URL"] = _pg.get_connection_url()
    os.environ["KAFKA_BOOTSTRAP"] = ""  # disable consumer thread during API tests

    import app as _app_mod

    with TestClient(_app_mod.app, raise_server_exceptions=True) as client:
        yield client, _pg.get_connection_url()


# ── Tests ────────────────────────────────────────────────────────────────────────

class TestLineageEndpoints:

    def test_lineage_upstream_traces_to_datasource(self, api_client):
        client, dsn = api_client
        tenant_id = str(uuid.uuid4())
        conn = psycopg2.connect(dsn)

        try:
            ds_id = _insert_entity(conn, "DataSource", f"s3://acme/{tenant_id}/",
                                   tenant_id,
                                   {"source_type": "s3",
                                    "source_path": f"s3://acme/{tenant_id}/"})
            raw_id = _insert_entity(conn, "RawDocument", f"sha256:doc:{tenant_id}",
                                    tenant_id,
                                    {"source_path": f"s3://acme/{tenant_id}/report.pdf"})
            chunk_key = f"sha256:doc:{tenant_id}:3"
            chunk_id = _insert_entity(conn, "DocumentChunk", chunk_key, tenant_id,
                                      {"doc_id": f"sha256:doc:{tenant_id}", "chunk_index": 3})
            _insert_lineage(conn, ds_id, raw_id, "discovered_in")
            _insert_lineage(conn, raw_id, chunk_id, "chunked_into")
            conn.commit()
        finally:
            conn.close()

        resp = client.get(f"/api/lineage/upstream/{quote(chunk_key, safe='')}")
        assert resp.status_code == 200
        chain = resp.json()

        entity_types = [r["entity_type"] for r in chain]
        assert "DocumentChunk" in entity_types
        assert "RawDocument" in entity_types
        assert "DataSource" in entity_types

        # DataSource should appear at the deepest depth (highest depth value)
        ds_row = next(r for r in chain if r["entity_type"] == "DataSource")
        assert ds_row["depth"] > 0

    def test_lineage_downstream_returns_all_derived_entities(self, api_client):
        client, dsn = api_client
        tenant_id = str(uuid.uuid4())
        source_path = f"s3://acme/{tenant_id}/report.pdf"
        conn = psycopg2.connect(dsn)

        try:
            raw_id = _insert_entity(conn, "RawDocument", f"sha256:rpt:{tenant_id}",
                                    tenant_id, {"source_path": source_path})
            chunk1_id = _insert_entity(conn, "DocumentChunk", f"sha256:rpt:{tenant_id}:0",
                                       tenant_id, {})
            chunk2_id = _insert_entity(conn, "DocumentChunk", f"sha256:rpt:{tenant_id}:1",
                                       tenant_id, {})
            emb1_id = _insert_entity(conn, "Embedding", f"emb:{tenant_id}:0",
                                     tenant_id, {})
            emb2_id = _insert_entity(conn, "Embedding", f"emb:{tenant_id}:1",
                                     tenant_id, {})
            _insert_lineage(conn, raw_id, chunk1_id, "chunked_into")
            _insert_lineage(conn, raw_id, chunk2_id, "chunked_into")
            _insert_lineage(conn, chunk1_id, emb1_id, "embedded_by")
            _insert_lineage(conn, chunk2_id, emb2_id, "embedded_by")
            conn.commit()
        finally:
            conn.close()

        encoded = quote(source_path, safe="")
        resp = client.get(f"/api/lineage/downstream/{encoded}?tenant_id={tenant_id}")
        assert resp.status_code == 200
        rows = resp.json()

        counts = {r["entity_type"]: r["count"] for r in rows}
        assert counts.get("DocumentChunk") == 2, f"expected 2 chunks, got {counts}"
        assert counts.get("Embedding") == 2, f"expected 2 embeddings, got {counts}"

    def test_stale_embeddings_detected_after_model_change(self, api_client):
        client, dsn = api_client
        tenant_id = str(uuid.uuid4())
        conn = psycopg2.connect(dsn)

        try:
            # Create old schema version (NOT current)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO metadata.schema_versions
                        (tenant_id, version_number, embedding_model, embedding_dimension,
                         embedding_backend, is_current)
                    VALUES (%s::uuid, 1, 'bge-small-en-v1.5', 384, 'local-cpu', FALSE)
                    RETURNING id
                    """,
                    (tenant_id,),
                )
                old_sv_id = str(cur.fetchone()[0])

                # Create new schema version (current)
                cur.execute(
                    """
                    INSERT INTO metadata.schema_versions
                        (tenant_id, version_number, embedding_model, embedding_dimension,
                         embedding_backend, is_current)
                    VALUES (%s::uuid, 2, 'bge-large-en-v1.5', 1024, 'local-cpu', TRUE)
                    RETURNING id
                    """,
                    (tenant_id,),
                )

            # Insert Embedding entity referencing the OLD schema version
            _insert_entity(conn, "Embedding", f"emb:stale:{tenant_id}",
                           tenant_id,
                           {"chunk_id": f"chunk:stale:{tenant_id}",
                            "model_name": "bge-small-en-v1.5"},
                           schema_version_id=old_sv_id)
            conn.commit()
        finally:
            conn.close()

        resp = client.get(f"/api/lineage/stale/{tenant_id}")
        assert resp.status_code == 200
        stale = resp.json()

        assert len(stale) >= 1, "Expected at least one stale embedding"
        row = stale[0]
        assert row["old_model"] == "bge-small-en-v1.5"
        assert row["current_model"] == "bge-large-en-v1.5"

    def test_rag_query_provenance_links_back_to_source(self, api_client):
        client, dsn = api_client
        tenant_id = str(uuid.uuid4())
        query_id = str(uuid.uuid4())
        conn = psycopg2.connect(dsn)

        try:
            # Schema version and pipeline run are required by the provenance JOIN
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO metadata.schema_versions
                        (tenant_id, version_number, embedding_model, embedding_dimension,
                         embedding_backend, chunk_size)
                    VALUES (%s::uuid, 1, 'bge-small-en-v1.5', 384, 'local-cpu', 512)
                    RETURNING id
                    """,
                    (tenant_id,),
                )
                sv_id = str(cur.fetchone()[0])

                cur.execute(
                    """
                    INSERT INTO metadata.pipeline_runs
                        (tenant_id, pipeline_type, schema_version_id)
                    VALUES (%s::uuid, 'embedding', %s)
                    RETURNING id
                    """,
                    (tenant_id, sv_id),
                )
                run_id = str(cur.fetchone()[0])

            source_path = f"s3://acme/{tenant_id}/board-deck.pdf"
            raw_id = _insert_entity(conn, "RawDocument", f"sha256:board:{tenant_id}",
                                    tenant_id,
                                    {"source_path": source_path,
                                     "content_type": "application/pdf"})
            chunk_key = f"sha256:board:{tenant_id}:0"
            chunk_id = _insert_entity(conn, "DocumentChunk", chunk_key, tenant_id,
                                      {"text_preview": "Revenue grew 24%",
                                       "page_number": "7"},
                                      pipeline_run_id=run_id,
                                      schema_version_id=sv_id)
            # chunked_into: RawDocument is upstream, DocumentChunk is downstream
            _insert_lineage(conn, raw_id, chunk_id, "chunked_into")

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO metadata.query_results (query_id, chunk_entity_id, rank, score)
                    VALUES (%s::uuid, %s::uuid, 1, 0.847)
                    """,
                    (query_id, chunk_id),
                )
            conn.commit()
        finally:
            conn.close()

        resp = client.get(f"/api/lineage/provenance/{query_id}")
        assert resp.status_code == 200
        rows = resp.json()

        assert len(rows) >= 1, "Expected provenance rows"
        row = rows[0]
        assert row["source_file"] == source_path
        assert row["embedding_model"] == "bge-small-en-v1.5"
        assert row["chunk_size"] == 512
        assert row["indexed_at"] is not None
        assert row["rank"] == 1

    def test_runs_endpoint_returns_pipeline_history(self, api_client):
        client, dsn = api_client
        tenant_id = str(uuid.uuid4())
        conn = psycopg2.connect(dsn)

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO metadata.pipeline_runs (tenant_id, pipeline_type, status)
                    VALUES (%s::uuid, 'ingestion', 'completed')
                    """,
                    (tenant_id,),
                )
            conn.commit()
        finally:
            conn.close()

        resp = client.get(f"/api/runs?tenant_id={tenant_id}")
        assert resp.status_code == 200
        runs = resp.json()
        assert len(runs) >= 1
        assert runs[0]["pipeline_type"] == "ingestion"

    def test_quality_endpoint_returns_failed_checks(self, api_client):
        client, dsn = api_client
        tenant_id = str(uuid.uuid4())
        conn = psycopg2.connect(dsn)

        try:
            entity_id = _insert_entity(conn, "RawDocument", f"sha256:corrupt:{tenant_id}",
                                       tenant_id, {})
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO metadata.data_quality
                        (entity_id, check_name, status, message)
                    VALUES (%s::uuid, 'parse_success', 'failed', 'PDF parse error')
                    """,
                    (entity_id,),
                )
            conn.commit()
        finally:
            conn.close()

        resp = client.get(f"/api/quality/{tenant_id}")
        assert resp.status_code == 200
        checks = resp.json()
        assert len(checks) >= 1
        check = checks[0]
        assert check["check_name"] == "parse_success"
        assert check["status"] == "failed"


class TestMetadataConsumer:

    def test_upload_path_publishes_datasource_metadata_event(self, _pg, _kafka):
        """Consumer processes DataSource event from Kafka and stores entity in DB."""
        # Import consumer from metadata service
        if _META_SRC not in sys.path:
            sys.path.insert(0, _META_SRC)
        for _m in ["config", "consumer", "db", "events"]:
            sys.modules.pop(_m, None)

        from consumer import MetadataConsumer  # noqa: E402
        from config import Config  # noqa: E402

        bootstrap = _kafka.get_bootstrap_server()
        dsn = _pg.get_connection_url()
        tenant_id = str(uuid.uuid4())
        entity_key = f"upload-ds:{tenant_id}"

        event = {
            "specversion": "1.0",
            "type": "metadata.entity.created",
            "source": "bff/upload",
            "subject": f"DataSource/{entity_key}",
            "data": {
                "entity_type": "DataSource",
                "entity_key": entity_key,
                "tenant_id": tenant_id,
                "attributes": {
                    "source_type": "upload",
                    "source_path": f"upload://{tenant_id}/report.pdf",
                },
                "upstream": [],
                "quality_checks": [],
            },
        }

        producer = Producer({"bootstrap.servers": bootstrap})
        producer.produce(
            TOPIC_META_EVENTS,
            key=entity_key.encode(),
            value=json.dumps(event).encode(),
        )
        producer.flush(timeout=10)

        db_conn = psycopg2.connect(dsn)
        dq_producer = Producer({"bootstrap.servers": bootstrap})

        cfg = Config()
        cfg.kafka_bootstrap = bootstrap
        cfg.metadata_events_topic = TOPIC_META_EVENTS
        cfg.kafka_consumer_group = f"test-consumer-{tenant_id}"
        cfg.data_quality_failed_topic = TOPIC_DQ_FAILED
        cfg.poll_timeout_seconds = 1.0

        svc = MetadataConsumer(db_conn=db_conn, producer=dq_producer, cfg=cfg)

        kafka_consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": cfg.kafka_consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        stop_event = threading.Event()

        def _run_consumer():
            kafka_consumer.subscribe([TOPIC_META_EVENTS])
            deadline = time.time() + 15
            try:
                while time.time() < deadline and not stop_event.is_set():
                    msg = kafka_consumer.poll(timeout=1.0)
                    if msg is None or msg.error():
                        continue
                    parsed = json.loads(msg.value().decode())
                    svc.process_event(parsed)
                    db_conn.commit()
                    stop_event.set()
            finally:
                kafka_consumer.close()

        thread = threading.Thread(target=_run_consumer, daemon=True)
        thread.start()
        stop_event.wait(timeout=20)
        thread.join(timeout=5)
        db_conn.close()

        assert stop_event.is_set(), "Consumer did not process the event within 20 s"

        # Verify entity was written to the DB
        verify_conn = psycopg2.connect(dsn)
        try:
            with verify_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT entity_key, attributes->>'source_type' AS source_type
                    FROM metadata.entities
                    WHERE entity_type = 'DataSource'
                      AND entity_key = %s
                    """,
                    (entity_key,),
                )
                row = cur.fetchone()
        finally:
            verify_conn.close()

        assert row is not None, f"DataSource entity {entity_key!r} not found in DB"
        assert row[1] == "upload", f"Expected source_type=upload, got {row[1]!r}"
