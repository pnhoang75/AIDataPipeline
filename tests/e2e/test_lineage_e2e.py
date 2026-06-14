"""E2E test: PDF ingestion lineage traces back to DataSource.

Exercises the complete lineage chain that the metadata pipeline builds:

  DataSource ──discovered_in──► RawDocument ──chunked_into──► DocumentChunk

Verifies that after a simulated PDF ingestion the Metadata Service's upstream
endpoint returns the full chain: DocumentChunk → RawDocument → DataSource with
the correct source_path attribute.

Run with:
    pytest tests/e2e/test_lineage_e2e.py -x --tb=short -q
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path
from urllib.parse import quote

import psycopg2
import pytest

testcontainers = pytest.importorskip("testcontainers", reason="testcontainers not installed")
from testcontainers.postgres import PostgresContainer  # noqa: E402

_ROOT = Path(__file__).parent.parent.parent
_META_SRC = str(_ROOT / "services" / "metadata-service" / "src")

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


def _insert_entity(conn, entity_type, entity_key, tenant_id, attributes=None) -> str:
    attrs = json.dumps(attributes or {})
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.entities
                (entity_type, entity_key, tenant_id, attributes)
            VALUES (%s, %s, %s::uuid, %s::jsonb)
            ON CONFLICT (tenant_id, entity_type, entity_key)
            DO UPDATE SET attributes = EXCLUDED.attributes
            RETURNING id
            """,
            (entity_type, entity_key, tenant_id, attrs),
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
def metadata_client(_pg):
    """FastAPI TestClient wired to the test PostgreSQL instance."""
    from fastapi.testclient import TestClient

    if _META_SRC not in sys.path:
        sys.path.insert(0, _META_SRC)
    for _m in ["config", "app", "db", "consumer", "events"]:
        sys.modules.pop(_m, None)

    os.environ["DATABASE_URL"] = _pg.get_connection_url()
    os.environ["KAFKA_BOOTSTRAP"] = ""  # disable consumer thread

    import app as _app_mod

    with TestClient(_app_mod.app, raise_server_exceptions=True) as client:
        yield client, _pg.get_connection_url()


# ── E2E test ────────────────────────────────────────────────────────────────────

def test_e2e_pdf_lineage_traces_to_datasource(metadata_client):
    """After ingesting a PDF, upstream lineage from a DocumentChunk reaches the DataSource.

    Simulates the metadata events emitted by the full ingestion pipeline:
      1. S3 connector emits DataSource entity on startup
      2. S3 connector emits RawDocument + discovered_in edge on file discovery
      3. Doc-processor emits DocumentChunk + chunked_into edge on chunking
    Then verifies GET /api/lineage/upstream/{chunk_key} returns the complete chain.
    """
    client, dsn = metadata_client
    tenant_id = str(uuid.uuid4())
    source_path = f"s3://pipeline-bucket/{tenant_id}/annual-report.pdf"

    conn = psycopg2.connect(dsn)
    try:
        # Step 1 — S3 connector: DataSource entity
        ds_id = _insert_entity(
            conn, "DataSource", f"s3://pipeline-bucket/{tenant_id}/",
            tenant_id,
            {
                "source_type": "s3",
                "endpoint": f"s3://pipeline-bucket/{tenant_id}/",
                "source_path": f"s3://pipeline-bucket/{tenant_id}/",
            },
        )

        # Step 2 — S3 connector: RawDocument entity + discovered_in edge
        doc_key = f"sha256:pdf:{tenant_id}"
        raw_id = _insert_entity(
            conn, "RawDocument", doc_key, tenant_id,
            {
                "source_path": source_path,
                "content_type": "application/pdf",
                "file_size_bytes": 204800,
            },
        )
        _insert_lineage(conn, ds_id, raw_id, "discovered_in")

        # Step 3 — Doc-processor: DocumentChunk entities + chunked_into edges
        chunk_keys = [f"{doc_key}:{i}" for i in range(3)]
        chunk_ids = []
        for i, ck in enumerate(chunk_keys):
            cid = _insert_entity(
                conn, "DocumentChunk", ck, tenant_id,
                {
                    "doc_id": doc_key,
                    "chunk_index": i,
                    "text_preview": f"Annual report excerpt page {i + 1}",
                },
            )
            _insert_lineage(conn, raw_id, cid, "chunked_into")
            chunk_ids.append(cid)

        conn.commit()
    finally:
        conn.close()

    # ── Assert: upstream from any chunk reaches DataSource ──────────────────────
    target_chunk_key = chunk_keys[0]
    encoded = quote(target_chunk_key, safe="")
    resp = client.get(f"/api/lineage/upstream/{encoded}")
    assert resp.status_code == 200, resp.text

    chain = resp.json()
    entity_types = {r["entity_type"] for r in chain}

    assert "DocumentChunk" in entity_types, f"DocumentChunk missing from chain: {chain}"
    assert "RawDocument" in entity_types, f"RawDocument missing from chain: {chain}"
    assert "DataSource" in entity_types, f"DataSource missing from chain: {chain}"

    # DataSource must carry the source_path in attributes
    ds_row = next(r for r in chain if r["entity_type"] == "DataSource")
    assert ds_row["attributes"].get("source_path"), "DataSource missing source_path attribute"

    # RawDocument must carry the PDF source_path
    raw_row = next(r for r in chain if r["entity_type"] == "RawDocument")
    assert raw_row["attributes"].get("source_path") == source_path, (
        f"RawDocument source_path mismatch: {raw_row['attributes'].get('source_path')!r}"
    )

    # Depth ordering: DataSource is furthest upstream (highest depth)
    ds_depth = ds_row["depth"]
    raw_depth = next(r["depth"] for r in chain if r["entity_type"] == "RawDocument")
    assert ds_depth > raw_depth, (
        f"DataSource depth {ds_depth} should be > RawDocument depth {raw_depth}"
    )
