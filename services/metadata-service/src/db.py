import json
import logging
import uuid as _uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import psycopg2.extras

logger = logging.getLogger(__name__)


def _row_to_dict(row) -> Dict[str, Any]:
    """Convert a psycopg2 row (dict-like) to a JSON-serializable dict."""
    result: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, _uuid.UUID):
            result[k] = str(v)
        elif isinstance(v, datetime):
            result[k] = v.isoformat()
        elif isinstance(v, Decimal):
            result[k] = float(v)
        elif isinstance(v, list):
            result[k] = [str(x) if isinstance(x, _uuid.UUID) else x for x in v]
        else:
            result[k] = v
    return result


def upsert_entity(
    conn,
    entity_type: str,
    entity_key: str,
    tenant_id: str,
    attributes: Dict[str, Any],
    pipeline_run_id: Optional[str] = None,
    schema_version_id: Optional[str] = None,
) -> str:
    """Upsert entity into metadata.entities; return the entity UUID string."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.entities
                (entity_type, entity_key, tenant_id, attributes,
                 pipeline_run_id, schema_version_id)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s)
            ON CONFLICT (tenant_id, entity_type, entity_key)
            DO UPDATE SET
                attributes = EXCLUDED.attributes,
                updated_at = now(),
                is_current = TRUE
            RETURNING id
            """,
            (
                entity_type,
                entity_key,
                tenant_id,
                json.dumps(attributes),
                pipeline_run_id,
                schema_version_id,
            ),
        )
        return str(cur.fetchone()[0])


def insert_lineage_edge(
    conn,
    upstream_type: str,
    upstream_key: str,
    tenant_id: str,
    downstream_id: str,
    relationship: str,
    pipeline_run_id: Optional[str] = None,
) -> None:
    """Insert a lineage edge from upstream entity to downstream entity."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM metadata.entities WHERE tenant_id = %s AND entity_type = %s AND entity_key = %s",
            (tenant_id, upstream_type, upstream_key),
        )
        row = cur.fetchone()
        if row is None:
            logger.warning("upstream entity not found: %s/%s for tenant %s", upstream_type, upstream_key, tenant_id)
            return
        upstream_id = str(row[0])
        cur.execute(
            """
            INSERT INTO metadata.lineage
                (upstream_id, downstream_id, relationship, pipeline_run_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (upstream_id, downstream_id, relationship) DO NOTHING
            """,
            (upstream_id, downstream_id, relationship, pipeline_run_id),
        )


def insert_quality_checks(
    conn,
    entity_id: str,
    run_id: Optional[str],
    quality_checks: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Insert quality check rows; return list of checks with status 'failed'."""
    failed = []
    with conn.cursor() as cur:
        for check in quality_checks:
            cur.execute(
                """
                INSERT INTO metadata.data_quality
                    (entity_id, run_id, check_name, status, value, threshold, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    entity_id,
                    run_id,
                    check["check_name"],
                    check["status"],
                    check.get("value"),
                    check.get("threshold"),
                    check.get("message"),
                ),
            )
            if check["status"] == "failed":
                failed.append(check)
    return failed


def update_entity_quality_failed(conn, entity_id: str) -> None:
    """Set quality_status=failed in the entity's attributes JSON."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE metadata.entities
            SET attributes = attributes || '{"quality_status": "failed"}'::jsonb,
                updated_at = now()
            WHERE id = %s
            """,
            (entity_id,),
        )


def query_upstream(conn, chunk_id: str) -> List[Dict[str, Any]]:
    """Recursive CTE: walk upstream from entity_key through lineage edges."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            WITH RECURSIVE upstream_chain AS (
                SELECT e.id, e.entity_type, e.entity_key, e.attributes,
                       NULL::TEXT AS relationship, 0 AS depth
                FROM metadata.entities e
                WHERE e.entity_key = %s

                UNION ALL

                SELECT parent.id, parent.entity_type, parent.entity_key, parent.attributes,
                       l.relationship, chain.depth + 1
                FROM metadata.entities parent
                JOIN metadata.lineage l ON l.upstream_id = parent.id
                JOIN upstream_chain chain ON l.downstream_id = chain.id
                WHERE chain.depth < 5
            )
            SELECT entity_type, entity_key, attributes,
                   relationship, depth
            FROM upstream_chain
            ORDER BY depth DESC
            """,
            (chunk_id,),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]


def query_downstream(conn, tenant_id: str, source_path: str) -> List[Dict[str, Any]]:
    """Recursive CTE: walk downstream from a RawDocument identified by source_path."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            WITH RECURSIVE downstream AS (
                SELECT e.id, e.entity_type, e.entity_key, 0 AS depth
                FROM metadata.entities e
                WHERE e.entity_type = 'RawDocument'
                  AND e.attributes->>'source_path' = %s
                  AND e.tenant_id = %s::uuid

                UNION ALL

                SELECT child.id, child.entity_type, child.entity_key, d.depth + 1
                FROM metadata.entities child
                JOIN metadata.lineage l ON l.downstream_id = child.id
                JOIN downstream d ON l.upstream_id = d.id
                WHERE d.depth < 4
            )
            SELECT entity_type,
                   COUNT(*)             AS count,
                   ARRAY_AGG(entity_key) AS entity_keys
            FROM downstream
            WHERE depth > 0
            GROUP BY entity_type
            """,
            (source_path, tenant_id),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]


def query_stale(conn, tenant_id: str) -> List[Dict[str, Any]]:
    """Return embeddings that were generated with an outdated schema version."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT e.entity_key AS embedding_id,
                   e.attributes->>'chunk_id'   AS chunk_id,
                   e.attributes->>'model_name' AS model_name,
                   sv.version_number           AS schema_version,
                   sv.embedding_model          AS old_model,
                   cv.embedding_model          AS current_model
            FROM metadata.entities e
            JOIN metadata.schema_versions sv ON sv.id = e.schema_version_id
            JOIN metadata.schema_versions cv ON cv.tenant_id = sv.tenant_id
                                             AND cv.is_current = TRUE
            WHERE e.entity_type = 'Embedding'
              AND e.tenant_id = %s::uuid
              AND sv.id <> cv.id
            ORDER BY sv.version_number
            """,
            (tenant_id,),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]


def query_provenance(conn, query_id: str) -> List[Dict[str, Any]]:
    """Return full provenance for each retrieved chunk for a RAG query."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                qr.rank,
                qr.score,
                chunk.entity_key                   AS chunk_id,
                chunk.attributes->>'text_preview'  AS text_preview,
                chunk.attributes->>'page_number'   AS page,
                doc.attributes->>'source_path'     AS source_file,
                doc.attributes->>'content_type'    AS format,
                sv.embedding_model                 AS embedding_model,
                sv.chunk_size                      AS chunk_size,
                pr.started_at                      AS indexed_at
            FROM metadata.query_results qr
            JOIN metadata.entities chunk   ON chunk.id = qr.chunk_entity_id
            JOIN metadata.lineage l_chunk  ON l_chunk.downstream_id = chunk.id
                                          AND l_chunk.relationship = 'chunked_into'
            JOIN metadata.entities doc     ON doc.id = l_chunk.upstream_id
            JOIN metadata.schema_versions sv ON sv.id = chunk.schema_version_id
            JOIN metadata.pipeline_runs pr ON pr.id = chunk.pipeline_run_id
            WHERE qr.query_id = %s::uuid
            ORDER BY qr.rank
            """,
            (query_id,),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]


def query_runs(conn, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return pipeline run history, optionally filtered by tenant."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, tenant_id, pipeline_type, connector_id, status,
                   started_at, finished_at, entities_processed,
                   entities_failed, bytes_processed
            FROM metadata.pipeline_runs
            WHERE (%s IS NULL OR tenant_id = %s::uuid)
            ORDER BY started_at DESC
            LIMIT 100
            """,
            (tenant_id, tenant_id),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]


def create_schema_version(
    conn,
    tenant_id: str,
    embedding_model: str,
    embedding_dimension: int,
    embedding_backend: str,
    chunk_size: int = 512,
    chunk_overlap: int = 64,
    chunking_strategy: str = "fixed",
    index_type: str = "IVF_FLAT",
    created_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Deactivate previous schema versions for the tenant and insert a new current one."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "UPDATE metadata.schema_versions SET is_current = FALSE WHERE tenant_id = %s::uuid",
            (tenant_id,),
        )
        cur.execute(
            """
            INSERT INTO metadata.schema_versions
                (tenant_id, version_number, chunk_size, chunk_overlap,
                 chunking_strategy, embedding_model, embedding_dimension,
                 embedding_backend, index_type, is_current, created_by)
            VALUES (
                %s::uuid,
                COALESCE(
                    (SELECT MAX(version_number) FROM metadata.schema_versions WHERE tenant_id = %s::uuid),
                    0
                ) + 1,
                %s, %s, %s, %s, %s, %s, %s, TRUE, %s
            )
            RETURNING id, version_number, embedding_model, embedding_dimension, is_current
            """,
            (
                tenant_id, tenant_id,
                chunk_size, chunk_overlap, chunking_strategy,
                embedding_model, embedding_dimension, embedding_backend,
                index_type, created_by,
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return _row_to_dict(row)


def query_quality(conn, tenant_id: str) -> List[Dict[str, Any]]:
    """Return failed/warned quality checks for a tenant."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT dq.id, dq.entity_id, dq.check_name, dq.status,
                   dq.value, dq.threshold, dq.message, dq.checked_at,
                   e.entity_type, e.entity_key
            FROM metadata.data_quality dq
            JOIN metadata.entities e ON e.id = dq.entity_id
            WHERE e.tenant_id = %s::uuid
              AND dq.status IN ('failed', 'warning')
            ORDER BY dq.checked_at DESC
            """,
            (tenant_id,),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
