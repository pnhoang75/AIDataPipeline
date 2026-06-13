import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


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
