from datetime import datetime, timezone


def write_source_file_status(
    db_conn,
    connector_id: str,
    source_id: str,
    event_id: str,
    ingest_status: str,
    error_message: str = None,
) -> None:
    now = datetime.now(timezone.utc)
    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source_file_status
                (connector_id, source_id, event_id, ingest_status, error_message, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (connector_id, source_id) DO UPDATE SET
                event_id = EXCLUDED.event_id,
                ingest_status = EXCLUDED.ingest_status,
                error_message = EXCLUDED.error_message,
                updated_at = EXCLUDED.updated_at
            """,
            (connector_id, source_id, event_id, ingest_status, error_message, now, now),
        )
    db_conn.commit()
