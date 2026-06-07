import logging

logger = logging.getLogger(__name__)


def update_source_file_status(
    db_conn, source_id: str, status: str, chunk_count: int = 0
) -> None:
    with db_conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE source_file_status
            SET ingest_status = %s, chunk_count = %s, updated_at = NOW()
            WHERE source_id = %s
            """,
            (status, chunk_count, source_id),
        )
    db_conn.commit()
