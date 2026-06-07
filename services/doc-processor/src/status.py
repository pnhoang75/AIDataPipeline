import logging

logger = logging.getLogger(__name__)


def update_source_file_status(db_conn, source_id: str, status: str) -> None:
    if db_conn is None:
        return
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                "UPDATE source_file_status SET ingest_status = %s WHERE source_id = %s",
                (status, source_id),
            )
        db_conn.commit()
    except Exception as exc:
        logger.warning("Failed to update source_file_status for %s: %s", source_id, exc)
