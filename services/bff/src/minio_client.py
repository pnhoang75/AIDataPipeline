import logging
import os

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio.infrastructure.svc:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "pipeline-uploads")


async def upload_file(tenant_id: str, session_id: str, filename: str, content: bytes) -> str:
    """Upload file content to MinIO. Returns the object path."""
    object_name = f"{tenant_id}/uploads/{session_id}/{filename}"
    try:
        import io

        from miniopy_async import Minio

        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        await client.put_object(MINIO_BUCKET, object_name, io.BytesIO(content), len(content))
        logger.info("Uploaded %s bytes → %s/%s", len(content), MINIO_BUCKET, object_name)
    except ImportError:
        logger.warning("miniopy-async not available; skipping MinIO upload for %s", object_name)
    return object_name
