import os
from typing import List


class Config:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "raw-documents")
    kafka_produce_timeout_ms: int = int(os.getenv("KAFKA_PRODUCE_TIMEOUT_MS", "5000"))

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    minio_bucket: str = os.getenv("MINIO_BUCKET", "documents")

    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis_connect_timeout_ms: int = int(os.getenv("REDIS_CONNECT_TIMEOUT_MS", "500"))
    redis_op_timeout_ms: int = int(os.getenv("REDIS_OP_TIMEOUT_MS", "200"))

    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN", "postgresql://pipeline:pipeline@localhost:5432/pipeline"
    )

    connector_id: str = os.getenv("CONNECTOR_ID", "default")
    tenant_id: str = os.getenv("TENANT_ID", "default")
    poll_interval_seconds: float = float(os.getenv("POLL_INTERVAL_SECONDS", "30"))

    @property
    def file_types(self) -> List[str]:
        raw = os.getenv("FILE_TYPES", "application/pdf,text/plain,text/html,application/json,text/csv")
        return [ft.strip() for ft in raw.split(",") if ft.strip()]


config = Config()
