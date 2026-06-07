import os
from typing import List


class Config:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "raw-documents")
    kafka_produce_timeout_ms: int = int(os.getenv("KAFKA_PRODUCE_TIMEOUT_MS", "5000"))

    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis_connect_timeout_ms: int = int(os.getenv("REDIS_CONNECT_TIMEOUT_MS", "500"))
    redis_op_timeout_ms: int = int(os.getenv("REDIS_OP_TIMEOUT_MS", "200"))

    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN", "postgresql://pipeline:pipeline@localhost:5432/pipeline"
    )

    connector_id: str = os.getenv("CONNECTOR_ID", "default")
    tenant_id: str = os.getenv("TENANT_ID", "default")
    poll_interval_seconds: float = float(os.getenv("POLL_INTERVAL_SECONDS", "30"))
    nfs_mount_path: str = os.getenv("NFS_MOUNT_PATH", "/mnt/nfs")

    @property
    def allowed_extensions(self) -> List[str]:
        raw = os.getenv("ALLOWED_EXTENSIONS", ".pdf,.txt,.html,.json,.csv,.docx")
        return [ext.strip().lower() for ext in raw.split(",") if ext.strip()]

    def is_allowed_extension(self, path: str) -> bool:
        ext = os.path.splitext(path)[1].lower()
        return ext in self.allowed_extensions


config = Config()
