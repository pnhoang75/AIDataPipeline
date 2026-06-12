import os


def _read_secret(file_path: str, env_var: str, default: str = "") -> str:
    """Read secret from mounted file; fall back to env var for local dev."""
    try:
        with open(file_path) as f:
            return f.read().strip()
    except OSError:
        return os.getenv(env_var, default)


class Config:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    kafka_input_topic: str = os.getenv("KAFKA_INPUT_TOPIC", "raw-documents")
    kafka_output_topic: str = os.getenv("KAFKA_OUTPUT_TOPIC", "document-chunks")
    kafka_dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "dlq-raw-documents")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "doc-processor")
    kafka_produce_timeout_ms: int = int(os.getenv("KAFKA_PRODUCE_TIMEOUT_MS", "5000"))
    kafka_max_poll_interval_ms: int = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))
    kafka_session_timeout_ms: int = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "45000"))

    chunk_size_tokens: int = int(os.getenv("CHUNK_SIZE_TOKENS", "512"))
    chunk_overlap_tokens: int = int(os.getenv("CHUNK_OVERLAP_TOKENS", "64"))

    postgres_dsn: str = _read_secret(
        "/etc/secrets/postgres-dsn",
        "POSTGRES_DSN",
        "postgresql://pipeline:pipeline@localhost:5432/pipeline",
    )

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = _read_secret("/etc/secrets/minio-access-key", "MINIO_ACCESS_KEY", "minio")
    minio_secret_key: str = _read_secret("/etc/secrets/minio-secret-key", "MINIO_SECRET_KEY", "minio")
    minio_secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"


config = Config()
