import os


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

    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN", "postgresql://pipeline:pipeline@localhost:5432/pipeline"
    )

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minio")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minio")
    minio_secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"


config = Config()
