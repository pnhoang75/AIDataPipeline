import os


class Config:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    kafka_input_topic: str = os.getenv("KAFKA_INPUT_TOPIC", "document-chunks")
    kafka_event_topic: str = os.getenv("KAFKA_EVENT_TOPIC", "embedding-events")
    kafka_dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "dlq-document-chunks")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "embedding-worker")
    kafka_produce_timeout_ms: int = int(os.getenv("KAFKA_PRODUCE_TIMEOUT_MS", "5000"))

    embedding_backend: str = os.getenv("EMBEDDING_BACKEND", "local-cpu")
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
    embedding_dim: int = int(os.getenv("EMBEDDING_DIM", "384"))
    embedding_batch_size: int = int(os.getenv("EMBEDDING_BATCH_SIZE", "32"))
    embedding_batch_timeout_ms: int = int(os.getenv("EMBEDDING_BATCH_TIMEOUT_MS", "500"))

    milvus_host: str = os.getenv("MILVUS_HOST", "localhost")
    milvus_port: int = int(os.getenv("MILVUS_PORT", "19530"))
    milvus_collection: str = os.getenv("MILVUS_COLLECTION", "documents")

    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN", "postgresql://pipeline:pipeline@localhost:5432/pipeline"
    )

    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_embedding_model: str = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
    openai_embedding_dim: int = int(os.getenv("OPENAI_EMBEDDING_DIM", "1536"))


config = Config()
