import os


def _read_secret(file_path: str, env_var: str, default: str = "") -> str:
    """Read secret from mounted file; fall back to env var for local dev."""
    try:
        with open(file_path) as f:
            return f.read().strip()
    except OSError:
        return os.getenv(env_var, default)


class Config:
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_ttl_seconds: int = int(os.getenv("CACHE_TTL_SECONDS", "300"))

    milvus_host: str = os.getenv("MILVUS_HOST", "localhost")
    milvus_port: int = int(os.getenv("MILVUS_PORT", "19530"))

    embedding_backend: str = os.getenv("EMBEDDING_BACKEND", "local-cpu")
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
    embedding_dim: int = int(os.getenv("EMBEDDING_DIM", "384"))

    circuit_failure_threshold: int = int(os.getenv("CB_FAILURE_THRESHOLD", "5"))
    circuit_recovery_timeout: float = float(os.getenv("CB_RECOVERY_TIMEOUT", "30"))

    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "")
    kafka_usage_topic: str = os.getenv("KAFKA_USAGE_TOPIC", "usage-events")
    kafka_metadata_topic: str = os.getenv("KAFKA_METADATA_TOPIC", "metadata-events")


config = Config()
