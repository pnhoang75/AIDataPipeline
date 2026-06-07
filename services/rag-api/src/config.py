import os


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


config = Config()
