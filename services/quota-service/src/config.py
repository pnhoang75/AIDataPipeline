"""Runtime configuration loaded from environment variables."""
import os
from dataclasses import dataclass, field


@dataclass
class Config:
    redis_host: str = field(default_factory=lambda: os.environ.get("REDIS_HOST", "localhost"))
    redis_port: int = field(default_factory=lambda: int(os.environ.get("REDIS_PORT", "6379")))
    redis_db: int = field(default_factory=lambda: int(os.environ.get("REDIS_DB", "0")))
    quota_db_url: str = field(
        default_factory=lambda: os.environ.get(
            "QUOTA_DB_URL",
            "postgresql+psycopg2://quota:quota@localhost:5432/quota",
        )
    )
    grpc_port: int = field(default_factory=lambda: int(os.environ.get("GRPC_PORT", "50051")))
    grpc_workers: int = field(default_factory=lambda: int(os.environ.get("GRPC_WORKERS", "4")))
    http_port: int = field(default_factory=lambda: int(os.environ.get("HTTP_PORT", "8080")))
    usage_flush_interval_secs: int = field(
        default_factory=lambda: int(os.environ.get("USAGE_FLUSH_INTERVAL_SECS", "60"))
    )

    @property
    def static_limits(self) -> dict:
        """Limits from QUOTA_STATIC_LIMIT_<METRIC>=<int> env vars (testbed override)."""
        result = {}
        for key, val in os.environ.items():
            if key.startswith("QUOTA_STATIC_LIMIT_"):
                metric = key[len("QUOTA_STATIC_LIMIT_"):]
                try:
                    result[metric] = int(val)
                except ValueError:
                    pass
        return result
