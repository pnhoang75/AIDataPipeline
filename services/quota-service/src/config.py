"""Runtime configuration loaded from environment variables."""
import os
from dataclasses import dataclass, field


def _read_secret(file_path: str, env_var: str, default: str = "") -> str:
    """Read secret from mounted file; fall back to env var for local dev."""
    try:
        with open(file_path) as f:
            return f.read().strip()
    except OSError:
        return os.environ.get(env_var, default)


def _build_db_url() -> str:
    # Prefer a complete URL from file (CNPG secret contains 'uri' key with full DSN).
    url_from_file = _read_secret("/etc/secrets/quota-db/uri", "QUOTA_DB_URL", "")
    if url_from_file:
        # CNPG uri uses postgres:// scheme; SQLAlchemy requires postgresql+psycopg2://
        return url_from_file.replace("postgres://", "postgresql+psycopg2://", 1).replace(
            "postgresql://", "postgresql+psycopg2://", 1
        )
    return "postgresql+psycopg2://quota:quota@localhost:5432/quota"


@dataclass
class Config:
    redis_host: str = field(default_factory=lambda: os.environ.get("REDIS_HOST", "localhost"))
    redis_port: int = field(default_factory=lambda: int(os.environ.get("REDIS_PORT", "6379")))
    redis_db: int = field(default_factory=lambda: int(os.environ.get("REDIS_DB", "0")))
    quota_db_url: str = field(default_factory=_build_db_url)
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
