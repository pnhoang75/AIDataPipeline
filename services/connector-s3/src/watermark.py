from datetime import datetime, timezone
from typing import Optional


WATERMARK_FIELD = "last_seen"


def _key(connector_id: str) -> str:
    return f"connector:{connector_id}:watermark"


def get_watermark(redis_client, connector_id: str) -> Optional[datetime]:
    raw = redis_client.hget(_key(connector_id), WATERMARK_FIELD)
    if raw is None:
        return None
    ts = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    return datetime.fromisoformat(ts)


def set_watermark(redis_client, connector_id: str, ts: datetime) -> None:
    redis_client.hset(_key(connector_id), WATERMARK_FIELD, ts.isoformat())
