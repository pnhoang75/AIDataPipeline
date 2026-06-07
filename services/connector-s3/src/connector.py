import logging
import mimetypes
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Iterator, Optional

from prometheus_client import Counter

from config import Config, config as default_config
from events import RawDocumentEvent
from watermark import get_watermark, set_watermark
from status import write_source_file_status

logger = logging.getLogger(__name__)

connector_errors_total = Counter(
    "connector_errors_total",
    "Total connector errors",
    ["reason"],
)

_KAFKA_RETRY_BASE_MS = 100
_KAFKA_RETRY_CAP_MS = 30_000
_KAFKA_MAX_RETRIES = 5


class SourceConnector(ABC):
    @abstractmethod
    def poll(self) -> Iterator[RawDocumentEvent]: ...

    @abstractmethod
    def ack(self, event_id: str) -> None: ...


class S3Connector(SourceConnector):
    def __init__(
        self,
        minio_client,
        kafka_producer,
        redis_client,
        db_conn,
        cfg: Config = None,
    ):
        self._minio = minio_client
        self._producer = kafka_producer
        self._redis = redis_client
        self._db = db_conn
        self._cfg = cfg or default_config

    def poll(self) -> Iterator[RawDocumentEvent]:
        watermark = get_watermark(self._redis, self._cfg.connector_id)
        new_watermark: Optional[datetime] = watermark
        objects = self._minio.list_objects(self._cfg.minio_bucket, recursive=True)

        for obj in objects:
            last_modified: datetime = obj.last_modified
            if last_modified.tzinfo is None:
                last_modified = last_modified.replace(tzinfo=timezone.utc)

            if watermark is not None and last_modified <= watermark:
                continue

            content_type = self._resolve_content_type(obj)
            source_id = f"{self._cfg.minio_bucket}/{obj.object_name}"

            if content_type not in self._cfg.file_types:
                logger.info("Skipping %s: content_type %s not in allowlist", source_id, content_type)
                write_source_file_status(
                    self._db,
                    self._cfg.connector_id,
                    source_id,
                    "",
                    "error",
                    f"content_type {content_type} not in allowlist",
                )
                continue

            event = RawDocumentEvent(
                source_type="s3",
                source_id=source_id,
                content_ref=f"s3://{source_id}",
                content_type=content_type,
                tenant_id=self._cfg.tenant_id,
                metadata={"etag": obj.etag or "", "size": obj.size or 0},
            )

            write_source_file_status(
                self._db,
                self._cfg.connector_id,
                source_id,
                event.event_id,
                "pending",
            )

            published = self._publish_with_retry(event)
            if not published:
                connector_errors_total.labels(reason="kafka_timeout").inc()
                logger.error("Skipping %s after max retries; watermark not advanced", source_id)
                continue

            if new_watermark is None or last_modified > new_watermark:
                new_watermark = last_modified

            yield event

        if new_watermark is not None and new_watermark != watermark:
            set_watermark(self._redis, self._cfg.connector_id, new_watermark)

    def ack(self, event_id: str) -> None:
        pass

    def _publish_with_retry(self, event: RawDocumentEvent) -> bool:
        delay_ms = _KAFKA_RETRY_BASE_MS
        for attempt in range(_KAFKA_MAX_RETRIES):
            try:
                self._producer.produce(
                    self._cfg.kafka_topic,
                    key=event.event_id.encode(),
                    value=event.to_json().encode(),
                )
                self._producer.flush(timeout=self._cfg.kafka_produce_timeout_ms / 1000)
                return True
            except Exception as exc:
                logger.warning("Kafka produce attempt %d failed: %s", attempt + 1, exc)
                if attempt < _KAFKA_MAX_RETRIES - 1:
                    time.sleep(delay_ms / 1000)
                    delay_ms = min(delay_ms * 2, _KAFKA_RETRY_CAP_MS)
        return False

    def _resolve_content_type(self, obj) -> str:
        guessed, _ = mimetypes.guess_type(obj.object_name)
        return guessed or "application/octet-stream"
