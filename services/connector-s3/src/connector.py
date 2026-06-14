import logging
import mimetypes
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Iterator, Optional

from opentelemetry import trace
from prometheus_client import Counter, REGISTRY

from config import Config, config as default_config
from events import RawDocumentEvent
from metadata_event import MetadataEventPublisher
from watermark import get_watermark, set_watermark
from status import write_source_file_status

logger = logging.getLogger(__name__)
_tracer = trace.get_tracer(__name__)

try:
    connector_errors_total = Counter(
        "connector_errors_total",
        "Total connector errors",
        ["reason"],
    )
except ValueError:
    # Already registered — multiple connectors imported in the same process (e.g. tests).
    connector_errors_total = REGISTRY._names_to_collectors["connector_errors_total"]

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
        metadata_producer=None,
    ):
        self._minio = minio_client
        self._producer = kafka_producer
        self._redis = redis_client
        self._db = db_conn
        self._cfg = cfg or default_config
        self._datasource_key = f"{self._cfg.tenant_id}/s3/{self._cfg.minio_bucket}"

        self._metadata_publisher: Optional[MetadataEventPublisher] = None
        if metadata_producer is not None:
            self._metadata_publisher = MetadataEventPublisher(
                producer=metadata_producer,
                topic=self._cfg.metadata_events_topic,
                connector_id=self._cfg.connector_id,
                tenant_id=self._cfg.tenant_id,
            )
            self._metadata_publisher.publish_datasource(
                entity_key=self._datasource_key,
                attributes={
                    "source_type": "s3",
                    "endpoint": f"s3://{self._cfg.minio_bucket}/",
                    "connector_id": self._cfg.connector_id,
                    "file_types": self._cfg.file_types,
                },
            )

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

            if self._metadata_publisher is not None:
                self._metadata_publisher.publish_rawdocument(
                    entity_key=event.content_ref,
                    attributes={
                        "source_path": event.content_ref,
                        "content_type": content_type,
                        "size_bytes": obj.size or 0,
                        "etag": obj.etag or "",
                    },
                    datasource_entity_key=self._datasource_key,
                )

            yield event

        if new_watermark is not None and new_watermark != watermark:
            set_watermark(self._redis, self._cfg.connector_id, new_watermark)

    def ack(self, event_id: str) -> None:
        pass

    def _publish_with_retry(self, event: RawDocumentEvent) -> bool:
        delay_ms = _KAFKA_RETRY_BASE_MS
        for attempt in range(_KAFKA_MAX_RETRIES):
            try:
                with _tracer.start_as_current_span("kafka.produce") as span:
                    span.set_attribute("messaging.system", "kafka")
                    span.set_attribute("messaging.destination", self._cfg.kafka_topic)
                    span.set_attribute("messaging.operation", "publish")
                    span.set_attribute("messaging.kafka.message_key", event.event_id)
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
