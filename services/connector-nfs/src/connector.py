import logging
import mimetypes
import os
import queue
import time
from abc import ABC, abstractmethod
from typing import Iterator, Optional, Set

from prometheus_client import Counter
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from config import Config, config as default_config
from events import RawDocumentEvent
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


def _known_files_key(connector_id: str) -> str:
    return f"connector:{connector_id}:known_files"


class SourceConnector(ABC):
    @abstractmethod
    def poll(self) -> Iterator[RawDocumentEvent]: ...

    @abstractmethod
    def ack(self, event_id: str) -> None: ...


class _NFSEventHandler(FileSystemEventHandler):
    def __init__(self, file_queue: queue.Queue, cfg: Config):
        self._queue = file_queue
        self._cfg = cfg

    def on_created(self, event):
        if event.is_directory:
            return
        if self._cfg.is_allowed_extension(event.src_path):
            self._queue.put(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            return
        if self._cfg.is_allowed_extension(event.src_path):
            self._queue.put(event.src_path)


class NFSConnector(SourceConnector):
    def __init__(
        self,
        mount_path: str,
        kafka_producer,
        redis_client,
        db_conn,
        cfg: Config = None,
    ):
        self._mount_path = mount_path
        self._producer = kafka_producer
        self._redis = redis_client
        self._db = db_conn
        self._cfg = cfg or default_config
        self._file_queue: queue.Queue = queue.Queue()
        self._observer: Optional[Observer] = None
        self._start_observer()

    def _start_observer(self) -> None:
        try:
            handler = _NFSEventHandler(self._file_queue, self._cfg)
            observer = Observer()
            observer.schedule(handler, self._mount_path, recursive=True)
            observer.start()
            self._observer = observer
        except Exception as exc:
            logger.warning(
                "Failed to start watchdog observer: %s; using tree-diff fallback", exc
            )
            self._observer = None

    def poll(self) -> Iterator[RawDocumentEvent]:
        if self._observer is not None and self._observer.is_alive():
            paths = self._drain_queue()
        else:
            paths = self._tree_diff()

        for path in sorted(paths):
            event = self._make_event(path)
            if event is None:
                continue

            write_source_file_status(
                self._db, self._cfg.connector_id, path, event.event_id, "pending"
            )

            published = self._publish_with_retry(event)
            if not published:
                connector_errors_total.labels(reason="kafka_timeout").inc()
                logger.error("Skipping %s after max retries; not marking as known", path)
                continue

            self._redis.sadd(_known_files_key(self._cfg.connector_id), path)
            yield event

    def ack(self, event_id: str) -> None:
        pass

    def stop(self) -> None:
        if self._observer is not None:
            self._observer.stop()
            self._observer.join()

    def _drain_queue(self) -> Set[str]:
        paths: Set[str] = set()
        try:
            while True:
                paths.add(self._file_queue.get_nowait())
        except queue.Empty:
            pass
        return paths

    def _tree_diff(self) -> Set[str]:
        known_raw = self._redis.smembers(_known_files_key(self._cfg.connector_id))
        known: Set[str] = {
            p.decode("utf-8") if isinstance(p, bytes) else p
            for p in (known_raw or set())
        }
        current: Set[str] = set()
        for root, _, files in os.walk(self._mount_path):
            for fname in files:
                fpath = os.path.join(root, fname)
                if self._cfg.is_allowed_extension(fpath):
                    current.add(fpath)
        return current - known

    def _make_event(self, path: str) -> Optional[RawDocumentEvent]:
        if not os.path.isfile(path):
            return None
        if not self._cfg.is_allowed_extension(path):
            return None
        content_type, _ = mimetypes.guess_type(path)
        content_type = content_type or "application/octet-stream"
        try:
            rel_path = os.path.relpath(path, self._mount_path)
        except ValueError:
            rel_path = path
        source_id = f"nfs://{rel_path}"
        return RawDocumentEvent(
            source_type="nfs",
            source_id=source_id,
            content_ref=path,
            content_type=content_type,
            tenant_id=self._cfg.tenant_id,
            metadata={"path": path, "mount_path": self._mount_path},
        )

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
