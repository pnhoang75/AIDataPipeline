import hashlib
import logging
import time
from typing import Callable, List, Optional

from prometheus_client import Counter

from chunker import Chunk, FixedSizeChunker
from config import Config, config as default_config
from events import DLQEnvelope, DocumentChunkEvent, RawDocumentEvent
from parsers import ParseError, parse
from status import update_source_file_status

logger = logging.getLogger(__name__)

messages_processed_total = Counter(
    "doc_processor_messages_processed_total",
    "Total messages processed",
    ["status"],
)
chunks_published_total = Counter(
    "doc_processor_chunks_published_total",
    "Total document chunks published",
)
dlq_routed_total = Counter(
    "doc_processor_dlq_routed_total",
    "Total messages routed to DLQ",
    ["reason"],
)

_CHUNK_PUBLISH_MAX_RETRIES = 5
_CHUNK_PUBLISH_RETRY_DELAY_S = 1.0


class DocumentProcessor:
    def __init__(
        self,
        consumer,
        producer,
        dlq_producer,
        content_fetcher: Callable[[str], bytes],
        db_conn=None,
        cfg: Config = None,
    ):
        self._consumer = consumer
        self._producer = producer
        self._dlq_producer = dlq_producer
        self._fetch_content = content_fetcher
        self._db = db_conn
        self._cfg = cfg or default_config
        self._chunker = FixedSizeChunker(
            chunk_size=self._cfg.chunk_size_tokens,
            overlap=self._cfg.chunk_overlap_tokens,
        )
        self._running = False

    def run(self, poll_timeout_s: float = 1.0) -> None:
        self._consumer.subscribe([self._cfg.kafka_input_topic])
        self._running = True
        try:
            while self._running:
                msg = self._consumer.poll(timeout=poll_timeout_s)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("Consumer error: %s", msg.error())
                    continue
                self._process_message(msg)
        finally:
            self._consumer.close()

    def stop(self) -> None:
        self._running = False

    def _process_message(self, msg) -> None:
        try:
            event = RawDocumentEvent.from_json(msg.value().decode("utf-8"))
        except Exception as exc:
            logger.error("Failed to deserialise message: %s", exc)
            self._consumer.commit(message=msg)
            return

        doc_id = _make_doc_id(event)

        try:
            content = self._fetch_content(event.content_ref)
        except Exception as exc:
            logger.error("Content fetch failed for %s: %s", event.source_id, exc)
            self._route_to_dlq(msg, event, "fetch_error", str(exc))
            self._consumer.commit(message=msg)
            messages_processed_total.labels(status="fetch_error").inc()
            return

        try:
            text = parse(content, event.content_type)
        except ParseError as exc:
            logger.error("Parse failed for %s: %s", event.source_id, exc)
            update_source_file_status(self._db, event.source_id, "error")
            self._route_to_dlq(msg, event, "parse_error", str(exc))
            self._consumer.commit(message=msg)
            messages_processed_total.labels(status="parse_error").inc()
            dlq_routed_total.labels(reason="parse_error").inc()
            return

        chunks = self._chunker.chunk(text, doc_id)
        if not chunks:
            logger.warning("No chunks produced for %s", event.source_id)
            self._consumer.commit(message=msg)
            messages_processed_total.labels(status="empty").inc()
            return

        published = self._publish_chunks(chunks, event)
        if not published:
            # Chunk publish exhausted all retries — route to DLQ, do NOT commit offset.
            self._route_to_dlq(msg, event, "chunk_publish_failed", "All publish retries exhausted")
            dlq_routed_total.labels(reason="chunk_publish_failed").inc()
            messages_processed_total.labels(status="chunk_publish_failed").inc()
            return

        self._consumer.commit(message=msg)
        messages_processed_total.labels(status="success").inc()
        chunks_published_total.inc(len(chunks))

    def _publish_chunks(self, chunks: List[Chunk], event: RawDocumentEvent) -> bool:
        total = len(chunks)
        for chunk in chunks:
            chunk_event = DocumentChunkEvent(
                doc_id=chunk.doc_id,
                chunk_id=chunk.chunk_id,
                chunk_index=chunk.index,
                total_chunks=total,
                text=chunk.text,
                source_type=event.source_type,
                source_id=event.source_id,
                content_type=event.content_type,
                tenant_id=event.tenant_id,
                metadata=event.metadata,
            )
            if not self._produce_with_retry(
                self._producer,
                self._cfg.kafka_output_topic,
                key=chunk.chunk_id,
                value=chunk_event.to_json(),
            ):
                return False

        try:
            self._producer.flush(timeout=self._cfg.kafka_produce_timeout_ms / 1000)
        except Exception as exc:
            logger.error("Producer flush failed: %s", exc)
            return False

        return True

    def _produce_with_retry(self, producer, topic: str, key: str, value: str) -> bool:
        for attempt in range(_CHUNK_PUBLISH_MAX_RETRIES):
            try:
                producer.produce(topic, key=key.encode(), value=value.encode())
                return True
            except Exception as exc:
                logger.warning("Produce attempt %d/%d failed: %s", attempt + 1, _CHUNK_PUBLISH_MAX_RETRIES, exc)
                if attempt < _CHUNK_PUBLISH_MAX_RETRIES - 1:
                    time.sleep(_CHUNK_PUBLISH_RETRY_DELAY_S)
        return False

    def _route_to_dlq(self, msg, event: RawDocumentEvent, reason: str, detail: str) -> None:
        envelope = DLQEnvelope(
            original_topic=msg.topic() or self._cfg.kafka_input_topic,
            original_partition=msg.partition() if msg.partition() is not None else 0,
            original_offset=msg.offset() if msg.offset() is not None else 0,
            original_timestamp=(
                int(msg.timestamp()[1] / 1000) if msg.timestamp() else int(time.time())
            ),
            failure_reason=reason,
            failure_detail=detail,
            original_payload=event.to_dict(),
        )
        try:
            self._dlq_producer.produce(
                self._cfg.kafka_dlq_topic,
                key=event.event_id.encode(),
                value=envelope.to_json().encode(),
            )
            self._dlq_producer.flush(timeout=self._cfg.kafka_produce_timeout_ms / 1000)
        except Exception as exc:
            logger.error("Failed to route message to DLQ: %s", exc)


def _make_doc_id(event: RawDocumentEvent) -> str:
    return hashlib.sha256(
        f"{event.source_type}:{event.source_id}:{event.event_id}".encode()
    ).hexdigest()[:32]
