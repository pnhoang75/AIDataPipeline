import logging
import time
from collections import defaultdict
from typing import Dict, List, Tuple

from backends import EmbeddingBackend, RateLimitError
from config import Config
from events import DocumentChunkEvent, DLQEnvelope, EmbeddingEvent
from milvus_writer import MilvusWriter
from status_updater import update_source_file_status

logger = logging.getLogger(__name__)


class EmbeddingWorker:
    def __init__(
        self,
        consumer,
        backend: EmbeddingBackend,
        milvus_writer: MilvusWriter,
        producer,
        dlq_producer,
        db_conn,
        cfg: Config,
    ):
        self._consumer = consumer
        self._backend = backend
        self._milvus_writer = milvus_writer
        self._producer = producer
        self._dlq_producer = dlq_producer
        self._db_conn = db_conn
        self._cfg = cfg
        self._running = False

    def _send_to_dlq(
        self, msg, chunk: DocumentChunkEvent, reason: str, detail: str
    ) -> None:
        envelope = DLQEnvelope(
            original_topic=msg.topic(),
            original_partition=msg.partition(),
            original_offset=msg.offset(),
            original_timestamp=msg.timestamp()[1],
            failure_reason=reason,
            failure_detail=detail,
            original_payload=chunk.to_dict(),
        )
        self._dlq_producer.produce(
            topic=self._cfg.kafka_dlq_topic,
            value=envelope.to_json().encode(),
        )
        self._dlq_producer.flush(self._cfg.kafka_produce_timeout_ms / 1000)

    def _publish_embedding_event(
        self, chunk: DocumentChunkEvent, chunk_count: int
    ) -> None:
        evt = EmbeddingEvent(
            doc_id=chunk.doc_id,
            source_id=chunk.source_id,
            source_type=chunk.source_type,
            tenant_id=chunk.tenant_id,
            chunk_count=chunk_count,
        )
        self._producer.produce(
            topic=self._cfg.kafka_event_topic,
            key=chunk.doc_id.encode(),
            value=evt.to_json().encode(),
            headers={"tenant_id": chunk.tenant_id},
        )

    def _process_batch(self, batch: List[Tuple]) -> None:
        """Embed, upsert, publish events, commit — or route to DLQ on failure."""
        if not batch:
            return

        messages = [b[0] for b in batch]
        chunks = [b[1] for b in batch]
        texts = [c.text for c in chunks]

        # Attempt embedding with one retry; handle rate limiting separately.
        embeddings = None
        for attempt in range(2):
            try:
                embeddings = self._backend.embed_batch(texts)
                break
            except RateLimitError as e:
                if attempt == 0:
                    logger.warning(
                        "Embedding backend rate limited; sleeping %.1f s", e.retry_after
                    )
                    time.sleep(e.retry_after)
                else:
                    logger.error("Embedding rate limit persists; routing batch to DLQ")
                    for msg, chunk in zip(messages, chunks):
                        self._send_to_dlq(msg, chunk, "embedding_rate_limit", str(e))
                    return
            except Exception as e:
                if attempt == 0:
                    logger.warning("Embedding failed (attempt 1): %s — retrying", e)
                    continue
                logger.error("Embedding failed after retry: %s", e)
                for msg, chunk in zip(messages, chunks):
                    self._send_to_dlq(msg, chunk, "embedding_error", str(e))
                return

        if embeddings is None:
            return

        rows = [
            {
                "doc_id": chunk.doc_id,
                "chunk_id": chunk.chunk_id,
                "source_type": chunk.source_type,
                "text": chunk.text,
                "embedding": embedding,
                "created_at": int(chunk.created_at),
                "metadata": {**chunk.metadata, "tenant_id": chunk.tenant_id},
                "tenant_id": chunk.tenant_id,
            }
            for chunk, embedding in zip(chunks, embeddings)
        ]

        # Group rows by tenant and write to per-tenant Milvus collections.
        tenant_rows: Dict[str, List[dict]] = defaultdict(list)
        for row in rows:
            tenant_rows[row["tenant_id"]].append(row)

        for t_id, t_rows in tenant_rows.items():
            collection = f"{t_id}_docs"
            try:
                self._milvus_writer.upsert(t_rows, collection=collection)
            except Exception as e:
                logger.error("Milvus upsert failed for tenant %s: %s", t_id, e)
                for msg, chunk in zip(messages, chunks):
                    self._send_to_dlq(msg, chunk, "milvus_error", str(e))
                return

        # Success path: publish completion events, update Postgres, commit offsets.
        for chunk in chunks:
            self._publish_embedding_event(chunk, len(chunks))
            try:
                update_source_file_status(
                    self._db_conn, chunk.source_id, "indexed", len(chunks)
                )
            except Exception as e:
                logger.warning("Status update failed for %s: %s", chunk.source_id, e)

        self._producer.flush(self._cfg.kafka_produce_timeout_ms / 1000)

        for msg in messages:
            self._consumer.commit(message=msg)

    def _collect_batch(self) -> List[Tuple]:
        """Poll Kafka until batch_size reached or timeout_ms elapses."""
        batch: List[Tuple] = []
        deadline = time.time() + self._cfg.embedding_batch_timeout_ms / 1000

        while len(batch) < self._cfg.embedding_batch_size:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            msg = self._consumer.poll(timeout=min(remaining, 0.05))
            if msg is None:
                continue
            if msg.error():
                logger.warning("Consumer error: %s", msg.error())
                continue
            try:
                chunk = DocumentChunkEvent.from_json(msg.value().decode())
                batch.append((msg, chunk))
            except Exception as e:
                logger.error("Failed to deserialise chunk event: %s", e)

        return batch

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        self._running = True
        logger.info("EmbeddingWorker starting")
        while self._running:
            batch = self._collect_batch()
            if batch:
                self._process_batch(batch)
