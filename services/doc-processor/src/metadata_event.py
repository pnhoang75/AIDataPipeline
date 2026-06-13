import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger(__name__)

_EVENT_TYPE = "metadata.entity.created"
_MIN_TOKEN_COUNT = 50


def build_cloudevent(
    event_type: str,
    source: str,
    subject: str,
    data: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "subject": subject,
        "id": str(uuid.uuid4()),
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
        "data": data,
    }


class MetadataEventPublisher:
    def __init__(self, producer, topic: str):
        self._producer = producer
        self._topic = topic

    def publish_document_chunk(
        self,
        chunk_id: str,
        doc_id: str,
        chunk_index: int,
        total_chunks: int,
        token_count: int,
        text_preview: str,
        tenant_id: str,
    ) -> None:
        quality_checks = [
            {
                "check_name": "not_empty",
                "status": "passed" if token_count > 0 else "failed",
            },
            {
                "check_name": "min_token_count",
                "status": "passed" if token_count >= _MIN_TOKEN_COUNT else "failed",
                "value": token_count,
                "threshold": _MIN_TOKEN_COUNT,
            },
        ]
        event = build_cloudevent(
            event_type=_EVENT_TYPE,
            source="doc-processor",
            subject=f"DocumentChunk/{chunk_id}",
            data={
                "entity_type": "DocumentChunk",
                "entity_key": chunk_id,
                "tenant_id": tenant_id,
                "attributes": {
                    "doc_id": doc_id,
                    "chunk_index": chunk_index,
                    "total_chunks": total_chunks,
                    "token_count": token_count,
                    "text_preview": text_preview,
                },
                "upstream": [
                    {
                        "entity_type": "RawDocument",
                        "entity_key": doc_id,
                        "relationship": "chunked_into",
                    }
                ],
                "quality_checks": quality_checks,
            },
        )
        self._safe_publish(event)

    def _safe_publish(self, event: Dict[str, Any]) -> None:
        try:
            self._producer.produce(
                self._topic,
                key=event["id"].encode(),
                value=json.dumps(event).encode(),
            )
            self._producer.flush(timeout=5.0)
        except Exception as exc:
            logger.warning("metadata event publish failed: %s", exc)
