import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_EVENT_TYPE = "metadata.entity.created"


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
    def __init__(self, producer, topic: str, model_name: str = "BAAI/bge-small-en-v1.5"):
        self._producer = producer
        self._topic = topic
        self._model_name = model_name

    def publish_rag_query(
        self,
        query_id: str,
        tenant_id: str,
        query_text: str,
        top_k: int,
        source_filter: Optional[str],
        collection: str,
        latency_ms: float,
        cached: bool,
        retrieved_chunks: List[Dict[str, Any]],
    ) -> None:
        query_text_hash = "sha256:" + hashlib.sha256(query_text.encode()).hexdigest()
        event = build_cloudevent(
            event_type=_EVENT_TYPE,
            source="rag-api",
            subject=f"RAGQuery/{query_id}",
            data={
                "entity_type": "RAGQuery",
                "entity_key": query_id,
                "tenant_id": tenant_id,
                "attributes": {
                    "query_text_hash": query_text_hash,
                    "top_k": top_k,
                    "source_filter": source_filter,
                    "collection": collection,
                    "latency_ms": round(latency_ms, 3),
                    "cached": cached,
                    "model_used": self._model_name,
                },
                "retrieved_chunks": retrieved_chunks,
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
