import json
import logging
import math
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

_EVENT_TYPE = "metadata.entity.created"
_NORM_THRESHOLD = 0.5


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


def _l2_norm(vector: List[float]) -> float:
    return math.sqrt(sum(x * x for x in vector))


class MetadataEventPublisher:
    def __init__(
        self,
        producer,
        topic: str,
        model_name: str,
        embedding_dim: int,
        backend: str,
    ):
        self._producer = producer
        self._topic = topic
        self._model_name = model_name
        self._embedding_dim = embedding_dim
        self._backend = backend

    def publish_embedding(
        self,
        chunk_id: str,
        tenant_id: str,
        embedding: List[float],
        collection_name: str,
    ) -> None:
        norm = round(_l2_norm(embedding), 6)
        entity_key = f"{chunk_id}:{self._model_name}"
        quality_checks = [
            {
                "check_name": "embedding_norm",
                "status": "passed" if norm >= _NORM_THRESHOLD else "failed",
                "value": norm,
                "threshold": _NORM_THRESHOLD,
            }
        ]
        event = build_cloudevent(
            event_type=_EVENT_TYPE,
            source="embedding-worker",
            subject=f"Embedding/{entity_key}",
            data={
                "entity_type": "Embedding",
                "entity_key": entity_key,
                "tenant_id": tenant_id,
                "attributes": {
                    "chunk_id": chunk_id,
                    "model_name": self._model_name,
                    "dimension": self._embedding_dim,
                    "backend": self._backend,
                    "collection_name": collection_name,
                    "embedding_norm": norm,
                },
                "upstream": [
                    {
                        "entity_type": "DocumentChunk",
                        "entity_key": chunk_id,
                        "relationship": "embedded_by",
                    },
                    {
                        "entity_type": "VectorCollection",
                        "entity_key": collection_name,
                        "relationship": "stored_in",
                    },
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
