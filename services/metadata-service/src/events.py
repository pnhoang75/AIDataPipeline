import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

_DQ_FAILED_TYPE = "data.quality.failed"


def _build_cloudevent(event_type: str, source: str, subject: str, data: Dict[str, Any]) -> Dict[str, Any]:
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


class DataQualityFailedPublisher:
    def __init__(self, producer, topic: str):
        self._producer = producer
        self._topic = topic

    def publish(
        self,
        entity_id: str,
        entity_type: str,
        entity_key: str,
        tenant_id: str,
        failed_checks: List[Dict[str, Any]],
    ) -> None:
        event = _build_cloudevent(
            event_type=_DQ_FAILED_TYPE,
            source="metadata-service",
            subject=f"{entity_type}/{entity_key}",
            data={
                "entity_id": entity_id,
                "entity_type": entity_type,
                "entity_key": entity_key,
                "tenant_id": tenant_id,
                "failed_checks": failed_checks,
            },
        )
        try:
            self._producer.produce(
                self._topic,
                key=entity_key.encode(),
                value=json.dumps(event).encode(),
            )
            self._producer.flush(timeout=5.0)
        except Exception as exc:
            logger.warning("DataQualityFailed publish failed: %s", exc)
