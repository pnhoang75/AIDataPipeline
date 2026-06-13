import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

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
    def __init__(self, producer, topic: str, connector_id: str, tenant_id: str):
        self._producer = producer
        self._topic = topic
        self._connector_id = connector_id
        self._tenant_id = tenant_id

    def publish_datasource(self, entity_key: str, attributes: Dict[str, Any]) -> None:
        event = build_cloudevent(
            event_type=_EVENT_TYPE,
            source=f"connector/{self._connector_id}",
            subject=f"DataSource/{entity_key}",
            data={
                "entity_type": "DataSource",
                "entity_key": entity_key,
                "tenant_id": self._tenant_id,
                "attributes": attributes,
            },
        )
        self._safe_publish(event)

    def publish_rawdocument(
        self,
        entity_key: str,
        attributes: Dict[str, Any],
        datasource_entity_key: str,
    ) -> None:
        event = build_cloudevent(
            event_type=_EVENT_TYPE,
            source=f"connector/{self._connector_id}",
            subject=f"RawDocument/{entity_key}",
            data={
                "entity_type": "RawDocument",
                "entity_key": entity_key,
                "tenant_id": self._tenant_id,
                "attributes": attributes,
                "upstream": [
                    {
                        "entity_type": "DataSource",
                        "entity_key": datasource_entity_key,
                        "relationship": "discovered_in",
                    }
                ],
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
