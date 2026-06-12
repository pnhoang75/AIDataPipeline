import json
import logging
import time
import uuid
from typing import Optional

logger = logging.getLogger(__name__)


class UsagePublisher:
    """Publish CloudEvents to the usage-events Kafka topic for OpenMeter."""

    def __init__(self, producer, topic: str = "usage-events"):
        self._producer = producer
        self._topic = topic

    def publish_rag_query(
        self,
        tenant_id: str,
        duration_ms: float,
        result_count: int,
        cached: bool,
    ) -> None:
        event = {
            "specversion": "1.0",
            "type": "pipeline.rag.query",
            "source": "rag-api",
            "id": str(uuid.uuid4()),
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "datacontenttype": "application/json",
            "subject": tenant_id,
            "data": {
                "tenant_id": tenant_id,
                "duration_ms": round(duration_ms, 3),
                "result_count": result_count,
                "cached": cached,
            },
        }
        try:
            self._producer.produce(
                topic=self._topic,
                key=tenant_id.encode(),
                value=json.dumps(event).encode(),
                headers={"content-type": "application/cloudevents+json"},
            )
        except Exception as exc:
            logger.warning("Failed to publish usage event: %s", exc)


def make_usage_publisher(cfg) -> Optional[UsagePublisher]:
    """Return a UsagePublisher if Kafka is configured; None otherwise."""
    if not cfg.kafka_bootstrap:
        return None
    try:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": cfg.kafka_bootstrap})
        return UsagePublisher(producer, cfg.kafka_usage_topic)
    except Exception as exc:
        logger.warning("Could not create usage publisher: %s", exc)
        return None
