import json
import logging
import os

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka.infrastructure.svc:9092")


async def publish_event(topic: str, payload: dict) -> None:
    """Publish a JSON event to a Kafka topic."""
    try:
        from aiokafka import AIOKafkaProducer

        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        await producer.start()
        try:
            await producer.send_and_wait(topic, json.dumps(payload).encode())
        finally:
            await producer.stop()
    except ImportError:
        logger.warning("aiokafka not available; skipping publish to %s: %s", topic, payload)
