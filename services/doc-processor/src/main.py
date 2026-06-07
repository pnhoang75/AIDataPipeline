import logging

from confluent_kafka import Consumer, Producer

from config import config
from fetcher import fetch_content_with_retry
from processor import DocumentProcessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap,
            "group.id": config.kafka_consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": config.kafka_max_poll_interval_ms,
            "session.timeout.ms": config.kafka_session_timeout_ms,
        }
    )
    producer = Producer({"bootstrap.servers": config.kafka_bootstrap})
    dlq_producer = Producer({"bootstrap.servers": config.kafka_bootstrap})

    processor = DocumentProcessor(
        consumer=consumer,
        producer=producer,
        dlq_producer=dlq_producer,
        content_fetcher=fetch_content_with_retry,
        cfg=config,
    )

    logger.info("Starting Document Processor")
    try:
        processor.run()
    except KeyboardInterrupt:
        processor.stop()
        logger.info("Stopped")


if __name__ == "__main__":
    main()
