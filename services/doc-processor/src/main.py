import structlog
from confluent_kafka import Consumer, Producer
from minio import Minio
from prometheus_client import start_http_server

from logging_config import setup_logging
from config import config
from fetcher import fetch_content_with_retry
from processor import DocumentProcessor

setup_logging("doc-processor")
logger = structlog.get_logger(__name__)


def main() -> None:
    start_http_server(9090)
    minio_client = Minio(
        config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        secure=config.minio_secure,
    )

    def _fetch(content_ref: str) -> bytes:
        return fetch_content_with_retry(content_ref, s3_client=minio_client)

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
        content_fetcher=_fetch,
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
