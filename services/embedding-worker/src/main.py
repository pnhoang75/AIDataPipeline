import psycopg2
import structlog
from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server

from logging_config import setup_logging
from backends import LocalCPUBackend, OpenAIBackend
from config import config
from milvus_writer import MilvusWriter
from worker import EmbeddingWorker

setup_logging("embedding-worker")
logger = structlog.get_logger(__name__)


def main() -> None:
    start_http_server(9090)
    logger.info("Starting Embedding Worker (backend=%s)", config.embedding_backend)

    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap,
            "group.id": config.kafka_consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([config.kafka_input_topic])

    producer = Producer({"bootstrap.servers": config.kafka_bootstrap})
    dlq_producer = Producer({"bootstrap.servers": config.kafka_bootstrap})

    if config.embedding_backend == "openai":
        backend = OpenAIBackend(
            model=config.openai_embedding_model,
            dim=config.openai_embedding_dim,
        )
    else:
        backend = LocalCPUBackend(model_name=config.embedding_model)

    milvus_writer = MilvusWriter(
        host=config.milvus_host,
        port=config.milvus_port,
        collection=config.milvus_collection,
        dim=backend.dim,
    )
    milvus_writer.connect()

    db_conn = psycopg2.connect(config.postgres_dsn)

    worker = EmbeddingWorker(
        consumer=consumer,
        backend=backend,
        milvus_writer=milvus_writer,
        producer=producer,
        dlq_producer=dlq_producer,
        db_conn=db_conn,
        cfg=config,
    )

    try:
        worker.run()
    except KeyboardInterrupt:
        worker.stop()
        logger.info("Stopped")
    finally:
        consumer.close()
        milvus_writer.close()
        db_conn.close()


if __name__ == "__main__":
    main()
