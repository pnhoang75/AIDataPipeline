import logging
import threading

from fastapi import FastAPI

from config import config

logger = logging.getLogger(__name__)

app = FastAPI(title="Metadata Service", version="1.0.0")
_consumer_thread: threading.Thread = None


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.on_event("startup")
async def startup_event():
    if not config.kafka_bootstrap or not config.database_url:
        logger.info("KAFKA_BOOTSTRAP or DATABASE_URL not set; skipping consumer startup")
        return
    try:
        import psycopg2
        from confluent_kafka import Consumer, Producer

        db_conn = psycopg2.connect(config.database_url)

        producer = Producer({"bootstrap.servers": config.kafka_bootstrap})
        kafka_consumer = Consumer(
            {
                "bootstrap.servers": config.kafka_bootstrap,
                "group.id": config.kafka_consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        from consumer import MetadataConsumer

        svc = MetadataConsumer(db_conn=db_conn, producer=producer, cfg=config)

        global _consumer_thread
        _consumer_thread = threading.Thread(
            target=svc.run,
            args=(kafka_consumer,),
            daemon=True,
            name="metadata-consumer",
        )
        _consumer_thread.start()
        logger.info("metadata consumer thread started")
    except Exception as exc:
        logger.error("failed to start consumer: %s", exc)
