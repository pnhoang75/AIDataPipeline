import logging
import time

import psycopg2
import redis as redis_lib
from confluent_kafka import Producer

from config import config
from connector import NFSConnector

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)


def build_kafka_producer() -> Producer:
    return Producer({"bootstrap.servers": config.kafka_bootstrap})


def build_redis_client():
    return redis_lib.from_url(
        config.redis_url,
        socket_connect_timeout=config.redis_connect_timeout_ms / 1000,
        socket_timeout=config.redis_op_timeout_ms / 1000,
    )


def build_db_conn():
    return psycopg2.connect(config.postgres_dsn)


def main() -> None:
    logger.info(
        "Starting NFS connector (connector_id=%s, mount=%s)",
        config.connector_id,
        config.nfs_mount_path,
    )

    kafka_producer = build_kafka_producer()
    redis_client = build_redis_client()
    db_conn = build_db_conn()

    connector = NFSConnector(
        config.nfs_mount_path, kafka_producer, redis_client, db_conn
    )

    poll_interval = config.poll_interval_seconds
    backoff_multiplier = 1.0

    while True:
        try:
            count = 0
            for event in connector.poll():
                count += 1
            logger.info("Poll complete: %d events published", count)
            backoff_multiplier = 1.0
        except Exception as exc:
            logger.error("Poll failed: %s", exc, exc_info=True)
            backoff_multiplier = min(backoff_multiplier * 2, 10.0)

        time.sleep(poll_interval * backoff_multiplier)


if __name__ == "__main__":
    main()
