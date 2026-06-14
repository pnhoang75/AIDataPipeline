import logging
import time

import psycopg2
import redis as redis_lib
import structlog
from confluent_kafka import Producer
from prometheus_client import start_http_server

from config import config
from connector import NFSConnector
from logging_config import setup_logging, bind_request_context

setup_logging("connector-nfs")
logger = structlog.get_logger(__name__)


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
    bind_request_context(tenant_id=config.tenant_id)
    start_http_server(9090)
    logger.info(
        "Starting NFS connector",
        connector_id=config.connector_id,
        mount=config.nfs_mount_path,
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
            logger.info("Poll complete", events_published=count)
            backoff_multiplier = 1.0
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            logger.warning("DB connection lost, reconnecting", error=str(exc))
            try:
                db_conn.close()
            except Exception:
                pass
            try:
                db_conn = build_db_conn()
                connector._db = db_conn
                logger.info("DB reconnected")
            except Exception as reconn_exc:
                logger.error("DB reconnect failed", error=str(reconn_exc))
                backoff_multiplier = min(backoff_multiplier * 2, 10.0)
        except Exception as exc:
            logger.error("Poll failed", error=str(exc), exc_info=True)
            backoff_multiplier = min(backoff_multiplier * 2, 10.0)

        time.sleep(poll_interval * backoff_multiplier)


if __name__ == "__main__":
    main()
