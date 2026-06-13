import os


class Config:
    database_url: str = os.getenv("DATABASE_URL", "")
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "")
    metadata_events_topic: str = os.getenv("METADATA_EVENTS_TOPIC", "metadata-events")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "metadata-service")
    data_quality_failed_topic: str = os.getenv("DATA_QUALITY_FAILED_TOPIC", "data-quality-failed")
    poll_timeout_seconds: float = float(os.getenv("POLL_TIMEOUT_SECONDS", "1.0"))


config = Config()
