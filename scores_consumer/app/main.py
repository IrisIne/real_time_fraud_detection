from __future__ import annotations

import json
import logging
import signal
import time
from typing import Any
from uuid import UUID

from confluent_kafka import Consumer, KafkaError
import psycopg

from .config import Settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("scores_consumer")

UPSERT_SQL = """
INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
VALUES (%s, %s, %s)
ON CONFLICT (transaction_id) DO UPDATE
SET score = EXCLUDED.score,
    fraud_flag = EXCLUDED.fraud_flag,
    created_at = NOW()
"""


class InvalidScoreMessage(ValueError):
    """Raised when a score message does not follow the public contract."""

    pass


def validate_score(payload: Any) -> tuple[str, float, int]:
    if not isinstance(payload, dict):
        raise InvalidScoreMessage("message must be a JSON object")
    try:
        transaction_id = str(UUID(str(payload["transaction_id"])))
        score = float(payload["score"])
        fraud_flag = int(payload["fraud_flag"])
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidScoreMessage("invalid transaction_id, score or fraud_flag") from exc
    if not 0.0 <= score <= 1.0:
        raise InvalidScoreMessage("score must be between 0 and 1")
    if fraud_flag not in (0, 1):
        raise InvalidScoreMessage("fraud_flag must be 0 or 1")
    return transaction_id, score, fraud_flag


def connect_with_retry(dsn: str, attempts: int = 30) -> psycopg.Connection:
    for attempt in range(1, attempts + 1):
        try:
            connection = psycopg.connect(dsn)
            LOGGER.info("Connected to PostgreSQL")
            return connection
        except psycopg.OperationalError:
            if attempt == attempts:
                raise
            LOGGER.warning("PostgreSQL is unavailable; retry %d/%d", attempt, attempts)
            time.sleep(min(attempt, 5))
    raise RuntimeError("unreachable")


def run() -> None:
    settings = Settings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
        }
    )
    consumer.subscribe([settings.scores_topic])
    connection = connect_with_retry(settings.postgres_dsn)
    should_run = True

    def stop(_signum, _frame) -> None:
        nonlocal should_run
        should_run = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    LOGGER.info("Scores consumer started; topic=%s", settings.scores_topic)

    try:
        while should_run:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    LOGGER.error("Kafka consumer error: %s", message.error())
                continue

            try:
                payload = json.loads(message.value().decode("utf-8"))
                transaction_id, score, fraud_flag = validate_score(payload)
            except (json.JSONDecodeError, UnicodeDecodeError, InvalidScoreMessage) as exc:
                LOGGER.warning("Skipping invalid score message: %s", exc)
                consumer.commit(message=message, asynchronous=False)
                continue

            try:
                with connection.cursor() as cursor:
                    cursor.execute(UPSERT_SQL, (transaction_id, score, fraud_flag))
                connection.commit()
                consumer.commit(message=message, asynchronous=False)
                LOGGER.info("Stored transaction_id=%s", transaction_id)
            except psycopg.Error:
                LOGGER.exception("Database write failed; offset will not be committed")
                try:
                    connection.rollback()
                except psycopg.Error:
                    connection.close()
                    connection = connect_with_retry(settings.postgres_dsn)
    finally:
        consumer.close()
        connection.close()
        LOGGER.info("Scores consumer stopped")


if __name__ == "__main__":
    run()
