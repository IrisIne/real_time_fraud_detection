from __future__ import annotations

import json
import logging
import signal

from confluent_kafka import KafkaError

from .config import Settings
from .kafka_client import build_consumer, build_producer, publish_json
from .preprocessing import PreprocessingError, TransactionPreprocessor
from .schemas import MessageValidationError, TransactionMessage, score_message
from .scorer import FraudScorer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("fraud_detector")


def run() -> None:
    settings = Settings()
    preprocessor = TransactionPreprocessor(settings.artifacts_path)
    scorer = FraudScorer(settings.model_path, settings.fraud_threshold)
    consumer = build_consumer(
        settings.kafka_bootstrap_servers,
        settings.consumer_group,
        settings.transactions_topic,
    )
    producer = build_producer(settings.kafka_bootstrap_servers)
    should_run = True

    def stop(_signum, _frame) -> None:
        nonlocal should_run
        should_run = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    LOGGER.info("Fraud detector started; topic=%s", settings.transactions_topic)

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
                transaction = TransactionMessage.from_dict(payload)
                features = preprocessor.transform(transaction.data)
                score, fraud_flag = scorer.predict(features)
                result = score_message(transaction.transaction_id, score, fraud_flag)
                publish_json(
                    producer,
                    settings.scores_topic,
                    transaction.transaction_id,
                    result,
                )
                consumer.commit(message=message, asynchronous=False)
                LOGGER.info(
                    "Scored transaction_id=%s score=%.6f fraud_flag=%d",
                    transaction.transaction_id,
                    score,
                    fraud_flag,
                )
            except (json.JSONDecodeError, UnicodeDecodeError, MessageValidationError, PreprocessingError) as exc:
                LOGGER.warning("Skipping invalid input message: %s", exc)
                consumer.commit(message=message, asynchronous=False)
            except Exception:
                LOGGER.exception("Failed to process message; offset will not be committed")
    finally:
        producer.flush(5.0)
        consumer.close()
        LOGGER.info("Fraud detector stopped")


if __name__ == "__main__":
    run()
