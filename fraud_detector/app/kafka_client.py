from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer


def build_consumer(bootstrap_servers: str, group_id: str, topic: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
        }
    )
    consumer.subscribe([topic])
    return consumer


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def publish_json(producer: Producer, topic: str, key: str, payload: dict[str, Any]) -> None:
    delivery_errors: list[Exception] = []

    def on_delivery(error, _message) -> None:
        if error is not None:
            delivery_errors.append(KafkaException(error))

    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload, allow_nan=False).encode("utf-8"),
        on_delivery=on_delivery,
    )
    remaining = producer.flush(10.0)
    if remaining:
        raise TimeoutError(f"{remaining} Kafka message(s) were not delivered")
    if delivery_errors:
        raise delivery_errors[0]
