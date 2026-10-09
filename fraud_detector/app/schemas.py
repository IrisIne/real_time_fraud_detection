from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID


class MessageValidationError(ValueError):
    """Raised when a Kafka message does not follow the public contract."""

    pass


@dataclass(frozen=True)
class TransactionMessage:
    transaction_id: str
    data: dict[str, Any]

    @classmethod
    def from_dict(cls, payload: Any) -> "TransactionMessage":
        if not isinstance(payload, dict):
            raise MessageValidationError("message must be a JSON object")
        transaction_id = payload.get("transaction_id")
        data = payload.get("data")
        try:
            UUID(str(transaction_id))
        except (TypeError, ValueError) as exc:
            raise MessageValidationError("transaction_id must be a valid UUID") from exc
        if not isinstance(data, dict):
            raise MessageValidationError("data must be a JSON object")
        return cls(transaction_id=str(transaction_id), data=data)


def score_message(transaction_id: str, score: float, fraud_flag: int) -> dict[str, Any]:
    return {
        "transaction_id": transaction_id,
        "score": float(score),
        "fraud_flag": int(fraud_flag),
    }
