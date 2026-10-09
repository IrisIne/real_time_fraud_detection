from uuid import uuid4

import pytest

from fraud_detector.app.schemas import MessageValidationError, TransactionMessage, score_message


def test_transaction_message_contract():
    transaction_id = str(uuid4())
    message = TransactionMessage.from_dict({"transaction_id": transaction_id, "data": {"amount": 1}})
    assert message.transaction_id == transaction_id


def test_invalid_uuid_is_rejected():
    with pytest.raises(MessageValidationError):
        TransactionMessage.from_dict({"transaction_id": "not-a-uuid", "data": {}})


def test_score_message_has_exact_public_fields():
    payload = score_message(str(uuid4()), 0.25, 0)
    assert set(payload) == {"transaction_id", "score", "fraud_flag"}
