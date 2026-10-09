from __future__ import annotations

import csv
from pathlib import Path

import pytest

pytest.importorskip("catboost")

from fraud_detector.app.preprocessing import TransactionPreprocessor
from fraud_detector.app.scorer import FraudScorer


ROOT = Path(__file__).resolve().parents[1]


def test_real_model_scores_one_transaction():
    with (ROOT / "data" / "test_sample.csv").open(encoding="utf-8", newline="") as file:
        transaction = next(csv.DictReader(file))
    preprocessor = TransactionPreprocessor(ROOT / "fraud_detector" / "artifacts" / "preprocessing.json")
    scorer = FraudScorer(str(ROOT / "fraud_detector" / "artifacts" / "model.cbm"), 0.98)
    score, flag = scorer.predict(preprocessor.transform(transaction))
    assert 0.0 <= score <= 1.0
    assert flag in (0, 1)
