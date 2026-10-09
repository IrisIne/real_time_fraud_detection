from __future__ import annotations

import csv
from pathlib import Path

import numpy as np
import pytest

from fraud_detector.app.preprocessing import (
    CATEGORICAL_FEATURES,
    PreprocessingError,
    TransactionPreprocessor,
)


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def transaction() -> dict[str, str]:
    with (ROOT / "data" / "test_sample.csv").open(encoding="utf-8", newline="") as file:
        return next(csv.DictReader(file))


@pytest.fixture(scope="module")
def preprocessor() -> TransactionPreprocessor:
    return TransactionPreprocessor(ROOT / "fraud_detector" / "artifacts" / "preprocessing.json")


def test_transform_produces_finite_continuous_features(preprocessor, transaction):
    features = preprocessor.transform(transaction)
    assert len(features) == 1
    assert set(CATEGORICAL_FEATURES).issubset(features.columns)
    assert np.isfinite(features[["amount_log", "population_city_log", "distance_log"]].to_numpy()).all()


def test_unknown_categories_are_supported(preprocessor, transaction):
    changed = dict(transaction)
    changed["merch"] = "merchant-never-seen-in-training"
    features = preprocessor.transform(changed)
    assert features.loc[0, "merch_cat"] == "cat_NAN"


def test_missing_required_field_is_rejected(preprocessor, transaction):
    changed = dict(transaction)
    changed.pop("amount")
    with pytest.raises(PreprocessingError, match="missing required fields"):
        preprocessor.transform(changed)
