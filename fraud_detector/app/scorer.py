from __future__ import annotations

from catboost import CatBoostClassifier
import pandas as pd


class FraudScorer:
    def __init__(self, model_path: str, threshold: float):
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("fraud threshold must be between 0 and 1")
        self.threshold = threshold
        self.model = CatBoostClassifier()
        self.model.load_model(model_path)

    def predict(self, features: pd.DataFrame) -> tuple[float, int]:
        expected = list(self.model.feature_names_)
        missing = [column for column in expected if column not in features.columns]
        if missing:
            raise ValueError(f"preprocessing did not produce model features: {missing}")
        ordered = features[expected]
        score = float(self.model.predict_proba(ordered)[0, 1])
        return score, int(score > self.threshold)
