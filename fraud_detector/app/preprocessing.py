from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


INPUT_COLUMNS = [
    "transaction_time",
    "merch",
    "cat_id",
    "amount",
    "name_1",
    "name_2",
    "gender",
    "street",
    "one_city",
    "us_state",
    "post_code",
    "lat",
    "lon",
    "population_city",
    "jobs",
    "merchant_lat",
    "merchant_lon",
]
CATEGORICAL_SOURCE_COLUMNS = ["gender", "merch", "cat_id", "one_city", "us_state", "jobs"]
TIME_COLUMNS = ["hour", "year", "month", "day_of_month", "day_of_week"]
CATEGORICAL_FEATURES = [f"{column}_cat" for column in CATEGORICAL_SOURCE_COLUMNS] + TIME_COLUMNS
CONTINUOUS_COLUMNS = ["amount", "population_city", "distance"]
EARTH_RADIUS_KM = 6371.009


class PreprocessingError(ValueError):
    """Raised when a transaction cannot be converted to model features."""

    pass


class TransactionPreprocessor:
    def __init__(self, artifacts_path: str | Path):
        with Path(artifacts_path).open("r", encoding="utf-8") as file:
            self.artifacts: dict[str, Any] = json.load(file)

    def transform(self, transaction: dict[str, Any]) -> pd.DataFrame:
        missing = [column for column in INPUT_COLUMNS if column not in transaction]
        if missing:
            raise PreprocessingError(f"missing required fields: {', '.join(missing)}")

        timestamp = pd.to_datetime(transaction["transaction_time"], errors="coerce")
        if pd.isna(timestamp):
            raise PreprocessingError("transaction_time has an invalid value")

        features: dict[str, Any] = {}
        for column in CATEGORICAL_SOURCE_COLUMNS:
            value = transaction.get(column)
            mapping = self.artifacts["category_mappings"][column]
            key = "__NA__" if self._is_missing(value) else str(value)
            features[f"{column}_cat"] = mapping.get(key, "cat_NAN")

        time_values = {
            "hour": int(timestamp.hour),
            "year": int(timestamp.year),
            "month": int(timestamp.month),
            "day_of_month": int(timestamp.day),
            "day_of_week": int(timestamp.dayofweek),
        }
        features.update(time_values)

        for column in CATEGORICAL_FEATURES:
            encoded_value = features[column]
            mean_mapping = self.artifacts["mean_encodings"][column]
            features[f"{column}_mean_enc"] = mean_mapping.get(str(encoded_value), np.nan)

        amount = self._as_float(transaction.get("amount"), "amount")
        population = self._as_float(transaction.get("population_city"), "population_city")
        distance = self._distance(transaction)
        continuous_values = {
            "amount": amount,
            "population_city": population,
            "distance": distance,
        }
        for column, value in continuous_values.items():
            if np.isnan(value):
                value = float(self.artifacts["continuous_means"][column])
            if value < 0:
                raise PreprocessingError(f"{column} must be non-negative")
            features[f"{column}_log"] = float(np.log1p(value))

        frame = pd.DataFrame([features])
        for column in CATEGORICAL_FEATURES:
            frame[column] = frame[column].astype(str)
        return frame

    @staticmethod
    def _is_missing(value: Any) -> bool:
        if value is None:
            return True
        try:
            return bool(pd.isna(value))
        except (TypeError, ValueError):
            return False

    @classmethod
    def _as_float(cls, value: Any, field: str) -> float:
        if cls._is_missing(value):
            return float("nan")
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise PreprocessingError(f"{field} must be numeric") from exc

    @classmethod
    def _distance(cls, transaction: dict[str, Any]) -> float:
        lat = cls._as_float(transaction.get("lat"), "lat")
        lon = cls._as_float(transaction.get("lon"), "lon")
        merchant_lat = cls._as_float(transaction.get("merchant_lat"), "merchant_lat")
        merchant_lon = cls._as_float(transaction.get("merchant_lon"), "merchant_lon")
        values = np.array([lat, lon, merchant_lat, merchant_lon], dtype=float)
        if np.isnan(values).any():
            return float("nan")
        lat1, lon1, lat2, lon2 = np.radians(values)
        delta_lat = lat2 - lat1
        delta_lon = lon2 - lon1
        haversine = (
            np.sin(delta_lat / 2.0) ** 2
            + np.cos(lat1) * np.cos(lat2) * np.sin(delta_lon / 2.0) ** 2
        )
        return float(2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(haversine)))
