"""Build compact inference-only preprocessing statistics from train.csv.

This utility is used during project preparation. The resulting JSON is committed
with the model; the runtime service does not read train.csv or train a model.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import zipfile

import numpy as np
import pandas as pd


CATEGORICAL_COLUMNS = ["gender", "merch", "cat_id", "one_city", "us_state", "jobs"]
TIME_COLUMNS = ["hour", "year", "month", "day_of_month", "day_of_week"]
EARTH_RADIUS_KM = 6371.009


def read_train(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            candidates = [name for name in archive.namelist() if Path(name).name == "train.csv"]
            if len(candidates) != 1:
                raise ValueError("ZIP archive must contain exactly one train.csv")
            with archive.open(candidates[0]) as source:
                return pd.read_csv(source)
    return pd.read_csv(path)


def key(value) -> str:
    return "__NA__" if pd.isna(value) else str(value)


def add_time_features(frame: pd.DataFrame) -> None:
    timestamps = pd.to_datetime(frame["transaction_time"], errors="raise")
    frame["hour"] = timestamps.dt.hour
    frame["year"] = timestamps.dt.year
    frame["month"] = timestamps.dt.month
    frame["day_of_month"] = timestamps.dt.day
    frame["day_of_week"] = timestamps.dt.dayofweek


def add_distance(frame: pd.DataFrame) -> None:
    lat1 = np.radians(pd.to_numeric(frame["lat"], errors="coerce"))
    lon1 = np.radians(pd.to_numeric(frame["lon"], errors="coerce"))
    lat2 = np.radians(pd.to_numeric(frame["merchant_lat"], errors="coerce"))
    lon2 = np.radians(pd.to_numeric(frame["merchant_lon"], errors="coerce"))
    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1
    haversine = (
        np.sin(delta_lat / 2.0) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin(delta_lon / 2.0) ** 2
    )
    frame["distance"] = 2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(haversine))


def build(train: pd.DataFrame) -> dict:
    required = {
        "transaction_time",
        "target",
        "amount",
        "population_city",
        "lat",
        "lon",
        "merchant_lat",
        "merchant_lon",
        *CATEGORICAL_COLUMNS,
    }
    missing = sorted(required.difference(train.columns))
    if missing:
        raise ValueError(f"train.csv is missing columns: {missing}")

    category_mappings: dict[str, dict[str, str]] = {}
    for column in CATEGORICAL_COLUMNS:
        ranked = (
            train.groupby(column, dropna=False)[["target"]]
            .count()
            .sort_values("target", ascending=False)
            .reset_index()
            .rename(columns={"target": "count"})
            .reset_index()
        )
        mapping: dict[str, str] = {}
        for _, row in ranked.iterrows():
            source_key = key(row[column])
            rank = int(row["index"])
            mapping[source_key] = "cat_NAN" if source_key == "__NA__" else (
                f"cat_{rank}" if rank < 50 else "cat_50+"
            )
        category_mappings[column] = mapping
        train[f"{column}_cat"] = train[column].map(lambda value: mapping[key(value)])

    add_time_features(train)
    add_distance(train)

    encoded_columns = [f"{column}_cat" for column in CATEGORICAL_COLUMNS] + TIME_COLUMNS
    mean_encodings: dict[str, dict[str, float]] = {}
    for column in encoded_columns:
        means = train.groupby(column, dropna=False)["target"].mean()
        mean_encodings[column] = {key(index): float(value) for index, value in means.items()}

    continuous_means = {
        column: float(pd.to_numeric(train[column], errors="coerce").mean())
        for column in ["amount", "population_city", "distance"]
    }

    return {
        "version": 1,
        "description": "Inference preprocessing compatible with the seminar CatBoost model",
        "category_mappings": category_mappings,
        "mean_encodings": mean_encodings,
        "continuous_means": continuous_means,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path, help="Path to train.csv or competition ZIP")
    parser.add_argument("output", type=Path, help="Output preprocessing.json")
    args = parser.parse_args()

    print(f"Reading {args.input} ...", flush=True)
    train = read_train(args.input)
    print(f"Loaded {len(train):,} rows", flush=True)
    artifacts = build(train)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as file:
        json.dump(artifacts, file, ensure_ascii=False, separators=(",", ":"))
    print(f"Wrote {args.output} ({args.output.stat().st_size:,} bytes)", flush=True)


if __name__ == "__main__":
    main()
