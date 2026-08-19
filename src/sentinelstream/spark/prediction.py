"""Spark-distributed model scoring and Phase 4 decision integration."""

from __future__ import annotations

import json
import pickle  # nosec B403
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sentinelstream.features.registry import FEATURE_VERSION
from sentinelstream.models.baselines import DROP_COLUMNS, rule_scores
from sentinelstream.models.decision import (
    PredictionSignals,
    ThresholdPolicy,
    make_decision,
)
from sentinelstream.spark.state import FEATURE_COLUMNS

_SPARK_COLUMNS = {
    "event_json",
    "event_time",
    "event_timestamp",
    "kafka_timestamp",
    "kafka_partition",
    "kafka_offset",
    "kafka_key",
    "message_id",
    "schema_version",
    "produced_at",
    "correlation_id",
}


@dataclass(frozen=True, slots=True)
class ModelBundle:
    """Pickle-compatible serving bundle containing an estimator and feature order."""

    estimator: Any
    feature_columns: tuple[str, ...] = ()
    anomaly_estimator: Any | None = None
    anomaly_minimum: float | None = None
    anomaly_maximum: float | None = None


def load_model_bundle(path: Path) -> ModelBundle:
    """Load a model bundle created by a training or serving workflow."""

    if not path.is_file():
        raise FileNotFoundError(f"model bundle does not exist: {path}")
    with path.open("rb") as handle:
        loaded = pickle.load(handle)  # nosec B301
    if isinstance(loaded, ModelBundle):
        return loaded
    if isinstance(loaded, dict) and "estimator" in loaded:
        return ModelBundle(
            estimator=loaded["estimator"],
            feature_columns=tuple(loaded.get("feature_columns", ())),
            anomaly_estimator=loaded.get("anomaly_estimator"),
            anomaly_minimum=loaded.get("anomaly_minimum"),
            anomaly_maximum=loaded.get("anomaly_maximum"),
        )
    return ModelBundle(estimator=loaded)


def _encoded_features(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    drop_columns = DROP_COLUMNS | _SPARK_COLUMNS
    selected = frame[[column for column in frame.columns if column not in drop_columns]].copy()
    encoded = pd.get_dummies(selected, dtype=float).fillna(0.0)
    if columns:
        encoded = encoded.reindex(columns=list(columns), fill_value=0.0)
    return encoded


def _probabilities(estimator: Any, matrix: pd.DataFrame) -> np.ndarray:
    if hasattr(estimator, "predict_proba"):
        values = np.asarray(estimator.predict_proba(matrix)[:, 1], dtype=float)
    elif hasattr(estimator, "decision_function"):
        decisions = np.asarray(estimator.decision_function(matrix), dtype=float)
        values = 1.0 / (1.0 + np.exp(-decisions))
    else:
        values = np.asarray(estimator.predict(matrix), dtype=float)
    return np.clip(values, 0.0, 1.0)


def _anomaly_score(bundle: ModelBundle, matrix: pd.DataFrame) -> np.ndarray | None:
    if bundle.anomaly_estimator is None:
        return None
    raw = -np.asarray(bundle.anomaly_estimator.decision_function(matrix), dtype=float)
    low = bundle.anomaly_minimum if bundle.anomaly_minimum is not None else float(raw.min())
    high = bundle.anomaly_maximum if bundle.anomaly_maximum is not None else float(raw.max())
    if high == low:
        return np.full(len(raw), 0.5)
    return np.clip((raw - low) / (high - low), 0.0, 1.0)


@dataclass(slots=True)
class SparkPredictionEngine:
    """Score feature rows on Spark workers and apply the existing risk policy."""

    bundle: ModelBundle | None = None
    model_version: str = "unknown"
    calibration_version: str = "unknown"
    model_probability_weight: float = 0.70
    rule_probability_weight: float = 0.20
    anomaly_probability_weight: float = 0.10
    policy: ThresholdPolicy = ThresholdPolicy()

    def score(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Return prediction and decision fields for one pandas batch."""

        if frame.empty:
            return frame.assign()
        bundle = self.bundle
        if bundle is None:
            model_probability = None
            anomaly = None
        else:
            matrix = _encoded_features(frame, bundle.feature_columns)
            model_probability = _probabilities(bundle.estimator, matrix)
            anomaly = _anomaly_score(bundle, matrix)
        rules = rule_scores(frame)
        active_weights = [self.rule_probability_weight]
        if model_probability is not None:
            active_weights.insert(0, self.model_probability_weight)
        if anomaly is not None:
            active_weights.append(self.anomaly_probability_weight)
        normaliser = sum(active_weights)
        final = self.rule_probability_weight * rules
        if model_probability is not None:
            final = final + self.model_probability_weight * model_probability
        if anomaly is not None:
            final = final + self.anomaly_probability_weight * anomaly
        final = np.clip(final / normaliser, 0.0, 1.0)
        anomaly_values = anomaly if anomaly is not None else np.full(len(frame), np.nan)
        decisions = [
            make_decision(
                PredictionSignals(
                    calibrated_probability=float(score),
                    anomaly_score=None
                    if np.isnan(anomaly_values[index])
                    else float(anomaly_values[index]),
                    rule_score=float(rules[index]),
                    model_version=self.model_version,
                    calibration_version=self.calibration_version,
                ),
                self.policy,
            )
            for index, score in enumerate(final)
        ]
        output = frame.copy()
        output["prediction_message_id"] = [
            str(uuid5(NAMESPACE_URL, f"sentinelstream:prediction:{event_id}:{self.model_version}"))
            for event_id in output["event_id"]
        ]
        output["prediction_produced_at"] = datetime.now(UTC)
        output["model_probability"] = (
            model_probability.tolist() if model_probability is not None else [None] * len(output)
        )
        output["calibrated_probability"] = final
        output["anomaly_score"] = [
            None if np.isnan(value) else float(value) for value in anomaly_values
        ]
        output["rule_score"] = rules
        output["final_risk_score"] = final
        output["confidence"] = [decision.confidence for decision in decisions]
        output["action"] = [decision.action.value for decision in decisions]
        output["risk_tier"] = [decision.risk_tier.value for decision in decisions]
        output["abstained"] = [decision.abstained for decision in decisions]
        output["reason_codes_json"] = [json.dumps(decision.reason_codes) for decision in decisions]
        output["features_json"] = [
            json.dumps(
                {
                    name: float(row[name])
                    for name in FEATURE_COLUMNS
                    if name in row and pd.notna(row[name])
                },
                sort_keys=True,
            )
            for _, row in output.iterrows()
        ]
        output["model_version"] = self.model_version
        output["calibration_version"] = self.calibration_version
        output["feature_version"] = FEATURE_VERSION
        return output


def prediction_schema(input_schema: StructType) -> StructType:
    """Extend a feature schema with model and decision output columns."""

    fields = list(input_schema.fields)
    fields.extend(
        [
            StructField("prediction_message_id", StringType(), False),
            StructField("prediction_produced_at", TimestampType(), False),
            StructField("model_probability", DoubleType(), True),
            StructField("calibrated_probability", DoubleType(), False),
            StructField("anomaly_score", DoubleType(), True),
            StructField("rule_score", DoubleType(), False),
            StructField("final_risk_score", DoubleType(), False),
            StructField("confidence", DoubleType(), False),
            StructField("action", StringType(), False),
            StructField("risk_tier", StringType(), False),
            StructField("abstained", BooleanType(), False),
            StructField("reason_codes_json", StringType(), False),
            StructField("features_json", StringType(), False),
            StructField("model_version", StringType(), False),
            StructField("calibration_version", StringType(), False),
            StructField("feature_version", StringType(), False),
        ]
    )
    return StructType(fields)


def build_prediction_stream(
    features: DataFrame,
    engine: SparkPredictionEngine,
) -> DataFrame:
    """Apply the model and policy on Spark workers using pandas batches."""

    schema = prediction_schema(features.schema)

    def score_batches(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for batch in batches:
            yield engine.score(batch)

    return features.mapInPandas(score_batches, schema=schema)
