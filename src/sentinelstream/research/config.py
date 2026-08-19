"""Validated configuration for research runs."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from sentinelstream.research.ablation import DEFAULT_ABLATION_COMBINATIONS


class ResearchConfig(BaseModel):
    """All inputs that affect a research result or its reproducibility metadata."""

    model_config = ConfigDict(extra="forbid")

    experiment_id: str = Field(min_length=1, max_length=120)
    research_question: str = Field(
        default="How do model and rule sources affect fraud detection utility?",
        min_length=1,
    )
    hypothesis: str = Field(
        default=(
            "Combining independent signals can improve operational utility, subject to calibration."
        ),
        min_length=1,
    )
    dataset_version: str = Field(default="unversioned", min_length=1)
    source: str = Field(default="unknown", min_length=1)
    feature_set_version: str = Field(default="phase3-v1", min_length=1)
    output_dir: Path = Path("reports/research")
    random_seed: int = Field(default=42, ge=0)
    train_fraction: float = Field(default=0.70, gt=0.0, lt=1.0)
    validation_fraction: float = Field(default=0.15, gt=0.0, lt=1.0)
    thresholds: tuple[float, ...] = (0.20, 0.35, 0.50, 0.65, 0.80)
    model_names: tuple[str, ...] = ("logistic_regression", "rules", "isolation_forest")
    ablation_combinations: tuple[str, ...] = DEFAULT_ABLATION_COMBINATIONS
    bootstrap_resamples: int = Field(default=200, ge=0, le=10_000)
    confidence_level: float = Field(default=0.95, gt=0.0, lt=1.0)
    minimum_segment_size: int = Field(default=30, ge=1)
    mlflow_tracking_uri: str | None = None
    mlflow_experiment_name: str = "sentinelstream-research"
    log_mlflow: bool = False

    @field_validator("experiment_id", "dataset_version", "feature_set_version", "source")
    @classmethod
    def strip_required_strings(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("value must not be blank")
        return value

    @field_validator("thresholds")
    @classmethod
    def validate_thresholds(cls, values: tuple[float, ...]) -> tuple[float, ...]:
        if not values:
            raise ValueError("at least one threshold is required")
        if any(not 0.0 <= value <= 1.0 for value in values):
            raise ValueError("thresholds must be between 0 and 1")
        return tuple(sorted(set(values)))

    @field_validator("model_names")
    @classmethod
    def validate_models(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if not values or any(not value.strip() for value in values):
            raise ValueError("at least one non-empty model name is required")
        return tuple(dict.fromkeys(value.strip() for value in values))

    @field_validator("ablation_combinations")
    @classmethod
    def validate_ablation_combinations(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value.strip() for value in values):
            raise ValueError("ablation combination names must not be blank")
        return tuple(dict.fromkeys(value.strip() for value in values))

    @model_validator(mode="after")
    def validate_split(self) -> ResearchConfig:
        if self.train_fraction + self.validation_fraction >= 1.0:
            raise ValueError("train and validation fractions must leave test data")
        if self.log_mlflow and not self.mlflow_tracking_uri:
            raise ValueError("mlflow_tracking_uri is required when log_mlflow is enabled")
        return self
