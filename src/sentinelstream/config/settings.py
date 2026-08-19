"""Typed application configuration with YAML and environment support."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    """Supported runtime environments."""

    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"


class SimulationSettings(BaseModel):
    """Controls for reproducible synthetic event generation."""

    model_config = ConfigDict(extra="forbid")

    random_seed: int = Field(default=42, ge=0)
    customer_count: int = Field(default=250, ge=1)
    merchant_count: int = Field(default=60, ge=1)
    fraud_ratio: float = Field(default=0.05, ge=0.0, le=1.0)
    min_amount: float = Field(default=0.50, gt=0.0)
    max_amount: float = Field(default=25_000.0, gt=0.0)
    start_time: datetime = Field(default_factory=lambda: datetime(2025, 1, 1, tzinfo=UTC))
    duration_days: int = Field(default=30, ge=1)
    duplicate_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    late_event_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    max_delay_seconds: int = Field(default=7_200, ge=0)
    malformed_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    out_of_order_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    burst_probability: float = Field(default=0.05, ge=0.0, le=1.0)
    drift_enabled: bool = False
    drift_after_fraction: float = Field(default=0.70, gt=0.0, lt=1.0)
    replay: bool = False

    @model_validator(mode="after")
    def validate_amount_range(self) -> SimulationSettings:
        if self.max_amount < self.min_amount:
            raise ValueError("max_amount must be greater than or equal to min_amount")
        if self.start_time.tzinfo is None or self.start_time.utcoffset() is None:
            raise ValueError("start_time must include a timezone")
        self.start_time = self.start_time.astimezone(UTC)
        return self


class KafkaSettings(BaseModel):
    """Runtime settings for Kafka clients and topic provisioning."""

    model_config = ConfigDict(extra="forbid")

    bootstrap_servers: str = "localhost:9092"
    client_id: str = "sentinelstream"
    consumer_group: str = "sentinelstream-risk-engine"
    topic_partitions: int = Field(default=6, ge=1)
    replication_factor: int = Field(default=1, ge=1)
    request_timeout_ms: int = Field(default=30_000, ge=1_000)
    max_poll_records: int = Field(default=100, ge=1)
    retry_attempts: int = Field(default=3, ge=1)
    retry_backoff_seconds: float = Field(default=0.25, ge=0.0)
    auto_offset_reset: Literal["earliest", "latest"] = "latest"
    auto_create_topics: bool = True
    security_protocol: str = "PLAINTEXT"
    sasl_mechanisms: str | None = None
    sasl_username: SecretStr | None = None
    sasl_password: SecretStr | None = None


class DatabaseSettings(BaseModel):
    """Relational persistence settings shared by repositories and migrations."""

    model_config = ConfigDict(extra="forbid")

    url: str | None = None
    echo: bool = False
    pool_size: int = Field(default=5, ge=1)
    max_overflow: int = Field(default=10, ge=0)
    pool_timeout_seconds: float = Field(default=30.0, gt=0.0)


class RedisSettings(BaseModel):
    """Redis cache settings with safe local fallback defaults."""

    model_config = ConfigDict(extra="forbid")

    url: str = "redis://localhost:6379/0"
    enabled: bool = True
    default_ttl_seconds: int = Field(default=300, ge=1)
    socket_timeout_seconds: float = Field(default=1.0, gt=0.0)
    fallback_max_entries: int = Field(default=10_000, ge=100)


class APISettings(BaseModel):
    """HTTP service, JWT, and request policy settings."""

    model_config = ConfigDict(extra="forbid")

    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65_535)
    jwt_algorithm: Literal["HS256"] = "HS256"
    access_token_minutes: int = Field(default=30, ge=1)
    cors_origins: tuple[str, ...] = ()
    max_request_bytes: int = Field(default=1_000_000, ge=1_024)


class SecuritySettings(BaseModel):
    """JWT, privacy, and secure-runtime policy settings."""

    model_config = ConfigDict(extra="forbid")

    issuer: str | None = None
    audience: str | None = None
    token_revocation_enabled: bool = True
    pii_masking_enabled: bool = True
    log_redaction_enabled: bool = True


class RateLimitSettings(BaseModel):
    """Fixed-window request limits backed by Redis when available."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    requests: int = Field(default=120, ge=1)
    window_seconds: int = Field(default=60, ge=1)
    authentication_requests: int = Field(default=10, ge=1)
    authentication_window_seconds: int = Field(default=60, ge=1)
    key_prefix: str = "sentinelstream:ratelimit"


class HybridSettings(BaseModel):
    """Weights and policy thresholds for the hybrid fraud engine."""

    model_config = ConfigDict(extra="forbid")

    supervised_weight: float = Field(default=0.40, ge=0.0, le=1.0)
    anomaly_weight: float = Field(default=0.15, ge=0.0, le=1.0)
    rules_weight: float = Field(default=0.15, ge=0.0, le=1.0)
    customer_profile_weight: float = Field(default=0.10, ge=0.0, le=1.0)
    merchant_profile_weight: float = Field(default=0.10, ge=0.0, le=1.0)
    device_profile_weight: float = Field(default=0.10, ge=0.0, le=1.0)
    review_threshold: float = Field(default=0.50, ge=0.0, lt=1.0)
    block_threshold: float = Field(default=0.85, gt=0.0, le=1.0)
    min_confidence: float = Field(default=0.20, ge=0.0, le=1.0)
    policy_version: str = "phase8-hybrid-policy-v1"

    @model_validator(mode="after")
    def validate_weights_and_thresholds(self) -> HybridSettings:
        weights = (
            self.supervised_weight,
            self.anomaly_weight,
            self.rules_weight,
            self.customer_profile_weight,
            self.merchant_profile_weight,
            self.device_profile_weight,
        )
        if not any(weight > 0.0 for weight in weights):
            raise ValueError("at least one hybrid source weight must be positive")
        if self.review_threshold >= self.block_threshold:
            raise ValueError("review_threshold must be below block_threshold")
        if not self.policy_version.strip():
            raise ValueError("policy_version must not be empty")
        return self


class DashboardSettings(BaseModel):
    """Dashboard API and refresh settings."""

    model_config = ConfigDict(extra="forbid")

    api_url: str = "http://127.0.0.1:8000"
    metrics_url: str | None = None
    refresh_seconds: float = Field(default=10.0, gt=0.0)
    request_timeout_seconds: float = Field(default=5.0, gt=0.0)
    page_size: int = Field(default=50, ge=1, le=200)
    verify_tls: bool = True
    auth_username: str | None = None
    auth_password: SecretStr | None = None


class MLOpsSettings(BaseModel):
    """MLflow, model-promotion, dataset, and tuning settings."""

    model_config = ConfigDict(extra="forbid")

    tracking_uri: str = "sqlite:///mlflow.db"
    registry_uri: str | None = None
    experiment_name: str = "sentinelstream-fraud"
    registered_model_name: str = "sentinelstream-fraud-model"
    artifact_dir: Path = Path("reports/experiments")
    dataset_metadata_dir: Path = Path("data/metadata")
    minimum_recall: float | None = Field(default=None, ge=0.0, le=1.0)
    minimum_precision: float | None = Field(default=None, ge=0.0, le=1.0)
    maximum_fraud_cost: float | None = Field(default=None, ge=0.0)
    maximum_calibration_error: float | None = Field(default=None, ge=0.0, le=1.0)
    optuna_study_name: str = "sentinelstream-fraud-tuning"
    optuna_storage: str | None = None
    optuna_trials: int = Field(default=10, ge=1)
    optuna_timeout_seconds: int | None = Field(default=300, ge=1)
    random_seed: int = Field(default=42, ge=0)
    tracking_username: str | None = None
    tracking_password: SecretStr | None = None
    tracking_token: SecretStr | None = None


class MonitoringSettings(BaseModel):
    """Prometheus, dependency health, and drift-monitoring settings."""

    model_config = ConfigDict(extra="forbid")

    prometheus_enabled: bool = True
    prometheus_namespace: str = "sentinelstream"
    drift_report_dir: Path = Path("reports/drift")
    reference_data_path: Path | None = None
    current_data_path: Path | None = None
    feature_drift_threshold: float = Field(default=0.20, ge=0.0, le=1.0)
    prediction_drift_threshold: float = Field(default=0.20, ge=0.0, le=1.0)
    health_timeout_seconds: float = Field(default=2.0, gt=0.0)
    kafka_health_url: str | None = None
    spark_health_url: str | None = None
    mlflow_health_url: str | None = None
    grafana_url: str | None = None


class BenchmarkSettings(BaseModel):
    """Safe defaults for reproducible, bounded local benchmarks."""

    model_config = ConfigDict(extra="forbid")

    iterations: int = Field(default=100, ge=1)
    warmup_iterations: int = Field(default=10, ge=0)
    api_url: str = "http://127.0.0.1:8000"
    concurrency: int = Field(default=1, ge=1)
    timeout_seconds: float = Field(default=5.0, gt=0.0)
    random_seed: int = Field(default=42, ge=0)


class SparkSettings(BaseModel):
    """Structured Streaming runtime, event-time, model, and sink settings."""

    model_config = ConfigDict(extra="forbid")

    app_name: str = "SentinelStreamStructuredStreaming"
    master: str = "local[*]"
    checkpoint_dir: Path = Path("data/checkpoints/sentinelstream")
    batch_interval_seconds: float = Field(default=5.0, gt=0.0)
    watermark_duration: str = "15 minutes"
    state_timeout_duration: str = "24 hours"
    history_retention_hours: float = Field(default=24.0, gt=0.0)
    starting_offsets: Literal["earliest", "latest"] = "latest"
    max_offsets_per_trigger: int | None = Field(default=None, ge=1)
    shuffle_partitions: int = Field(default=4, ge=1)
    kafka_connector_package: str | None = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    jdbc_driver_package: str | None = "org.postgresql:postgresql:42.7.5"
    source_topic: str = "sentinelstream.validated-transactions.v1"
    input_message_kind: Literal["transaction", "validated"] = "validated"
    validated_topic: str = "sentinelstream.validated-transactions.v1"
    prediction_topic: str = "sentinelstream.predictions.v1"
    alert_topic: str = "sentinelstream.alerts.v1"
    audit_topic: str = "sentinelstream.audit.v1"
    dead_letter_topic: str = "sentinelstream.dead-letter.v1"
    parquet_output_dir: Path = Path("data/streaming")
    postgres_jdbc_url: str | None = None
    postgres_table: str = "sentinelstream_stream_events"
    postgres_user: str | None = None
    postgres_password: SecretStr | None = None
    model_path: Path | None = None
    model_version: str = "unknown"
    calibration_version: str = "unknown"
    model_probability_weight: float = Field(default=0.70, ge=0.0, le=1.0)
    rule_probability_weight: float = Field(default=0.20, ge=0.0, le=1.0)
    anomaly_probability_weight: float = Field(default=0.10, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def validate_probability_weights(self) -> SparkSettings:
        total = (
            self.model_probability_weight
            + self.rule_probability_weight
            + self.anomaly_probability_weight
        )
        if abs(total - 1.0) > 1e-9:
            raise ValueError("Spark prediction probability weights must sum to 1")
        return self


class AppSettings(BaseSettings):
    """Validated application settings for local and future service runtimes."""

    model_config = SettingsConfigDict(
        env_prefix="SENTINELSTREAM_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    app_name: str = "SentinelStream"
    environment: Environment = Environment.DEVELOPMENT
    timezone: str = "UTC"
    data_dir: Path = Path("data")
    log_level: str = "INFO"
    database_url: str | None = None
    secret_key: SecretStr | None = None
    simulation: SimulationSettings = Field(default_factory=SimulationSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    api: APISettings = Field(default_factory=APISettings)
    hybrid: HybridSettings = Field(default_factory=HybridSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    rate_limit: RateLimitSettings = Field(default_factory=RateLimitSettings)
    dashboard: DashboardSettings = Field(default_factory=DashboardSettings)
    mlops: MLOpsSettings = Field(default_factory=MLOpsSettings)
    monitoring: MonitoringSettings = Field(default_factory=MonitoringSettings)
    benchmark: BenchmarkSettings = Field(default_factory=BenchmarkSettings)

    @model_validator(mode="after")
    def validate_runtime_secrets(self) -> AppSettings:
        if self.environment is Environment.PRODUCTION:
            if self.secret_key is None:
                raise ValueError("secret_key is required in production")
            secret = self.secret_key.get_secret_value()
            if len(secret) < 32:
                raise ValueError("production secret_key must be at least 32 characters")
            if secret in {
                "local-development-secret-change-me-32-chars",
                "replace-with-a-long-random-secret",
                "replace-me",
            }:
                raise ValueError("production secret_key must not use a development placeholder")
            if not (self.database.url or self.database_url):
                raise ValueError("database.url is required in production")
        return self


_NESTED_ENV_FIELDS = {
    "SENTINELSTREAM_SIMULATION__RANDOM_SEED": ("simulation", "random_seed"),
    "SENTINELSTREAM_SIMULATION__CUSTOMER_COUNT": ("simulation", "customer_count"),
    "SENTINELSTREAM_SIMULATION__MERCHANT_COUNT": ("simulation", "merchant_count"),
    "SENTINELSTREAM_SIMULATION__FRAUD_RATIO": ("simulation", "fraud_ratio"),
    "SENTINELSTREAM_SIMULATION__MIN_AMOUNT": ("simulation", "min_amount"),
    "SENTINELSTREAM_SIMULATION__MAX_AMOUNT": ("simulation", "max_amount"),
    "SENTINELSTREAM_SIMULATION__START_TIME": ("simulation", "start_time"),
    "SENTINELSTREAM_SIMULATION__DURATION_DAYS": ("simulation", "duration_days"),
    "SENTINELSTREAM_SIMULATION__DUPLICATE_RATE": ("simulation", "duplicate_rate"),
    "SENTINELSTREAM_SIMULATION__LATE_EVENT_RATE": ("simulation", "late_event_rate"),
    "SENTINELSTREAM_SIMULATION__MAX_DELAY_SECONDS": ("simulation", "max_delay_seconds"),
    "SENTINELSTREAM_SIMULATION__MALFORMED_RATE": ("simulation", "malformed_rate"),
    "SENTINELSTREAM_SIMULATION__OUT_OF_ORDER_RATE": ("simulation", "out_of_order_rate"),
    "SENTINELSTREAM_SIMULATION__BURST_PROBABILITY": ("simulation", "burst_probability"),
    "SENTINELSTREAM_SIMULATION__DRIFT_ENABLED": ("simulation", "drift_enabled"),
    "SENTINELSTREAM_SIMULATION__DRIFT_AFTER_FRACTION": ("simulation", "drift_after_fraction"),
    "SENTINELSTREAM_SIMULATION__REPLAY": ("simulation", "replay"),
    "SENTINELSTREAM_KAFKA__BOOTSTRAP_SERVERS": ("kafka", "bootstrap_servers"),
    "SENTINELSTREAM_KAFKA__CLIENT_ID": ("kafka", "client_id"),
    "SENTINELSTREAM_KAFKA__CONSUMER_GROUP": ("kafka", "consumer_group"),
    "SENTINELSTREAM_KAFKA__TOPIC_PARTITIONS": ("kafka", "topic_partitions"),
    "SENTINELSTREAM_KAFKA__REPLICATION_FACTOR": ("kafka", "replication_factor"),
    "SENTINELSTREAM_KAFKA__REQUEST_TIMEOUT_MS": ("kafka", "request_timeout_ms"),
    "SENTINELSTREAM_KAFKA__MAX_POLL_RECORDS": ("kafka", "max_poll_records"),
    "SENTINELSTREAM_KAFKA__RETRY_ATTEMPTS": ("kafka", "retry_attempts"),
    "SENTINELSTREAM_KAFKA__RETRY_BACKOFF_SECONDS": ("kafka", "retry_backoff_seconds"),
    "SENTINELSTREAM_KAFKA__AUTO_OFFSET_RESET": ("kafka", "auto_offset_reset"),
    "SENTINELSTREAM_KAFKA__AUTO_CREATE_TOPICS": ("kafka", "auto_create_topics"),
    "SENTINELSTREAM_KAFKA__SECURITY_PROTOCOL": ("kafka", "security_protocol"),
    "SENTINELSTREAM_KAFKA__SASL_MECHANISMS": ("kafka", "sasl_mechanisms"),
    "SENTINELSTREAM_KAFKA__SASL_USERNAME": ("kafka", "sasl_username"),
    "SENTINELSTREAM_KAFKA__SASL_PASSWORD": ("kafka", "sasl_password"),
    "SENTINELSTREAM_DATABASE__URL": ("database", "url"),
    "SENTINELSTREAM_DATABASE__ECHO": ("database", "echo"),
    "SENTINELSTREAM_DATABASE__POOL_SIZE": ("database", "pool_size"),
    "SENTINELSTREAM_DATABASE__MAX_OVERFLOW": ("database", "max_overflow"),
    "SENTINELSTREAM_DATABASE__POOL_TIMEOUT_SECONDS": (
        "database",
        "pool_timeout_seconds",
    ),
    "SENTINELSTREAM_REDIS__URL": ("redis", "url"),
    "SENTINELSTREAM_REDIS__ENABLED": ("redis", "enabled"),
    "SENTINELSTREAM_REDIS__DEFAULT_TTL_SECONDS": ("redis", "default_ttl_seconds"),
    "SENTINELSTREAM_REDIS__SOCKET_TIMEOUT_SECONDS": (
        "redis",
        "socket_timeout_seconds",
    ),
    "SENTINELSTREAM_REDIS__FALLBACK_MAX_ENTRIES": ("redis", "fallback_max_entries"),
    "SENTINELSTREAM_API__HOST": ("api", "host"),
    "SENTINELSTREAM_API__PORT": ("api", "port"),
    "SENTINELSTREAM_API__JWT_ALGORITHM": ("api", "jwt_algorithm"),
    "SENTINELSTREAM_API__ACCESS_TOKEN_MINUTES": ("api", "access_token_minutes"),
    "SENTINELSTREAM_API__CORS_ORIGINS": ("api", "cors_origins"),
    "SENTINELSTREAM_API__MAX_REQUEST_BYTES": ("api", "max_request_bytes"),
    "SENTINELSTREAM_SECURITY__ISSUER": ("security", "issuer"),
    "SENTINELSTREAM_SECURITY__AUDIENCE": ("security", "audience"),
    "SENTINELSTREAM_SECURITY__TOKEN_REVOCATION_ENABLED": (
        "security",
        "token_revocation_enabled",
    ),
    "SENTINELSTREAM_SECURITY__PII_MASKING_ENABLED": ("security", "pii_masking_enabled"),
    "SENTINELSTREAM_SECURITY__LOG_REDACTION_ENABLED": ("security", "log_redaction_enabled"),
    "SENTINELSTREAM_RATE_LIMIT__ENABLED": ("rate_limit", "enabled"),
    "SENTINELSTREAM_RATE_LIMIT__REQUESTS": ("rate_limit", "requests"),
    "SENTINELSTREAM_RATE_LIMIT__WINDOW_SECONDS": ("rate_limit", "window_seconds"),
    "SENTINELSTREAM_RATE_LIMIT__AUTHENTICATION_REQUESTS": (
        "rate_limit",
        "authentication_requests",
    ),
    "SENTINELSTREAM_RATE_LIMIT__AUTHENTICATION_WINDOW_SECONDS": (
        "rate_limit",
        "authentication_window_seconds",
    ),
    "SENTINELSTREAM_RATE_LIMIT__KEY_PREFIX": ("rate_limit", "key_prefix"),
    "SENTINELSTREAM_HYBRID__SUPERVISED_WEIGHT": ("hybrid", "supervised_weight"),
    "SENTINELSTREAM_HYBRID__ANOMALY_WEIGHT": ("hybrid", "anomaly_weight"),
    "SENTINELSTREAM_HYBRID__RULES_WEIGHT": ("hybrid", "rules_weight"),
    "SENTINELSTREAM_HYBRID__CUSTOMER_PROFILE_WEIGHT": (
        "hybrid",
        "customer_profile_weight",
    ),
    "SENTINELSTREAM_HYBRID__MERCHANT_PROFILE_WEIGHT": (
        "hybrid",
        "merchant_profile_weight",
    ),
    "SENTINELSTREAM_HYBRID__DEVICE_PROFILE_WEIGHT": (
        "hybrid",
        "device_profile_weight",
    ),
    "SENTINELSTREAM_HYBRID__REVIEW_THRESHOLD": ("hybrid", "review_threshold"),
    "SENTINELSTREAM_HYBRID__BLOCK_THRESHOLD": ("hybrid", "block_threshold"),
    "SENTINELSTREAM_HYBRID__MIN_CONFIDENCE": ("hybrid", "min_confidence"),
    "SENTINELSTREAM_HYBRID__POLICY_VERSION": ("hybrid", "policy_version"),
    "SENTINELSTREAM_DASHBOARD__API_URL": ("dashboard", "api_url"),
    "SENTINELSTREAM_DASHBOARD__METRICS_URL": ("dashboard", "metrics_url"),
    "SENTINELSTREAM_DASHBOARD__REFRESH_SECONDS": ("dashboard", "refresh_seconds"),
    "SENTINELSTREAM_DASHBOARD__REQUEST_TIMEOUT_SECONDS": (
        "dashboard",
        "request_timeout_seconds",
    ),
    "SENTINELSTREAM_DASHBOARD__PAGE_SIZE": ("dashboard", "page_size"),
    "SENTINELSTREAM_DASHBOARD__VERIFY_TLS": ("dashboard", "verify_tls"),
    "SENTINELSTREAM_DASHBOARD__AUTH_USERNAME": ("dashboard", "auth_username"),
    "SENTINELSTREAM_DASHBOARD__AUTH_PASSWORD": ("dashboard", "auth_password"),
    "SENTINELSTREAM_MLOPS__TRACKING_URI": ("mlops", "tracking_uri"),
    "SENTINELSTREAM_MLOPS__REGISTRY_URI": ("mlops", "registry_uri"),
    "SENTINELSTREAM_MLOPS__EXPERIMENT_NAME": ("mlops", "experiment_name"),
    "SENTINELSTREAM_MLOPS__REGISTERED_MODEL_NAME": ("mlops", "registered_model_name"),
    "SENTINELSTREAM_MLOPS__ARTIFACT_DIR": ("mlops", "artifact_dir"),
    "SENTINELSTREAM_MLOPS__DATASET_METADATA_DIR": ("mlops", "dataset_metadata_dir"),
    "SENTINELSTREAM_MLOPS__MINIMUM_RECALL": ("mlops", "minimum_recall"),
    "SENTINELSTREAM_MLOPS__MINIMUM_PRECISION": ("mlops", "minimum_precision"),
    "SENTINELSTREAM_MLOPS__MAXIMUM_FRAUD_COST": ("mlops", "maximum_fraud_cost"),
    "SENTINELSTREAM_MLOPS__MAXIMUM_CALIBRATION_ERROR": (
        "mlops",
        "maximum_calibration_error",
    ),
    "SENTINELSTREAM_MLOPS__OPTUNA_STUDY_NAME": ("mlops", "optuna_study_name"),
    "SENTINELSTREAM_MLOPS__OPTUNA_STORAGE": ("mlops", "optuna_storage"),
    "SENTINELSTREAM_MLOPS__OPTUNA_TRIALS": ("mlops", "optuna_trials"),
    "SENTINELSTREAM_MLOPS__OPTUNA_TIMEOUT_SECONDS": (
        "mlops",
        "optuna_timeout_seconds",
    ),
    "SENTINELSTREAM_MLOPS__RANDOM_SEED": ("mlops", "random_seed"),
    "SENTINELSTREAM_MLOPS__TRACKING_USERNAME": ("mlops", "tracking_username"),
    "SENTINELSTREAM_MLOPS__TRACKING_PASSWORD": ("mlops", "tracking_password"),
    "SENTINELSTREAM_MLOPS__TRACKING_TOKEN": ("mlops", "tracking_token"),
    "SENTINELSTREAM_MONITORING__PROMETHEUS_ENABLED": (
        "monitoring",
        "prometheus_enabled",
    ),
    "SENTINELSTREAM_MONITORING__PROMETHEUS_NAMESPACE": (
        "monitoring",
        "prometheus_namespace",
    ),
    "SENTINELSTREAM_MONITORING__DRIFT_REPORT_DIR": ("monitoring", "drift_report_dir"),
    "SENTINELSTREAM_MONITORING__REFERENCE_DATA_PATH": (
        "monitoring",
        "reference_data_path",
    ),
    "SENTINELSTREAM_MONITORING__CURRENT_DATA_PATH": ("monitoring", "current_data_path"),
    "SENTINELSTREAM_MONITORING__FEATURE_DRIFT_THRESHOLD": (
        "monitoring",
        "feature_drift_threshold",
    ),
    "SENTINELSTREAM_MONITORING__PREDICTION_DRIFT_THRESHOLD": (
        "monitoring",
        "prediction_drift_threshold",
    ),
    "SENTINELSTREAM_MONITORING__HEALTH_TIMEOUT_SECONDS": (
        "monitoring",
        "health_timeout_seconds",
    ),
    "SENTINELSTREAM_MONITORING__KAFKA_HEALTH_URL": ("monitoring", "kafka_health_url"),
    "SENTINELSTREAM_MONITORING__SPARK_HEALTH_URL": ("monitoring", "spark_health_url"),
    "SENTINELSTREAM_MONITORING__MLFLOW_HEALTH_URL": ("monitoring", "mlflow_health_url"),
    "SENTINELSTREAM_MONITORING__GRAFANA_URL": ("monitoring", "grafana_url"),
    "SENTINELSTREAM_BENCHMARK__ITERATIONS": ("benchmark", "iterations"),
    "SENTINELSTREAM_BENCHMARK__WARMUP_ITERATIONS": (
        "benchmark",
        "warmup_iterations",
    ),
    "SENTINELSTREAM_BENCHMARK__API_URL": ("benchmark", "api_url"),
    "SENTINELSTREAM_BENCHMARK__CONCURRENCY": ("benchmark", "concurrency"),
    "SENTINELSTREAM_BENCHMARK__TIMEOUT_SECONDS": ("benchmark", "timeout_seconds"),
    "SENTINELSTREAM_BENCHMARK__RANDOM_SEED": ("benchmark", "random_seed"),
    "SENTINELSTREAM_SPARK__APP_NAME": ("spark", "app_name"),
    "SENTINELSTREAM_SPARK__MASTER": ("spark", "master"),
    "SENTINELSTREAM_SPARK__CHECKPOINT_DIR": ("spark", "checkpoint_dir"),
    "SENTINELSTREAM_SPARK__BATCH_INTERVAL_SECONDS": (
        "spark",
        "batch_interval_seconds",
    ),
    "SENTINELSTREAM_SPARK__WATERMARK_DURATION": ("spark", "watermark_duration"),
    "SENTINELSTREAM_SPARK__STATE_TIMEOUT_DURATION": ("spark", "state_timeout_duration"),
    "SENTINELSTREAM_SPARK__HISTORY_RETENTION_HOURS": (
        "spark",
        "history_retention_hours",
    ),
    "SENTINELSTREAM_SPARK__STARTING_OFFSETS": ("spark", "starting_offsets"),
    "SENTINELSTREAM_SPARK__MAX_OFFSETS_PER_TRIGGER": (
        "spark",
        "max_offsets_per_trigger",
    ),
    "SENTINELSTREAM_SPARK__SHUFFLE_PARTITIONS": ("spark", "shuffle_partitions"),
    "SENTINELSTREAM_SPARK__KAFKA_CONNECTOR_PACKAGE": (
        "spark",
        "kafka_connector_package",
    ),
    "SENTINELSTREAM_SPARK__JDBC_DRIVER_PACKAGE": ("spark", "jdbc_driver_package"),
    "SENTINELSTREAM_SPARK__SOURCE_TOPIC": ("spark", "source_topic"),
    "SENTINELSTREAM_SPARK__INPUT_MESSAGE_KIND": ("spark", "input_message_kind"),
    "SENTINELSTREAM_SPARK__VALIDATED_TOPIC": ("spark", "validated_topic"),
    "SENTINELSTREAM_SPARK__PREDICTION_TOPIC": ("spark", "prediction_topic"),
    "SENTINELSTREAM_SPARK__ALERT_TOPIC": ("spark", "alert_topic"),
    "SENTINELSTREAM_SPARK__AUDIT_TOPIC": ("spark", "audit_topic"),
    "SENTINELSTREAM_SPARK__DEAD_LETTER_TOPIC": ("spark", "dead_letter_topic"),
    "SENTINELSTREAM_SPARK__PARQUET_OUTPUT_DIR": ("spark", "parquet_output_dir"),
    "SENTINELSTREAM_SPARK__POSTGRES_JDBC_URL": ("spark", "postgres_jdbc_url"),
    "SENTINELSTREAM_SPARK__POSTGRES_TABLE": ("spark", "postgres_table"),
    "SENTINELSTREAM_SPARK__POSTGRES_USER": ("spark", "postgres_user"),
    "SENTINELSTREAM_SPARK__POSTGRES_PASSWORD": ("spark", "postgres_password"),
    "SENTINELSTREAM_SPARK__MODEL_PATH": ("spark", "model_path"),
    "SENTINELSTREAM_SPARK__MODEL_VERSION": ("spark", "model_version"),
    "SENTINELSTREAM_SPARK__CALIBRATION_VERSION": ("spark", "calibration_version"),
    "SENTINELSTREAM_SPARK__MODEL_PROBABILITY_WEIGHT": (
        "spark",
        "model_probability_weight",
    ),
    "SENTINELSTREAM_SPARK__RULE_PROBABILITY_WEIGHT": (
        "spark",
        "rule_probability_weight",
    ),
    "SENTINELSTREAM_SPARK__ANOMALY_PROBABILITY_WEIGHT": (
        "spark",
        "anomaly_probability_weight",
    ),
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file does not exist: {path}")
    with path.open("r", encoding="utf-8") as handle:
        values = yaml.safe_load(handle) or {}
    if not isinstance(values, dict):
        raise ValueError(f"Configuration root must be a mapping: {path}")
    return values


def _environment_values() -> dict[str, Any]:
    values: dict[str, Any] = {}
    dotenv_values_map = {
        key: value for key, value in dotenv_values(".env").items() if value is not None
    }
    dotenv_values_map.update(os.environ)
    scalar_fields = {
        "SENTINELSTREAM_APP_NAME": "app_name",
        "SENTINELSTREAM_ENVIRONMENT": "environment",
        "SENTINELSTREAM_TIMEZONE": "timezone",
        "SENTINELSTREAM_DATA_DIR": "data_dir",
        "SENTINELSTREAM_LOG_LEVEL": "log_level",
        "SENTINELSTREAM_DATABASE_URL": "database_url",
        "SENTINELSTREAM_SECRET_KEY": "secret_key",  # nosec B105
    }
    for environment_name, field_name in scalar_fields.items():
        if environment_name in dotenv_values_map:
            values[field_name] = dotenv_values_map[environment_name]
    for environment_name, path in _NESTED_ENV_FIELDS.items():
        if environment_name in dotenv_values_map:
            values.setdefault(path[0], {})[path[1]] = dotenv_values_map[environment_name]
    return values


def load_settings(
    config_path: Path | None = None,
    *,
    environment: Environment | str | None = None,
) -> AppSettings:
    """Load base/profile YAML and apply environment variables last."""

    requested_environment = environment or _environment_values().get("environment", "development")
    environment_name = Environment(requested_environment).value
    if config_path is not None:
        values = _read_yaml(config_path)
    else:
        values = _read_yaml(Path("configs/base.yaml"))
        profile_path = Path("configs") / f"{environment_name}.yaml"
        if profile_path.is_file():
            values = _deep_merge(values, _read_yaml(profile_path))
    values = _deep_merge(values, _environment_values())
    if environment is not None:
        values["environment"] = Environment(environment).value
    return AppSettings(**values)
