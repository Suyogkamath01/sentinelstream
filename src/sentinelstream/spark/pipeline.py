"""End-to-end Spark Structured Streaming topology for SentinelStream."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    create_map,
    current_timestamp,
    from_json,
    lit,
    struct,
    to_json,
    udf,
)
from pyspark.sql.types import ArrayType, MapType, StringType

from sentinelstream.config.settings import AppSettings
from sentinelstream.models.decision import RecommendedAction, ThresholdPolicy
from sentinelstream.spark.ingestion import (
    SparkKafkaTransactionReader,
    ValidatedStreams,
    kafka_records,
    validated_kafka_records,
)
from sentinelstream.spark.metrics import SparkStreamingMetrics, timed_batch
from sentinelstream.spark.prediction import (
    SparkPredictionEngine,
    build_prediction_stream,
    load_model_bundle,
)
from sentinelstream.spark.runtime import create_spark_session
from sentinelstream.spark.sinks import SparkBatchStorage, write_kafka_batch
from sentinelstream.spark.state import (
    StatefulFeatureConfig,
    build_stateful_feature_stream,
    duration_to_timedelta,
)
from sentinelstream.spark.windows import build_window_metrics_stream
from sentinelstream.streaming.contracts import STREAM_SCHEMA_VERSION
from sentinelstream.streaming.observability import log_event


def _derived_message_id(prediction_message_id: str, kind: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"sentinelstream:{kind}:{prediction_message_id}"))


_derived_message_id_udf = udf(_derived_message_id, StringType(), useArrow=False)


@dataclass(frozen=True, slots=True)
class SparkPipelineStreams:
    """Logical stream branches produced before queries are started."""

    source: DataFrame
    valid: DataFrame
    malformed: DataFrame
    features: DataFrame
    predictions: DataFrame
    window_metrics: DataFrame


@dataclass(slots=True)
class SparkStreamingQueries:
    """Handles for the independently checkpointed queries in one topology."""

    prediction: Any
    malformed: Any
    validated: Any | None = None
    windowed: Any | None = None

    def stop(self) -> None:
        for query in (self.prediction, self.malformed, self.validated, self.windowed):
            if query is not None and query.isActive:
                query.stop()

    def await_termination(self, timeout: float | None = None) -> None:
        self.prediction.awaitTermination(timeout)


@dataclass(slots=True)
class SparkStructuredStreamingPipeline:
    """Build and run the checkpointed Kafka-to-prediction Spark topology."""

    settings: AppSettings
    spark: SparkSession | None = None
    metrics: SparkStreamingMetrics = field(default_factory=SparkStreamingMetrics)
    policy: ThresholdPolicy = field(default_factory=ThresholdPolicy)
    _engine: SparkPredictionEngine | None = field(default=None, init=False)
    _storage: SparkBatchStorage | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if self.spark is None:
            self.spark = create_spark_session(self.settings.spark)
        bundle = (
            load_model_bundle(self.settings.spark.model_path)
            if self.settings.spark.model_path is not None
            else None
        )
        self._engine = SparkPredictionEngine(
            bundle=bundle,
            model_version=self.settings.spark.model_version,
            calibration_version=self.settings.spark.calibration_version,
            model_probability_weight=self.settings.spark.model_probability_weight,
            rule_probability_weight=self.settings.spark.rule_probability_weight,
            anomaly_probability_weight=self.settings.spark.anomaly_probability_weight,
            policy=self.policy,
        )
        self._storage = SparkBatchStorage(
            parquet_output_dir=self.settings.spark.parquet_output_dir,
            postgres_jdbc_url=self.settings.spark.postgres_jdbc_url,
            postgres_table=self.settings.spark.postgres_table,
            postgres_user=self.settings.spark.postgres_user,
            postgres_password=(
                self.settings.spark.postgres_password.get_secret_value()
                if self.settings.spark.postgres_password is not None
                else None
            ),
        )

    @property
    def engine(self) -> SparkPredictionEngine:
        if self._engine is None:
            raise RuntimeError("Spark prediction engine has not been initialised")
        return self._engine

    @property
    def storage(self) -> SparkBatchStorage:
        if self._storage is None:
            raise RuntimeError("Spark storage writer has not been initialised")
        return self._storage

    def build_streams(self) -> SparkPipelineStreams:
        """Build the source, validation, state, and prediction branches."""

        if self.spark is None:
            raise RuntimeError("Spark session has not been initialised")
        reader = SparkKafkaTransactionReader(
            bootstrap_servers=self.settings.kafka.bootstrap_servers,
            source_topic=self.settings.spark.source_topic,
            starting_offsets=self.settings.spark.starting_offsets,
            max_offsets_per_trigger=self.settings.spark.max_offsets_per_trigger,
            message_kind=self.settings.spark.input_message_kind,
        )
        source = reader.read(self.spark)
        validated: ValidatedStreams = reader.parse_and_validate(source)
        feature_config = StatefulFeatureConfig(
            watermark_duration=self.settings.spark.watermark_duration,
            allowed_lateness=duration_to_timedelta(self.settings.spark.watermark_duration),
            history_retention=timedelta(hours=self.settings.spark.history_retention_hours),
            state_timeout=duration_to_timedelta(self.settings.spark.state_timeout_duration),
        )
        features = build_stateful_feature_stream(validated.valid, feature_config)
        predictions = build_prediction_stream(features, self.engine)
        window_metrics = build_window_metrics_stream(
            validated.valid,
            watermark_duration=self.settings.spark.watermark_duration,
        )
        return SparkPipelineStreams(
            source=source,
            valid=validated.valid,
            malformed=validated.malformed,
            features=features,
            predictions=predictions,
            window_metrics=window_metrics,
        )

    def start(self) -> SparkStreamingQueries:
        """Start all query branches with independent checkpoint locations."""

        streams = self.build_streams()
        checkpoint_root = self.settings.spark.checkpoint_dir
        checkpoint_root.mkdir(parents=True, exist_ok=True)
        trigger = f"{self.settings.spark.batch_interval_seconds} seconds"

        malformed_query = (
            streams.malformed.writeStream.queryName("sentinelstream-dead-letter")
            .option("checkpointLocation", str(checkpoint_root / "dead-letter"))
            .trigger(processingTime=trigger)
            .foreachBatch(self._write_malformed_batch)
            .start()
        )
        validated_query = None
        if self.settings.spark.input_message_kind == "transaction":
            validated_query = (
                streams.valid.writeStream.queryName("sentinelstream-validated")
                .option("checkpointLocation", str(checkpoint_root / "validated"))
                .trigger(processingTime=trigger)
                .foreachBatch(self._write_validated_batch)
                .start()
            )
        window_query = (
            streams.window_metrics.writeStream.queryName("sentinelstream-window-metrics")
            .option("checkpointLocation", str(checkpoint_root / "windows"))
            .option("path", str(self.settings.spark.parquet_output_dir / "windows"))
            .outputMode("append")
            .format("parquet")
            .trigger(processingTime=trigger)
            .start()
        )
        prediction_query = (
            streams.predictions.writeStream.queryName("sentinelstream-predictions")
            .option("checkpointLocation", str(checkpoint_root / "predictions"))
            .trigger(processingTime=trigger)
            .foreachBatch(self._write_prediction_batch)
            .start()
        )
        log_event(
            self.metrics.logger,
            logging.INFO,
            "spark_stream_started",
            source_topic=self.settings.spark.source_topic,
            prediction_topic=self.settings.spark.prediction_topic,
            checkpoint_dir=str(checkpoint_root),
        )
        return SparkStreamingQueries(
            prediction=prediction_query,
            malformed=malformed_query,
            validated=validated_query,
            windowed=window_query,
        )

    def _write_malformed_batch(self, batch: DataFrame, batch_id: int) -> None:
        started = timed_batch()
        count = batch.count()
        if count:
            write_kafka_batch(
                kafka_records(batch, topic=self.settings.spark.dead_letter_topic),
                bootstrap_servers=self.settings.kafka.bootstrap_servers,
                topic=self.settings.spark.dead_letter_topic,
            )
        self.metrics.record_batch(
            batch_id=batch_id,
            processed_events=0,
            malformed_events=count,
            duration_ms=(timed_batch() - started) * 1_000,
        )

    def _write_validated_batch(self, batch: DataFrame, batch_id: int) -> None:
        del batch_id
        write_kafka_batch(
            validated_kafka_records(batch, topic=self.settings.spark.validated_topic),
            bootstrap_servers=self.settings.kafka.bootstrap_servers,
            topic=self.settings.spark.validated_topic,
        )

    def _prediction_messages(self, accepted: DataFrame) -> DataFrame:
        features = from_json(
            col("features_json"),
            "map<string,double>",
        )
        message = struct(
            col("prediction_message_id").alias("message_id"),
            lit(STREAM_SCHEMA_VERSION).alias("schema_version"),
            col("prediction_produced_at").alias("produced_at"),
            col("correlation_id"),
            col("event_id"),
            col("transaction_id"),
            col("customer_id"),
            features.alias("features"),
            col("model_probability"),
            col("calibrated_probability"),
            col("anomaly_score"),
            col("rule_score"),
            col("confidence"),
            col("model_version"),
            col("calibration_version"),
            col("feature_version"),
            lit(None).cast(MapType(StringType(), StringType())).alias("explanation"),
        )
        return accepted.select(
            col("customer_id").alias("key"),
            to_json(message).alias("value"),
        )

    def _alert_messages(self, accepted: DataFrame) -> DataFrame:
        reason_codes = from_json(
            col("reason_codes_json"),
            ArrayType(StringType(), containsNull=False),
        )
        decision = struct(
            col("calibrated_probability"),
            col("confidence"),
            col("abstained"),
            col("risk_tier"),
            col("action"),
            lit(self.policy.review_threshold).alias("review_threshold"),
            lit(self.policy.block_threshold).alias("block_threshold"),
            lit(self.policy.version).alias("policy_version"),
            col("model_version"),
            col("calibration_version"),
            reason_codes.alias("reason_codes"),
        ).alias("decision")
        message = struct(
            _derived_message_id_udf(col("prediction_message_id"), lit("alert")).alias("message_id"),
            lit(STREAM_SCHEMA_VERSION).alias("schema_version"),
            col("prediction_produced_at").alias("produced_at"),
            col("correlation_id"),
            col("event_id"),
            col("transaction_id"),
            col("customer_id"),
            decision,
            col("prediction_message_id").alias("prediction_message_id"),
            reason_codes.alias("reason_codes"),
            lit(None).cast(MapType(StringType(), StringType())).alias("explanation"),
        )
        return accepted.filter(
            col("action").isin(RecommendedAction.REVIEW.value, RecommendedAction.BLOCK.value)
        ).select(
            col("customer_id").alias("key"),
            to_json(message).alias("value"),
        )

    def _audit_messages(self, accepted: DataFrame) -> DataFrame:
        details = create_map(
            lit("action"),
            col("action"),
            lit("risk_tier"),
            col("risk_tier"),
            lit("final_risk_score"),
            col("final_risk_score").cast("string"),
        )
        predicted = struct(
            col("prediction_message_id").alias("message_id"),
            lit(STREAM_SCHEMA_VERSION).alias("schema_version"),
            col("prediction_produced_at").alias("produced_at"),
            col("correlation_id"),
            lit("predicted").alias("event_type"),
            col("action").alias("status"),
            lit(self.settings.spark.prediction_topic).alias("source_topic"),
            col("event_id"),
            col("transaction_id"),
            details.alias("details"),
        )
        alerted = accepted.filter(
            col("action").isin(RecommendedAction.REVIEW.value, RecommendedAction.BLOCK.value)
        ).select(
            col("customer_id").alias("key"),
            _derived_message_id_udf(col("prediction_message_id"), lit("audit-alert")).alias(
                "message_id"
            ),
            lit(STREAM_SCHEMA_VERSION).alias("schema_version"),
            col("prediction_produced_at").alias("produced_at"),
            col("correlation_id"),
            lit("alerted").alias("event_type"),
            col("action").alias("status"),
            lit(self.settings.spark.alert_topic).alias("source_topic"),
            col("event_id"),
            col("transaction_id"),
            details.alias("details"),
        )
        return accepted.select(
            col("customer_id").alias("key"),
            to_json(predicted).alias("value"),
        ).unionByName(
            alerted.select(
                col("key"),
                to_json(struct(*[col(field) for field in alerted.columns if field != "key"])).alias(
                    "value"
                ),
            )
        )

    def _write_prediction_batch(self, batch: DataFrame, batch_id: int) -> None:
        started = timed_batch()
        total = batch.count()
        accepted = batch.filter(col("accepted")).cache()
        processed = accepted.count()
        dropped = total - processed
        duplicate_events = batch.filter(col("reason") == "duplicate_event").count()
        late_events = batch.filter(col("reason") == "event_beyond_watermark").count()
        latency_row = (
            batch.select(
                (current_timestamp().cast("double") - col("event_time").cast("double")).alias(
                    "latency_seconds"
                )
            )
            .agg({"latency_seconds": "avg"})
            .collect()[0][0]
        )
        processing_latency_ms = max(float(latency_row or 0.0) * 1_000, 0.0)
        if total:
            self.storage.write(batch, batch_id=batch_id)
        if processed:
            write_kafka_batch(
                self._prediction_messages(accepted),
                bootstrap_servers=self.settings.kafka.bootstrap_servers,
                topic=self.settings.spark.prediction_topic,
            )
            write_kafka_batch(
                self._alert_messages(accepted),
                bootstrap_servers=self.settings.kafka.bootstrap_servers,
                topic=self.settings.spark.alert_topic,
            )
            write_kafka_batch(
                self._audit_messages(accepted),
                bootstrap_servers=self.settings.kafka.bootstrap_servers,
                topic=self.settings.spark.audit_topic,
            )
        accepted.unpersist()
        self.metrics.record_batch(
            batch_id=batch_id,
            processed_events=processed,
            dropped_events=dropped,
            duplicate_events=duplicate_events,
            late_events=late_events,
            duration_ms=(timed_batch() - started) * 1_000,
            processing_latency_ms=processing_latency_ms,
        )
