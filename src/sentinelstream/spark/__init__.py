"""Spark Structured Streaming components for SentinelStream."""

from sentinelstream.spark.ingestion import SparkKafkaTransactionReader, ValidatedStreams
from sentinelstream.spark.metrics import MetricsSnapshot, SparkStreamingMetrics
from sentinelstream.spark.pipeline import SparkStructuredStreamingPipeline
from sentinelstream.spark.prediction import ModelBundle, SparkPredictionEngine
from sentinelstream.spark.state import (
    FEATURE_COLUMNS,
    StatefulFeatureConfig,
    build_feature_batch,
    build_stateful_feature_stream,
    duration_to_timedelta,
    process_event_sequence,
)
from sentinelstream.spark.windows import (
    build_sliding_customer_windows,
    build_tumbling_merchant_windows,
    build_window_metrics_stream,
)

__all__ = [
    "FEATURE_COLUMNS",
    "MetricsSnapshot",
    "ModelBundle",
    "SparkKafkaTransactionReader",
    "SparkPredictionEngine",
    "SparkStreamingMetrics",
    "SparkStructuredStreamingPipeline",
    "StatefulFeatureConfig",
    "ValidatedStreams",
    "build_feature_batch",
    "build_sliding_customer_windows",
    "build_stateful_feature_stream",
    "build_tumbling_merchant_windows",
    "build_window_metrics_stream",
    "duration_to_timedelta",
    "process_event_sequence",
]
