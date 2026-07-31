"""Event-time window aggregations used for velocity and activity monitoring."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    approx_count_distinct,
    col,
    count,
    lit,
    window,
)
from pyspark.sql.functions import sum as spark_sum


def build_sliding_customer_windows(
    events: DataFrame,
    *,
    window_duration: str = "1 hour",
    slide_duration: str = "10 minutes",
    watermark_duration: str = "15 minutes",
) -> DataFrame:
    """Aggregate customer velocity and diversity over overlapping event-time windows."""

    return (
        events.withWatermark("event_time", watermark_duration)
        .groupBy(
            window("event_time", window_duration, slide_duration),
            "customer_id",
        )
        .agg(
            count("event_id").alias("transaction_count"),
            spark_sum("transaction_amount").alias("transaction_amount_sum"),
            approx_count_distinct("merchant_id").alias("unique_merchants"),
            approx_count_distinct("device_id").alias("unique_devices"),
            approx_count_distinct("country").alias("unique_countries"),
            approx_count_distinct("customer_id").alias("unique_customers"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("sliding_customer").alias("window_type"),
            col("customer_id").alias("entity_id"),
            col("transaction_count"),
            col("transaction_amount_sum"),
            col("unique_merchants"),
            col("unique_devices"),
            col("unique_countries"),
            col("unique_customers"),
        )
    )


def build_tumbling_merchant_windows(
    events: DataFrame,
    *,
    window_duration: str = "1 hour",
    watermark_duration: str = "15 minutes",
) -> DataFrame:
    """Aggregate merchant popularity over non-overlapping event-time windows."""

    return (
        events.withWatermark("event_time", watermark_duration)
        .groupBy(window("event_time", window_duration), "merchant_id")
        .agg(
            count("event_id").alias("transaction_count"),
            spark_sum("transaction_amount").alias("transaction_amount_sum"),
            approx_count_distinct("customer_id").alias("unique_merchants"),
            approx_count_distinct("device_id").alias("unique_devices"),
            approx_count_distinct("country").alias("unique_countries"),
            approx_count_distinct("customer_id").alias("unique_customers"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("tumbling_merchant").alias("window_type"),
            col("merchant_id").alias("entity_id"),
            col("transaction_count"),
            col("transaction_amount_sum"),
            col("unique_merchants"),
            col("unique_devices"),
            col("unique_countries"),
            col("unique_customers"),
        )
    )


def build_window_metrics_stream(
    events: DataFrame,
    *,
    watermark_duration: str = "15 minutes",
) -> DataFrame:
    """Return a common schema for the sliding and tumbling monitoring streams."""

    return build_sliding_customer_windows(
        events,
        watermark_duration=watermark_duration,
    ).unionByName(
        build_tumbling_merchant_windows(
            events,
            watermark_duration=watermark_duration,
        )
    )
