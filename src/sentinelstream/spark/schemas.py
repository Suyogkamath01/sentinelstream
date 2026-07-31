"""Spark SQL schemas shared by the Kafka, feature, and prediction stages."""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("card_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("transaction_amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("country", StringType(), False),
        StructField("city", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("device_id", StringType(), False),
        StructField("ip_address", StringType(), False),
        StructField("card_present", BooleanType(), False),
        StructField("authentication_method", StringType(), False),
        StructField("transaction_status", StringType(), False),
        StructField("event_version", LongType(), False),
        StructField("ingestion_timestamp", StringType(), False),
    ]
)

TRANSACTION_MESSAGE_SCHEMA = StructType(
    [
        StructField("message_id", StringType(), False),
        StructField("schema_version", StringType(), False),
        StructField("produced_at", StringType(), False),
        StructField("correlation_id", StringType(), True),
        StructField("validation_version", StringType(), True),
        StructField("event", EVENT_SCHEMA, False),
    ]
)


def feature_schema(feature_columns: tuple[str, ...]) -> StructType:
    """Build the schema emitted by the per-customer stateful feature function."""

    fields = [
        StructField("event_json", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("country", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("transaction_amount", DoubleType(), False),
        StructField("accepted", BooleanType(), False),
        StructField("reason", StringType(), True),
    ]
    fields.extend(StructField(name, DoubleType(), False) for name in feature_columns)
    return StructType(fields)


FEATURE_STATE_SCHEMA = StructType([StructField("events_json", StringType(), False)])
