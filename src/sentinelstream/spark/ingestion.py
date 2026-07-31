"""Kafka ingestion, strict contract validation, and malformed-event routing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    from_json,
    lit,
    struct,
    substring,
    to_json,
    to_timestamp,
    udf,
)
from pyspark.sql.types import StringType

from sentinelstream.spark.schemas import TRANSACTION_MESSAGE_SCHEMA
from sentinelstream.streaming.contracts import STREAM_SCHEMA_VERSION, TransactionMessage
from sentinelstream.streaming.topics import TopicName, normalise_topic


def _validate_payload(payload: str | None, message_kind: str) -> str | None:
    if not payload:
        return "empty Kafka value"
    try:
        if message_kind == "validated":
            from sentinelstream.streaming.contracts import ValidatedTransactionMessage

            ValidatedTransactionMessage.model_validate_json(payload)
        else:
            TransactionMessage.model_validate_json(payload)
    except Exception as exc:
        return str(exc)[:2_000]
    return None


def _dead_letter_message_id(topic: str, partition: int, offset: int) -> str:
    """Derive a stable DLQ identity from the immutable Kafka record position."""

    return str(uuid5(NAMESPACE_URL, f"sentinelstream:dead-letter:{topic}:{partition}:{offset}"))


@dataclass(frozen=True, slots=True)
class ValidatedStreams:
    """Valid and malformed branches of one Kafka source stream."""

    valid: DataFrame
    malformed: DataFrame


@dataclass(slots=True)
class SparkKafkaTransactionReader:
    """Read a SentinelStream Kafka topic and enforce its Pydantic contract."""

    bootstrap_servers: str
    source_topic: TopicName | str = TopicName.VALIDATED_TRANSACTIONS
    starting_offsets: Literal["earliest", "latest"] = "latest"
    max_offsets_per_trigger: int | None = None
    message_kind: Literal["transaction", "validated"] = "validated"

    def read(self, spark: SparkSession) -> DataFrame:
        reader = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", normalise_topic(self.source_topic).value)
            .option("startingOffsets", self.starting_offsets)
            .option("failOnDataLoss", "true")
        )
        if self.max_offsets_per_trigger is not None:
            reader = reader.option("maxOffsetsPerTrigger", self.max_offsets_per_trigger)
        return reader.load()

    def parse_and_validate(self, source: DataFrame) -> ValidatedStreams:
        """Return flat event columns and a DLQ-ready malformed branch."""

        validation_udf = udf(
            lambda payload: _validate_payload(payload, self.message_kind),
            StringType(),
            useArrow=False,
        )
        payload = col("value").cast("string")
        parsed = (
            source.withColumn("payload", payload)
            .withColumn("validation_error", validation_udf(payload))
            .withColumn("message", from_json(payload, TRANSACTION_MESSAGE_SCHEMA))
        )
        valid = parsed.filter(col("validation_error").isNull())
        event = col("message.event")
        event_json = to_json(event)
        valid_flat = valid.select(
            col("message.message_id").alias("message_id"),
            col("message.schema_version").alias("schema_version"),
            col("message.produced_at").alias("produced_at"),
            col("message.correlation_id").alias("correlation_id"),
            event_json.alias("event_json"),
            event.getField("transaction_id").alias("transaction_id"),
            event.getField("event_id").alias("event_id"),
            event.getField("customer_id").alias("customer_id"),
            event.getField("account_id").alias("account_id"),
            event.getField("card_id").alias("card_id"),
            event.getField("merchant_id").alias("merchant_id"),
            event.getField("merchant_category").alias("merchant_category"),
            event.getField("transaction_amount").alias("transaction_amount"),
            event.getField("currency").alias("currency"),
            event.getField("transaction_type").alias("transaction_type"),
            event.getField("channel").alias("channel"),
            event.getField("timestamp").alias("event_timestamp"),
            to_timestamp(event.getField("timestamp")).alias("event_time"),
            event.getField("country").alias("country"),
            event.getField("city").alias("city"),
            event.getField("latitude").alias("latitude"),
            event.getField("longitude").alias("longitude"),
            event.getField("device_id").alias("device_id"),
            event.getField("ip_address").alias("ip_address"),
            event.getField("card_present").alias("card_present"),
            event.getField("authentication_method").alias("authentication_method"),
            event.getField("transaction_status").alias("transaction_status"),
            event.getField("event_version").alias("event_version"),
            event.getField("ingestion_timestamp").alias("ingestion_timestamp"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition").cast("int").alias("kafka_partition"),
            col("offset").cast("long").alias("kafka_offset"),
            col("key").cast("string").alias("kafka_key"),
        )
        malformed = parsed.filter(col("validation_error").isNotNull())
        malformed_records = malformed.select(
            coalesce(col("key").cast("string"), lit("malformed")).alias("key"),
            to_json(
                struct(
                    udf(_dead_letter_message_id, StringType(), useArrow=False)(
                        col("topic"), col("partition"), col("offset")
                    ).alias("message_id"),
                    lit(STREAM_SCHEMA_VERSION).alias("schema_version"),
                    current_timestamp().alias("produced_at"),
                    lit(None).cast("string").alias("correlation_id"),
                    col("topic").alias("original_topic"),
                    col("partition").cast("int").alias("original_partition"),
                    col("offset").cast("long").alias("original_offset"),
                    col("key").cast("string").alias("original_key"),
                    substring(col("payload"), 1, 1_000_000).alias("raw_payload"),
                    lit("spark_schema_error").alias("error_type"),
                    col("validation_error").alias("error_message"),
                    lit(1).alias("attempts"),
                    current_timestamp().alias("failed_at"),
                )
            ).alias("value"),
        )
        return ValidatedStreams(valid_flat, malformed_records)


def kafka_records(frame: DataFrame, *, topic: TopicName | str) -> DataFrame:
    """Convert a dataframe with string key/value columns into a Kafka sink frame."""

    return frame.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
    ).withColumn("topic", lit(normalise_topic(topic).value))


def validated_kafka_records(frame: DataFrame, *, topic: TopicName | str) -> DataFrame:
    """Serialise validated rows using the Phase 6 validated-message contract."""

    event_fields = struct(
        col("transaction_id"),
        col("event_id"),
        col("customer_id"),
        col("account_id"),
        col("card_id"),
        col("merchant_id"),
        col("merchant_category"),
        col("transaction_amount"),
        col("currency"),
        col("transaction_type"),
        col("channel"),
        col("event_timestamp").alias("timestamp"),
        col("country"),
        col("city"),
        col("latitude"),
        col("longitude"),
        col("device_id"),
        col("ip_address"),
        col("card_present"),
        col("authentication_method"),
        col("transaction_status"),
        col("event_version"),
        col("ingestion_timestamp"),
    ).alias("event")
    message = struct(
        col("message_id"),
        col("schema_version"),
        col("produced_at"),
        col("correlation_id"),
        lit("phase2-validation-v1").alias("validation_version"),
        event_fields,
    )
    return frame.select(
        col("customer_id").alias("key"),
        to_json(message).alias("value"),
    ).withColumn("topic", lit(normalise_topic(topic).value))
