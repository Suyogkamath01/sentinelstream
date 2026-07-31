"""Kafka, Parquet, and optional JDBC writers for Spark micro-batches."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit


def _has_rows(frame: DataFrame) -> bool:
    return bool(frame.take(1))


def write_kafka_batch(
    frame: DataFrame,
    *,
    bootstrap_servers: str,
    topic: str,
    key_column: str = "key",
    value_column: str = "value",
) -> None:
    """Write a non-empty micro-batch with stable Kafka producer settings."""

    if not _has_rows(frame):
        return
    (
        frame.select(
            col(key_column).cast("string").alias("key"),
            col(value_column).cast("string").alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .option("kafka.acks", "all")
        .option("kafka.enable.idempotence", "true")
        .option("kafka.max.in.flight.requests.per.connection", "5")
        .option("kafka.retries", "10")
        .mode("append")
        .save()
    )


@dataclass(frozen=True, slots=True)
class SparkBatchStorage:
    """Persist scored micro-batches using the repository's supported stores."""

    parquet_output_dir: Path
    postgres_jdbc_url: str | None = None
    postgres_table: str = "sentinelstream_stream_events"
    postgres_user: str | None = None
    postgres_password: str | None = None
    postgres_driver: str = "org.postgresql.Driver"
    postgres_properties: Mapping[str, str] = field(default_factory=dict)

    def write(self, frame: DataFrame, *, batch_id: int) -> None:
        if not _has_rows(frame):
            return
        parquet_path = self.parquet_output_dir / "predictions"
        parquet_path.mkdir(parents=True, exist_ok=True)
        (
            frame.withColumn("_spark_batch_id", lit(batch_id))
            .write.mode("append")
            .parquet(str(parquet_path))
        )
        if self.postgres_jdbc_url:
            options = {
                "url": self.postgres_jdbc_url,
                "dbtable": self.postgres_table,
                "driver": self.postgres_driver,
                **self.postgres_properties,
            }
            if self.postgres_user:
                options["user"] = self.postgres_user
            if self.postgres_password:
                options["password"] = self.postgres_password
            frame.write.format("jdbc").options(**options).mode("append").save()
