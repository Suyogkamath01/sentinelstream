"""Spark session construction kept separate from stream topology code."""

from __future__ import annotations

from pyspark.sql import SparkSession

from sentinelstream.config.settings import SparkSettings


def create_spark_session(settings: SparkSettings) -> SparkSession:
    """Create a UTC Spark session with the configured Kafka connector."""

    builder = SparkSession.builder.appName(settings.app_name)
    if settings.master.strip():
        builder = builder.master(settings.master)
    builder = (
        builder.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "false")
    )
    packages = tuple(
        package
        for package in (settings.kafka_connector_package, settings.jdbc_driver_package)
        if package
    )
    if packages:
        builder = builder.config(
            "spark.jars.packages",
            ",".join(packages),
        )
    return builder.getOrCreate()
