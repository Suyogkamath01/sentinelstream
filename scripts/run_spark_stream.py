#!/usr/bin/env python
"""Start the configured Spark Structured Streaming topology."""

from sentinelstream.config.settings import load_settings
from sentinelstream.spark.pipeline import SparkStructuredStreamingPipeline


def main() -> None:
    settings = load_settings()
    pipeline = SparkStructuredStreamingPipeline(settings)
    pipeline.start().await_termination()


if __name__ == "__main__":
    main()
