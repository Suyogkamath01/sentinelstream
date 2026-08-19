# ruff: noqa: E402

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sentinelstream.config.settings import AppSettings, SparkSettings
from sentinelstream.data.schemas import TransactionEvent
from sentinelstream.spark.ingestion import SparkKafkaTransactionReader
from sentinelstream.spark.pipeline import SparkStructuredStreamingPipeline
from sentinelstream.spark.prediction import SparkPredictionEngine
from sentinelstream.spark.state import (
    FEATURE_COLUMNS,
    StatefulFeatureConfig,
    build_feature_batch,
    build_stateful_feature_stream,
    duration_to_timedelta,
    process_event_sequence,
)
from sentinelstream.spark.windows import build_window_metrics_stream
from sentinelstream.streaming.contracts import (
    AlertMessage,
    AuditMessage,
    DeadLetterMessage,
    PredictionMessage,
    ValidatedTransactionMessage,
)
from sentinelstream.streaming.topics import TopicName


def _event(index: int, timestamp: datetime, *, amount: float = 25.0) -> TransactionEvent:
    return TransactionEvent(
        transaction_id=uuid5(NAMESPACE_URL, f"sentinelstream:transaction:{index}"),
        event_id=uuid5(NAMESPACE_URL, f"sentinelstream:event:{index}"),
        customer_id="customer-001",
        account_id="account-001",
        card_id="card-001",
        merchant_id=f"merchant-{index % 3:03d}",
        merchant_category="grocery",
        transaction_amount=amount,
        currency="USD",
        transaction_type="purchase",
        channel="pos",
        timestamp=timestamp,
        country="US",
        city="New York",
        latitude=40.7128,
        longitude=-74.0060,
        device_id=f"device-{index % 2:03d}",
        ip_address="192.0.2.1",
        card_present=True,
        authentication_method="pin",
        transaction_status="approved",
        event_version=1,
        ingestion_timestamp=timestamp,
    )


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    java25 = Path("/opt/homebrew/opt/openjdk@25")
    if java25.is_dir():
        os.environ.setdefault("JAVA_HOME", str(java25))
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("sentinelstream-phase7-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_duration_parser_supports_configured_watermarks() -> None:
    assert duration_to_timedelta("15 minutes") == timedelta(minutes=15)
    assert duration_to_timedelta("24 hours") == timedelta(hours=24)
    with pytest.raises(ValueError):
        duration_to_timedelta("a while")


def test_kafka_contract_parser_separates_valid_and_poison_events(spark: SparkSession) -> None:
    timestamp = datetime(2025, 1, 1, 12, tzinfo=UTC)
    valid = ValidatedTransactionMessage(event=_event(1, timestamp)).model_dump_json()
    source = spark.createDataFrame(
        [
            (
                b"customer-001",
                valid.encode(),
                TopicName.VALIDATED_TRANSACTIONS.value,
                0,
                1,
                timestamp,
            ),
            (
                b"poison",
                b'{"event":{"missing":true}}',
                TopicName.VALIDATED_TRANSACTIONS.value,
                0,
                2,
                timestamp,
            ),
        ],
        schema=StructType(
            [
                StructField("key", BinaryType(), True),
                StructField("value", BinaryType(), True),
                StructField("topic", StringType(), False),
                StructField("partition", IntegerType(), False),
                StructField("offset", LongType(), False),
                StructField("timestamp", TimestampType(), False),
            ]
        ),
    )

    streams = SparkKafkaTransactionReader(
        bootstrap_servers="unused",
        source_topic=TopicName.VALIDATED_TRANSACTIONS,
        message_kind="validated",
    ).parse_and_validate(source)

    assert streams.valid.count() == 1
    malformed = json.loads(streams.malformed.collect()[0]["value"])
    dead_letter = DeadLetterMessage.model_validate(malformed)
    assert dead_letter.original_offset == 2
    assert dead_letter.error_type == "spark_schema_error"
    repeated = json.loads(streams.malformed.collect()[0]["value"])
    assert repeated["message_id"] == malformed["message_id"]


def test_event_time_sequence_rejects_duplicates_and_beyond_watermark() -> None:
    start = datetime(2025, 1, 1, 12, tzinfo=UTC)
    first = _event(1, start)
    second = _event(2, start + timedelta(minutes=5), amount=40.0)
    out_of_order = _event(3, start + timedelta(minutes=3), amount=30.0)
    future = _event(4, start + timedelta(hours=1), amount=50.0)
    late = _event(5, start + timedelta(minutes=10), amount=35.0)

    rows = process_event_sequence(
        [first, second, second, out_of_order, future, late],
        StatefulFeatureConfig(allowed_lateness=timedelta(minutes=15)),
    )

    assert [row["accepted"] for row in rows] == [True, True, False, True, True, False]
    assert rows[2]["reason"] == "duplicate_event"
    assert rows[5]["reason"] == "event_beyond_watermark"
    assert rows[3]["customer_prior_transaction_count"] == 1.0


def test_merchant_velocity_uses_the_most_recent_merchant_event() -> None:
    start = datetime(2025, 1, 1, 12, tzinfo=UTC)
    first = _event(1, start)
    same_merchant = _event(4, start + timedelta(minutes=30))
    current = _event(7, start + timedelta(minutes=31))

    rows = process_event_sequence([first, same_merchant, current])

    assert rows[-1]["merchant_transaction_velocity_1h"] == pytest.approx(120.0)


def test_bounded_spark_features_match_event_time_feature_contract(spark: SparkSession) -> None:
    start = datetime(2025, 1, 1, 12, tzinfo=UTC)
    events = [_event(1, start), _event(2, start + timedelta(minutes=5), amount=40.0)]
    frame = spark.createDataFrame(
        [
            {
                "event_json": event.model_dump_json(),
                "event_id": str(event.event_id),
                "transaction_id": str(event.transaction_id),
                "customer_id": event.customer_id,
                "event_time": event.timestamp,
            }
            for event in events
        ]
    )

    result = build_feature_batch(frame).orderBy("event_time").collect()

    assert result[0]["customer_prior_transaction_count"] == 0.0
    assert result[1]["customer_prior_transaction_count"] == 1.0
    assert result[1]["rolling_transaction_count_1h"] == 1.0
    assert result[1]["rolling_transaction_amount_1h"] == 25.0
    assert set(FEATURE_COLUMNS).issubset(result[1].asDict())


def test_window_metrics_include_sliding_customer_and_tumbling_merchant_views(
    spark: SparkSession,
) -> None:
    start = datetime(2025, 1, 1, 12, tzinfo=UTC)
    frame = spark.createDataFrame(
        [
            {
                "event_id": str(index),
                "event_time": start + timedelta(minutes=index * 5),
                "customer_id": "customer-001",
                "merchant_id": f"merchant-{index}",
                "device_id": "device-001",
                "country": "US",
                "transaction_amount": float(10 + index),
            }
            for index in range(2)
        ]
    )

    result = build_window_metrics_stream(
        frame,
        watermark_duration="15 minutes",
    ).collect()

    assert {row["window_type"] for row in result} == {
        "sliding_customer",
        "tumbling_merchant",
    }
    assert max(row["transaction_count"] for row in result) == 2


def test_stateful_stream_restores_checkpoint_and_preserves_customer_history(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    checkpoint_dir = tmp_path / "checkpoint"
    input_dir.mkdir()
    start = datetime(2025, 1, 1, 12, tzinfo=UTC)
    first = _event(1, start)
    second = _event(2, start + timedelta(minutes=5), amount=40.0)

    def write_event(event: TransactionEvent, filename: str) -> None:
        (input_dir / filename).write_text(
            json.dumps(
                {
                    "event_json": event.model_dump_json(),
                    "event_id": str(event.event_id),
                    "transaction_id": str(event.transaction_id),
                    "customer_id": event.customer_id,
                    "event_time": event.timestamp.isoformat(),
                }
            ),
            encoding="utf-8",
        )

    input_schema = StructType(
        [
            StructField("event_json", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("event_time", TimestampType(), False),
        ]
    )
    write_event(first, "batch-1.json")
    stream = spark.readStream.schema(input_schema).json(str(input_dir))
    features = build_stateful_feature_stream(
        stream,
        StatefulFeatureConfig(
            watermark_duration="15 minutes",
            allowed_lateness=timedelta(minutes=15),
            history_retention=timedelta(hours=24),
            state_timeout=timedelta(hours=24),
        ),
    )
    query = (
        features.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(output_dir))
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(30)
    assert query.exception() is None
    query.stop()

    write_event(second, "batch-2.json")
    restarted = (
        build_stateful_feature_stream(
            spark.readStream.schema(input_schema).json(str(input_dir)),
            StatefulFeatureConfig(
                watermark_duration="15 minutes",
                allowed_lateness=timedelta(minutes=15),
                history_retention=timedelta(hours=24),
                state_timeout=timedelta(hours=24),
            ),
        )
        .writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(output_dir))
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(availableNow=True)
        .start()
    )
    restarted.awaitTermination(30)
    assert restarted.exception() is None
    restarted.stop()

    output = spark.read.parquet(str(output_dir)).orderBy("event_time").collect()
    assert len(output) == 2
    assert output[-1]["customer_prior_transaction_count"] == 1.0


def test_prediction_alert_and_audit_serialization_uses_phase6_contracts(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    timestamp = datetime(2025, 1, 1, 12, tzinfo=UTC)
    row = process_event_sequence([_event(10, timestamp)])[0]
    row.update(
        {
            "transaction_amount": 3_000.0,
            "amount_z_score": 4.0,
            "customer_seconds_since_previous": 30.0,
            "device_novelty": 1.0,
            "country_novelty": 1.0,
        }
    )
    scored = SparkPredictionEngine(model_version="phase7-test", calibration_version="test").score(
        pd.DataFrame([row])
    )
    scored["correlation_id"] = str(uuid5(NAMESPACE_URL, "sentinelstream:correlation:10"))
    settings = AppSettings(spark=SparkSettings(parquet_output_dir=tmp_path / "streaming"))
    pipeline = SparkStructuredStreamingPipeline(settings=settings, spark=spark)
    scored_frame = spark.createDataFrame(scored)

    prediction = json.loads(pipeline._prediction_messages(scored_frame).collect()[0]["value"])
    alert = json.loads(pipeline._alert_messages(scored_frame).collect()[0]["value"])
    audits = [json.loads(row["value"]) for row in pipeline._audit_messages(scored_frame).collect()]

    prediction_message = PredictionMessage.model_validate(prediction)
    assert isinstance(prediction_message, PredictionMessage)
    assert prediction_message.model_probability is None
    assert isinstance(AlertMessage.model_validate(alert), AlertMessage)
    assert all(isinstance(AuditMessage.model_validate(value), AuditMessage) for value in audits)
