# Phase 7 — Spark Structured Streaming

Phase 7 adds the Spark Structured Streaming execution layer on top of the
Phase 6 Kafka contracts. The implementation is in `sentinelstream.spark` and
does not replace the existing Kafka producer, consumer, or decision modules.

## Topology

`SparkStructuredStreamingPipeline` creates one Kafka source and validates each
value against the Phase 6 Pydantic envelope. The valid branch is used by the
stateful feature and prediction stages; malformed values become bounded
`DeadLetterMessage` records. When the input contract is the unvalidated
transaction envelope, the valid branch is also written to the validated topic.

The prediction branch publishes:

- `sentinelstream.predictions.v1` — `PredictionMessage` records;
- `sentinelstream.alerts.v1` — `AlertMessage` records for review and block actions;
- `sentinelstream.audit.v1` — prediction and alert lifecycle records; and
- `sentinelstream.dead-letter.v1` — bounded poison-message records.

When `spark.model_path` is unset, the Spark branch is explicitly rule-only:
`model_probability` remains null and the configured rule score is normalized as
the available risk signal. This fallback is useful for local plumbing checks;
it is not a trained-model result.

The feedback topic remains analyst-owned and is consumed by the existing Phase
6 online pipeline. Topic names and partition-key conventions remain defined by
`sentinelstream.streaming.topics`.

## Event time and state

The valid stream uses the transaction timestamp as `event_time`, applies the
configured watermark, and groups state by customer. State is stored as bounded
JSON history with event-time timeouts. The state function sorts each micro-batch
by event time before calculating features, so an event that arrives out of order
still receives features from strictly earlier business events. Duplicate event
or transaction IDs are classified explicitly, and records beyond the watermark
are emitted as dropped rows for metrics.

The stateful feature set includes the Phase 3 behavioural features plus rolling
amount/count statistics, customer and merchant velocity, unique merchant/device/
country counts, spending deviation, merchant popularity, device novelty, country
novelty, and time since the previous transaction. The bounded helper
`process_event_sequence` is kept next to the Spark state function for parity
tests and deterministic local verification.

Two event-time aggregate views are also available: a ten-minute sliding customer
velocity view over a one-hour horizon and a one-hour tumbling merchant activity
view. They are persisted under the configured streaming Parquet directory by a
separate checkpointed query.

## Checkpoints and recovery

The pipeline uses separate checkpoints for dead letters, validated events,
predictions, and window metrics under `spark.checkpoint_dir`. Spark therefore
recovers Kafka offsets and state store contents independently after a restart.
The Spark Kafka writer enables `acks=all`, idempotence, bounded retries, and a
maximum in-flight request count of five. Prediction and alert IDs are derived
deterministically from the event and model version, which gives downstream
consumers a stable deduplication key if a `foreachBatch` retry republishes a
record.

Replay is controlled by `spark.starting_offsets`. Use `earliest` with a new
checkpoint directory for a full replay; reuse an existing checkpoint to resume
from committed offsets and state.

## Storage and configuration

Every completed prediction batch is appended to:

- `spark.parquet_output_dir/predictions` as Parquet; and
- `spark.postgres_jdbc_url`/`spark.postgres_table` when a JDBC URL is configured.

PostgreSQL JDBC driver loading is deployment-specific and is intentionally not
bundled with the Python package. Provide the PostgreSQL driver on Spark's
classpath when enabling the JDBC sink.

The Spark dependency is optional:

```bash
uv sync --group dev --group spark --no-group ml
```

Spark's Kafka source requires the configured
`spark.kafka_connector_package` (defaulting to the matching Spark 4.2.0 Scala
2.13 connector used by the Compose Spark image). The PostgreSQL JDBC sink uses
the separately configured `spark.jdbc_driver_package`. Set either value to an
operator-managed package or `null` when the required jars are already on the
classpath. A supported Java runtime is also required; verify the local
Spark/Java combination before deployment.

The pipeline can be started from Python:

```python
from sentinelstream.config import load_settings
from sentinelstream.spark import SparkStructuredStreamingPipeline

settings = load_settings(environment="development")
queries = SparkStructuredStreamingPipeline(settings).start()
try:
    queries.await_termination()
finally:
    queries.stop()
```

Useful settings include `SENTINELSTREAM_SPARK__BATCH_INTERVAL_SECONDS`,
`SENTINELSTREAM_SPARK__WATERMARK_DURATION`,
`SENTINELSTREAM_SPARK__STATE_TIMEOUT_DURATION`,
`SENTINELSTREAM_SPARK__STARTING_OFFSETS`, and
`SENTINELSTREAM_SPARK__MODEL_PATH`.

## Metrics and failure handling

`SparkStreamingMetrics` records processed, dropped, duplicate, malformed, and
late events, batch count, total/max batch duration, event processing latency,
and the last batch ID. Each
completed micro-batch emits a structured log event named
`spark_stream_batch_completed`; startup emits `spark_stream_started`. A failed
micro-batch is left uncommitted by Spark and is retried from its checkpoint.

The pipeline is intentionally at-least-once across the independent prediction,
alert, audit, and storage writes inside `foreachBatch`. Deterministic message
IDs, Kafka idempotence, and Phase 6 consumer deduplication are the consistency
mechanisms for retries; exactly-once coordination across Kafka and PostgreSQL
requires a deployment-specific transactional sink.

## Tests

`tests/integration/test_spark_streaming.py` covers contract parsing and poison
messages, duplicate and late-event behavior, bounded feature parity, sliding and
tumbling windows, stateful checkpoint restoration, and prediction/alert/audit
contract serialization. The tests use a local Spark session and do not require a
Kafka broker; the Kafka connector load path is separately smoke-tested during
validation.
