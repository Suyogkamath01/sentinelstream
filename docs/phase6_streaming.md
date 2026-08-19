# Phase 6 — Kafka streaming pipeline

Phase 6 adds a typed Kafka boundary around the Phase 2–5 contracts. The
streaming package is deliberately transport-focused: it does not replace the
existing feature calculator, model interfaces, decision policy, or
explainability APIs.

The production adapter uses `confluent-kafka` and librdkafka for idempotent
delivery; tests use the transport-neutral in-memory implementation.

## Topics and partitioning

The default topic catalog contains six partitions per topic and a replication
factor of one for local development. Production deployments should override
the replication factor and provision topics through the Kafka admin API.

| Topic | Payload | Partition key |
| --- | --- | --- |
| `sentinelstream.transactions.v1` | `TransactionMessage` | `customer_id` |
| `sentinelstream.validated-transactions.v1` | `ValidatedTransactionMessage` | `customer_id` |
| `sentinelstream.predictions.v1` | `PredictionMessage` | `customer_id` |
| `sentinelstream.alerts.v1` | `AlertMessage` | `customer_id` |
| `sentinelstream.feedback.v1` | `FeedbackMessage` | `transaction_id` |
| `sentinelstream.audit.v1` | `AuditMessage` | `event_id` or message ID |
| `sentinelstream.dead-letter.v1` | `DeadLetterMessage` | original key or message ID |

Customer-keyed topics preserve the ordering needed by the online behavioural
state. Partition selection uses a stable CRC32 key hash in the local transport;
Kafka applies its own consistent key partitioner in production.

## Processing semantics

`KafkaMessageProducer` validates typed messages, serialises them as compact
JSON, uses `acks=all` and idempotent production, and applies bounded
application retries around broker acknowledgements. Kafka's own retry setting
is also configured from the same retry policy.

`KafkaMessageConsumer` disables auto-commit. It commits an offset only after a
handler completes, or after a poison/failed message has been durably published
to the dead-letter topic. Schema failures are poison messages and go directly
to the DLQ. Handler failures receive exponential backoff retries before DLQ
publication. The raw payload is retained in the DLQ with bounded size; payload
contents are not written to structured logs.

Replay is performed with a fresh consumer group and `auto_offset_reset=earliest`
so the retained history can be reprocessed without moving the live group's
offsets. Replay is at-least-once; downstream handlers should remain idempotent
using message, event, or transaction identifiers.

## Pipeline integration

`StreamingPipeline` publishes a validated transaction, calls the existing
`StreamingFeatureCalculator`, accepts an injected predictor that returns the
existing `PredictionSignals`, applies the existing `ThresholdPolicy`, and
publishes predictions, review/block alerts, and audit records. Analyst feedback
is accepted as a typed message and currently audited for later model-monitoring
work; feedback-driven retraining is outside Phase 6.

`RuleBasedPredictor` is a transparent local fallback for demonstrations when a
trained model artifact is not loaded. Production serving should inject a
versioned model/calibrator predictor.

## Running against Kafka

Install dependencies and configure the broker through YAML or environment
variables:

```bash
make install
export SENTINELSTREAM_KAFKA__BOOTSTRAP_SERVERS=localhost:9092
```

Provision the catalog with `KafkaTopicManager.ensure_topics()` before starting
consumers. Production sets `auto_create_topics: false`; local development may
leave it enabled for convenience. The repository integration tests use the
in-memory broker and therefore do not claim a live Kafka broker or cluster
benchmark.
