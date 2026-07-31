# Phase 3 — streaming-compatible behavioural features

Phase 3 establishes one feature contract for offline and online use. The batch
builder and the in-memory streaming calculator both call
`calculate_behavioural_features`; the latter adds event-time state management,
deduplication, watermarks, and late-event outcomes.

## Feature contract

Features include calendar signals, prior customer count and amount statistics,
10-minute/1-hour/24-hour rolling windows, exponentially weighted amount,
merchant/device/country novelty, failed and declined transaction rates,
card-present ratio, geographic distance and velocity, and an impossible-travel
signal. Every historical calculation excludes the current event and all events
with a later event timestamp.

## Event-time policy

`StreamingFeatureCalculator` tracks the maximum observed event timestamp and
sets `watermark = max_event_time - allowed_lateness`. Duplicate event IDs and
transaction IDs are rejected. Events older than the watermark are retained in
an explicit late-event collection and do not mutate feature state. Events that
arrive out of order within the lateness bound are accepted and are evaluated
against event-time history.

The Phase 3 state implementation is intentionally in-memory and dependency
light. Kafka and Spark Structured Streaming will use this contract in later
phases; this phase does not claim a distributed checkpoint or restart guarantee.
