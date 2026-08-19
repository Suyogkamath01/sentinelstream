# Phase 1 data dictionary

`event` is the model-serving transaction contract. `ground_truth` is synthetic
evaluation metadata and must not be passed to a production prediction path.

| Field | Meaning |
| --- | --- |
| `transaction_id` | Stable identifier for the business transaction. |
| `event_id` | Identifier for this event delivery. Duplicate deliveries retain the same event ID. |
| `customer_id` / `account_id` / `card_id` | Pseudonymous simulation entities. |
| `merchant_id` / `merchant_category` | Merchant identity and category. |
| `transaction_amount` / `currency` | Positive transaction amount and ISO-style currency code. |
| `timestamp` | Event time in UTC. |
| `ingestion_timestamp` | Time at which the simulator makes the event available, in UTC. |
| `ground_truth.fraud_label` | Simulation-only fraud label. |
| `ground_truth.fraud_type` | Simulation-only scenario name. |
