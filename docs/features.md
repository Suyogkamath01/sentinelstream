# Feature documentation

## Raw event fields

`TransactionEvent` contains typed identifiers, amount, currency, merchant
category, transaction/channel attributes, event and ingestion timestamps,
country/city/coordinates, device/IP references, authentication, and status.
Serving contracts do not include simulation-only labels.

## Engineered and behavioural fields

The batch path in `features/batch.py` calls the same behavioural implementation
used by streaming. It produces transaction count, rolling amount statistics,
velocity, unique merchant/device/country counts, time since a previous
transaction, spending deviation, merchant popularity, device novelty, and
country novelty. Event time is sorted before state updates; streaming adds
watermarks, deduplication, and bounded state cleanup.

## Risk and anomaly inputs

Customer, merchant, and device profiles supply bounded risk and confidence
values. The anomaly detector contributes a normalised anomaly score. The rules
path uses transparent signals such as amount deviation, rapid repeats,
novelty, cash withdrawal, and high amount. The hybrid engine renormalises
configured weights when a source is unavailable.

## Missing values and consistency

Validation rejects missing contract fields. Feature encoders fit category
vocabularies on the training partition and align later partitions to that
vocabulary. Streaming and batch feature parity tests protect the shared
behaviour definitions; online state still depends on watermark and replay
configuration.

## Leakage and versioning risks

Features must use only information available at event time. Profile updates,
labels, analyst feedback, and future transactions must not leak into historical
training rows. Store a feature-set version with model and dataset manifests.
Any change to a behavioural window, category encoding, model input order, or
profile calculation requires compatibility and regression checks.
