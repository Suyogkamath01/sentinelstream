# Dataset card

## Source and variants

The repository includes a deterministic synthetic transaction generator for
development and demonstrations. It can add fraud scenarios, duplicates, late
events, malformed records, and out-of-order delivery. External or production
datasets are not included by default and must be documented by their owner.

## Schema and labels

Serving fields follow `TransactionEvent`: identifiers, amount/currency,
merchant category, timestamp, location, device, network, authentication, and
status. Generated files may include `simulation_fraud_label` and
`simulation_fraud_type`. JSONL keeps labels under `ground_truth`; validation
normalises them to `fraud_label` only for offline training/evaluation.

## Validation and splits

`ingest_path` validates typed events, quarantines malformed records, removes
duplicate event/transaction identifiers, and writes a quality report.
`temporal_split` creates non-overlapping chronological train, validation, and
test partitions. Dataset manifests store row count, columns, dtypes, checksum,
source, feature-set version, generation timestamp, and split metadata without
copying the dataset.

## Imbalance, privacy, and bias

Fraud prevalence is configurable in generated data and should be measured from
each real dataset version. Amount, country, device, merchant, and customer
fields can encode sensitive or proxy information. Do not move raw identifiers
into reports or logs; use role-restricted access, masking, retention controls,
and a documented lawful basis for any real data.

## Limitations and permitted use

Synthetic records are suitable for software development, deterministic tests,
and demonstration—not for claims about real-world fraud performance. Dataset
licence, provenance, consent, and permitted use for external data must be
recorded before training. No external dataset is shipped with this repository.
