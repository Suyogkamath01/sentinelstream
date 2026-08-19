# Phase 2 — ingestion and temporal baselines

Phase 2 validates the simulator output before it reaches training. Invalid
records are quarantined with a reason code; duplicate event or transaction IDs
are rejected after the first accepted delivery. Labels are required for model
training but remain outside the serving event contract.

## Leakage boundary

The data is ordered by event timestamp before splitting. Batch features are
calculated on the complete ordered frame, but every historical value uses a
shifted group calculation, so a transaction cannot use its own or a future
transaction's amount, device, country, or merchant history. Categorical
vocabularies are fit from the training partition and future partitions are
aligned to those columns.

## Baselines

The training CLI compares:

- prior-probability dummy classifier;
- deterministic rules;
- class-weighted logistic regression;
- balanced random forest;
- XGBoost with class weighting;
- Isolation Forest anomaly scores.

Metrics include PR-AUC, average precision, precision, recall, F1, F-beta,
recall at fixed precision, precision/recall at top-k, Matthews correlation,
Brier score, detected and missed fraud amount, and expected financial cost.
Accuracy is intentionally not used as a selection metric.

## Commands

```bash
uv run python scripts/train_model.py \
  --input data/samples/transactions.jsonl \
  --models dummy rules logistic_regression random_forest xgboost isolation_forest
```

The command writes a quarantine file, a data-quality report, JSON metrics, and
MLflow-compatible run records. MLflow tracking is optional for local tests;
the JSON tracker remains available when its backend is unavailable.
