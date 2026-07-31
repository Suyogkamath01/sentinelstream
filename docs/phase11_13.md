# Phases 11–13: dashboard, MLOps, and monitoring

## Streamlit dashboard

The dashboard at `src/sentinelstream/dashboard/app.py` is an API client and
presentation layer. It does not calculate fraud decisions or read database
tables directly. It authenticates with the FastAPI bearer-token endpoint and
provides overview, bounded live transactions, alert lifecycle actions,
investigation, customer/merchant history, search, offline evaluation, drift,
health, and feedback pages.

Dashboard configuration is under `SENTINELSTREAM_DASHBOARD__*`. Live polling is
disabled until selected and uses `st.fragment` when available, with the
configured interval and a manual refresh fallback.

## MLOps

`sentinelstream.mlops` contains explicit adapters for MLflow parameters, metrics,
artifacts, signatures, input examples, sklearn model logging, registry
registration, reversible lifecycle aliases, promotion reports, dataset
manifests, sklearn compatibility checks, and bounded seeded Optuna studies.

MLflow tracking and registry URIs are configured by
`SENTINELSTREAM_MLOPS__TRACKING_URI` and `SENTINELSTREAM_MLOPS__REGISTRY_URI`.
The promotion workflow does not promote a model just because training finished.
Configure thresholds only for validation criteria meaningful for a deployment and
retain the previous production alias for rollback.

## Monitoring

The API exposes the backwards-compatible JSON operational endpoint at `/metrics`
and Prometheus exposition at `/prometheus/metrics`. Metrics use bounded labels;
transaction, customer, merchant, and event identifiers are never labels.

Provisioning files are under `monitoring/`: Prometheus scrape and alert rules,
Grafana datasource/dashboard providers, and six dashboards covering system,
API, fraud operations, streaming, model behaviour, and data quality/drift.

`DriftMonitor` writes JSON and human-readable HTML reports to the configured
report directory. It uses Evidently's `DataDriftPreset` when the installed
version supports it and always retains the deterministic project report. Drift
is an investigation signal, not proof of model failure. Dataset metadata links
reference/current windows, feature-set versions, checksums, and model versions.

Dependency health distinguishes healthy, degraded, unavailable, and unknown;
missing URLs for Kafka, Spark, MLflow, Prometheus, and Grafana are reported as
unknown rather than healthy. PostgreSQL, Redis, and configured model artifacts
are checked directly.

## Validation examples

```bash
uv run pytest -q tests/unit/dashboard tests/unit/mlops tests/unit/monitoring
uv run pytest -q tests/integration/test_api.py
uv run ruff check src tests scripts migrations
uv run ruff format --check src tests scripts migrations
uv run mypy src/sentinelstream
for file in monitoring/grafana/dashboards/*.json; do uv run python -m json.tool "$file" >/dev/null; done
```
