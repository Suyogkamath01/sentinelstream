# Final demonstration guide

This guide describes the observable end-to-end workflow. Commands that need
Docker, Kafka, Spark, PostgreSQL, Redis, or MLflow must be run where those
services are available; Compose files alone are not evidence that services
have been started.

## Start

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
docker compose logs -f api spark-streaming
```

Open Streamlit at <http://127.0.0.1:8501>, FastAPI/OpenAPI at
<http://127.0.0.1:8000/docs>, Kafka UI at <http://127.0.0.1:8080>, MLflow at
<http://127.0.0.1:5000>, Prometheus at <http://127.0.0.1:9090>, and Grafana at
<http://127.0.0.1:3000>. Local credentials come from `.env`.

## Initialise and generate data

The Compose API entrypoint applies Alembic migrations and `kafka-init` creates
the declared topics. Generate a deterministic sample file locally:

```bash
make generate-sample
uv run python scripts/train_model.py --input data/samples/transactions.jsonl
```

Generated labels are simulation metadata for offline evaluation. Serving
events must publish the typed `TransactionMessage` contract without labels. The
repository includes a bounded producer for this purpose:

```bash
uv run python scripts/publish_sample_transactions.py --count 20 --seed 42
```

## Observe actual behaviour

1. Confirm migrations, topic creation, `/ready`, `/live`, and Prometheus targets.
2. Run `kafka-validator` and the checkpointed Spark stream configured for the validated topic.
3. Publish valid typed transactions with `publish_sample_transactions.py`.
4. Inspect validated, prediction, alert, audit, and dead-letter topics in Kafka UI.
5. Confirm stream output in Parquet or the configured Spark JDBC sink. The current
   deployment does not project Kafka/Spark output into the ORM transaction and
   alert tables automatically; API/Streamlit persistence views are populated by
   the direct prediction service until that projection is added.
6. Acknowledge or resolve an alert and submit analyst feedback through the API or dashboard.
7. Inspect model metadata in MLflow and operational panels in Grafana.
8. Generate a reference/current drift report with the monitoring utilities.

The expected outcome is actual records and metrics from running services, not
prefilled dashboard fixtures. An offline smoke path is:

```bash
uv run python scripts/run_demo.py --count 100
```

## Stop and clean up

```bash
docker compose down
# Intentional local reset; removes application data volumes:
docker compose down -v
```

## Troubleshooting

Check `docker compose ps`, service-specific logs, `/ready`, and Prometheus
targets first. A missing model, connector, broker, or authentication provider
should be reported as degraded or unavailable rather than replaced with
invented values. If the full stack does not fit on the host, run the generator,
research runner, and local API tests without Docker.
