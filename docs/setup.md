# Setup guide

## Requirements

- Python 3.12–3.14
- [uv](https://docs.astral.sh/uv/)
- Docker Engine with Compose v2 for the full stack
- At least 8 GB memory for the Spark/Kafka demonstration profile

Install local dependencies:

```bash
uv python install 3.12
uv venv --python 3.12
make install
```

The `ml` group is optional for the minimal API/test path; `make install`
already includes the research group for local plots. The separate `ml` group
contains optional SHAP/XGBoost dependencies and is installed only when the
current Python/platform has compatible wheels.

## Configuration

```bash
cp .env.example .env
```

Change `SENTINELSTREAM_SECRET_KEY`, database credentials, Grafana credentials,
and dashboard credentials before exposing any port. Environment variables use
the `SENTINELSTREAM_<SECTION>__<FIELD>` convention. The complete documented
set is in `.env.example` and typed settings are in
`src/sentinelstream/config/settings.py`.

## Local application setup

```bash
make install
alembic upgrade head
make generate-sample
uv run python scripts/train_model.py --input data/samples/transactions.jsonl
```

Run services separately when Docker is not being used:

```bash
uv run uvicorn sentinelstream.api.main:app --host 127.0.0.1 --port 8000
uv run streamlit run src/sentinelstream/dashboard/app.py
```

The API defaults to SQLite only when no database URL is configured. PostgreSQL,
Kafka, Redis, Spark, and MLflow must be running and configured for their live
integration paths.

## Full stack

```bash
cp .env.example .env
docker compose up --build
```

Migrations run from the API entrypoint and topics are created by `kafka-init`.
Use `docker compose ps` to confirm health. See [`docs/deployment.md`](deployment.md)
for volumes, shutdown, reset, and troubleshooting.

## Model availability

Serving accepts a configured model artifact through the existing model-loading
settings. A model is not invented when an artifact is missing: the API reports
the configuration or readiness problem. Record model, feature, calibration,
and dataset versions with the MLOps helpers and [`docs/model_card.md`](model_card.md).
