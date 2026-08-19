# Docker deployment

The complete local stack is defined in docker-compose.yml. It uses
health-gated dependencies, persistent named volumes, and an internal
sentinelstream network.

## Start and stop

    cp .env.example .env
    docker compose up --build
    docker compose up --build -d
    docker compose logs -f api
    docker compose ps
    docker compose stop
    docker compose down
    docker compose down -v  # intentionally removes local data volumes

The API entrypoint runs alembic upgrade head before Uvicorn. Kafka topics are
created by kafka-init. Existing PostgreSQL, Redis, Kafka, MLflow, Prometheus,
and Grafana volumes are preserved by docker compose down.

## Local interfaces

| Service | URL |
| --- | --- |
| Streamlit | http://127.0.0.1:8501 |
| FastAPI | http://127.0.0.1:8000 |
| FastAPI OpenAPI | http://127.0.0.1:8000/docs |
| Kafka UI | http://127.0.0.1:8080 |
| MLflow | http://127.0.0.1:5000 |
| Prometheus | http://127.0.0.1:9090 |
| Grafana | http://127.0.0.1:3000 |
| Spark master UI | http://127.0.0.1:8081 |

Development credentials are sourced from .env or Compose defaults. Change
them before exposing any port outside a local machine. The API demo user is
configured by SENTINELSTREAM_DASHBOARD__AUTH_USERNAME and
SENTINELSTREAM_DASHBOARD__AUTH_PASSWORD; Grafana uses
GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD.

## Demo flow

After the stack is healthy, use the existing generator and Kafka producer
commands to send actual transactions. Predictions, alerts, and metrics are
then persisted or exposed by their existing service layers. No dashboard
fixture is injected by Compose.

Docker validation is environment-dependent. Use docker compose config for
static validation, then build and start the stack to validate image health,
migrations, topic creation, readiness, Prometheus scraping, and Grafana
provisioning.
