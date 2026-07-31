# Phases 8–10 — Hybrid engine, persistence, and API

## Phase 8

`sentinelstream.models.hybrid` fuses supervised, anomaly, rules, customer,
merchant, and device evidence using configurable non-negative weights. Missing
sources are renormalised instead of treated as zero. The engine emits a fused
score, confidence, risk tier, policy action, reason codes, alert priority, and
source-level evidence. `AblationExperiment` runs source-removal experiments
against supplied inputs and reports only measured deltas. `validate_calibration`
wraps the existing held-out calibration report with explicit acceptance limits;
it does not generate performance claims.

## Phase 9

SQLAlchemy models and repositories cover transactions, predictions, alerts,
feedback, customer/merchant/device profiles, and audit records. Indexed search,
pagination, unique identifiers, score constraints, and prediction-alert
relationships are included. Alembic uses `migrations/versions/0001_initial.py`
for schema creation. PostgreSQL is supported through `postgresql+psycopg` URLs;
SQLite is used for local development and tests.

`RedisCache` caches profiles, predictions, sessions, and arbitrary JSON values.
Redis outages or disabled Redis fall back to a bounded in-process TTL cache and
emit structured warnings. Cache invalidation methods are explicit so profile
updates can invalidate stale risk data.

## Phase 10

`sentinelstream.api.create_app` exposes OpenAPI-documented endpoints for
predictions, explanations, alerts, feedback, transaction history, health,
readiness, liveness, and metrics. All business routes use bearer JWTs and role
checks. Admin and analyst roles can mutate alert/feedback state; operators can
submit predictions; readonly users can inspect data. Authentication is injected
through an application-level authenticator, so deployment-specific user stores
are not embedded in the service.

For local setup:

```bash
uv sync --group dev --group database --group api --no-group ml
alembic upgrade head
```

Set `SENTINELSTREAM_SECRET_KEY` in production. Configure the database and Redis
URLs through the settings documented in `.env.example`. The API does not
automatically create production schemas when `initialize_database=False`; use
Alembic in deployment pipelines.
