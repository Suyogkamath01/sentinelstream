# Testing guide

The default suite is local and does not require Kafka, PostgreSQL, Redis,
MLflow, or a running Docker daemon. Tests that use installed Spark or a
temporary SQLite database remain deterministic and bounded.

    uv run pytest
    uv run pytest -m security
    uv run pytest -m performance
    uv run pytest -m integration
    uv run pytest --cov=sentinelstream --cov-report=term-missing

Markers available in pyproject.toml include unit, integration, api, kafka,
spark, database, mlops, monitoring, security, performance, and slow. External
service suites should be run only after those services are started.

Security checks are configured as an explicit opt-in group:

    uv sync --group security
    uv run bandit -r src -q
    uv run pip-audit

The commands report the state of the installed environment; no vulnerability
or coverage result is implied by this document.
