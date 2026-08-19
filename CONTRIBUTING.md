# Contributing to SentinelStream

SentinelStream is a portfolio-scale platform with a preference for small,
typed, backwards-compatible changes. Read the relevant architecture and
security documentation before changing public contracts.

## Development setup

```bash
uv python install 3.12
uv venv --python 3.12
make install
cp .env.example .env
```

Generated data, model artefacts, MLflow stores, checkpoints, credentials, and
benchmark/research outputs belong outside version control unless intentionally
selected as a small fixture.

## Workflow and code style

1. Create a focused branch from `main`.
2. Preserve public interfaces and configuration naming where possible.
3. Use type hints and the existing Ruff/mypy configuration.
4. Keep comments concise and explain non-obvious decisions, not syntax.
5. Keep simulation labels separate from serving contracts.
6. Document event-time, leakage, privacy, or distributed-state assumptions.
7. Add or update meaningful tests for changed behaviour.

## Validation

```bash
make check
make docs-validate
make security
```

Use focused markers while iterating:

```bash
uv run pytest -m unit
uv run pytest -m api
uv run pytest -m security
uv run pytest -m research
uv run pytest -m integration
```

External-service tests must state which services were started. Do not report
coverage, benchmark, model, drift, or security results that were not actually
measured.

## Commits and pull requests

Use a short imperative commit subject and keep unrelated changes separate. A
pull request should explain the problem, design impact, configuration changes,
security/PII implications, tests run, and any unavailable external validation.
Update documentation, `.env.example`, migrations, dashboards, or changelog
entries when the change affects them. Include screenshots only when they were
actually captured from a running service.

## Security reports

Do not publish exploit details, secrets, or personal data in an issue. Follow
[`SECURITY.md`](SECURITY.md) and use a private maintainer channel configured by
the repository owner. If no private channel exists, report only a high-level
description through the repository owner’s preferred private contact and wait
for acknowledgement before disclosing technical details.
