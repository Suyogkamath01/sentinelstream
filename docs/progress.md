# Project progress

## Phase 1 — Foundation and transaction simulator

Status: complete on 2026-07-26.

Delivered a typed package, YAML/environment settings, canonical event schema,
reproducible customer and merchant simulation, configurable fraud scenarios,
event anomalies, JSON Lines/Parquet writers, and automated tests. The checks
were run locally with Python 3.14 because Python 3.12 was not installed in the
workspace; the project metadata requires Python 3.12.

Next: ingest generated records into a shared canonical dataset, quarantine
invalid records, and build temporal baseline models without target leakage.

## Phase 2 — Ingestion, validation, and baseline modelling

Status: complete on 2026-07-26.

Added JSONL and Parquet ingestion, canonical validation, duplicate detection,
quarantine files, quality reports, content hashing, temporal train/validation/
test splitting, shifted historical features, fraud-focused metrics, baseline
classifiers, Isolation Forest, XGBoost, and optional MLflow tracking.

Executed checks: 33 tests passed, Ruff passed, mypy passed for 22 source files,
and the complete baseline CLI ran all six requested model families on a
temporary generated dataset. Results are stored only as generated artefacts;
no performance claim is hardcoded into the project documentation.

Next: implement streaming-compatible feature state and verify offline/online
feature parity before introducing Kafka and Spark.

## Phase 3 — Streaming-compatible behavioural features

Status: complete on 2026-07-26.

Added shared temporal and behavioural feature definitions, rolling windows,
novelty and velocity signals, a versioned feature registry, and an in-memory
event-time calculator with deduplication, bounded lateness, watermarks, and
late-event retention. The batch builder now calls the same behavioural feature
function as the streaming calculator.

Executed checks: 37 tests passed, Ruff passed, mypy passed for 26 source files,
and batch/streaming parity tests passed for the shared feature contract.

Next: integrate the contract with typed Kafka topics and Spark Structured
Streaming, including checkpoints and restart recovery.


## Phase 4 — Calibration and cost-aware decisioning

Status: complete on 2026-07-26.

Added Platt and isotonic probability calibration, calibration error reports,
financial cost models, review-capacity-constrained threshold optimization,
segment-aware versioned policy thresholds, confidence-based abstention, and
auditable allow/review/block decisions with disagreement reason codes.

Executed checks: 45 tests passed, Ruff passed, mypy passed for 29 source files,
and the complete formatting check passed.

Next: add explainable hybrid detection, analyst-facing reason codes, and
model/rule/anomaly evidence aggregation.


## Phase 5 — Explainability

Status: complete on 2026-07-26.

Added optional SHAP local/global explanations, deterministic feature-ablation
fallbacks, permutation importance, stable fraud reason codes, human-readable
summaries, threshold-crossing counterfactual prototypes, explanation
stability scoring, versioned JSONL persistence, and explanation configuration.

Executed checks: 51 tests passed, Ruff passed, mypy passed for 30 source files,
and the Phase 5 fallback and persistence tests passed without SHAP installed.

Next: integrate typed Kafka topics and Spark Structured Streaming with
checkpointed event-time processing.

## Phase 6 — Kafka streaming pipeline

Status: complete on 2026-07-26.

Added versioned Kafka topic contracts, deterministic partition-key rules,
idempotent publishing, manual offset management, bounded retries, schema
validation, poison-message dead-letter handling, structured JSON logging,
earliest-offset replay support, analyst feedback and audit messages, and an
online pipeline that reuses the existing streaming features and decision policy.
Integration tests use a partitioned in-memory Kafka-compatible transport so
they are deterministic and do not require a broker process.

Executed checks: 57 tests passed, Ruff passed, formatting passed, and mypy
passed for 40 source files. The default and development environment was
synchronised successfully with `confluent-kafka`; the optional ML dependency
group remains incompatible with the workspace's Python 3.14 because its
resolved SHAP/llvmlite version requires Python below 3.10. A live Kafka
cluster, Spark Structured Streaming, database persistence, and API delivery
remain outside Phase 6.

## Phase 7 — Spark Structured Streaming

Status: complete on 2026-07-26.

Added the optional Spark Structured Streaming layer on top of the Phase 6 Kafka
contracts: Kafka source integration, strict schema validation and poison-message
dead letters, event-time watermarks, checkpointed customer state, duplicate and
late-event handling, streaming behavioural features, sliding/tumbling window
metrics, model/rule/anomaly scoring, decision and alert publishing, audit
publishing, Parquet persistence, optional PostgreSQL JDBC persistence, restart
recovery, and structured micro-batch metrics.

Executed checks: Spark integration tests, the complete test suite, Ruff,
formatting, and mypy were run locally. Spark tests use the pinned PySpark 4.2.0
runtime with the
matching Kafka connector and a supported local Java runtime.

## Phase 8 — Hybrid Fraud Engine

Status: complete on 2026-07-26.

Added configurable weighted fusion across supervised, anomaly, rules,
customer-profile, merchant-profile, and device-profile intelligence. The engine
renormalises missing sources, calculates confidence, applies configurable risk
tiers and thresholds, prioritises alerts, validates held-out calibration, and
supports measured source ablation experiments.

## Phase 9 — Databases

Status: complete on 2026-07-26.

Added SQLAlchemy models, relationships, constraints, indexes, pagination-aware
repositories, Alembic migration bootstrap, and PostgreSQL/SQLite support for
transactions, predictions, alerts, feedback, profiles, and audit records.
Added a Redis cache with TTLs, invalidation, and graceful in-process fallback.

## Phase 10 — FastAPI Backend

Status: complete on 2026-07-26.

Added OpenAPI-documented prediction, explanation, alert, feedback, history,
health, readiness, liveness, and metrics endpoints with JWT authentication and
role-based permissions. Authentication is injected through a deployment-specific
user provider rather than embedding credentials in the repository.

Executed checks: 72 tests passed, including hybrid, database, migration, Redis,
API, authentication, permission, validation, and full regression coverage.
Ruff, formatting, and mypy also passed locally.

## Phase 11 — Streamlit dashboard

Status: complete on 2026-07-26.

Added an authenticated API client, reusable data transformations and
components, bounded live refresh, alert lifecycle actions, transaction
investigation, customer and merchant history, offline evaluation, drift, system
health, and analyst feedback views.

## Phase 12 — MLOps

Status: complete on 2026-07-26.

Added MLflow tracking and model logging, registry aliases, explicit promotion
criteria and rollback context, dataset manifests, compatibility checks, and
bounded seeded Optuna tuning.

## Phase 13 — Monitoring

Status: complete on 2026-07-26.

Added Prometheus metrics and API exposition, dependency health checks,
Evidently-compatible drift reports with JSON/HTML artifacts, Prometheus alert
rules, and provisioned Grafana dashboards.

## Phase 14 — Security

Status: complete. Added issuer/audience-aware JWT validation, token revocation,
permission-based RBAC, Redis-backed rate limiting, request bounds, PII masking,
secure logging, secret validation, container hardening, dependency-scan targets,
and the practical threat model in [`SECURITY.md`](../SECURITY.md).

## Phase 15 — Testing

Status: complete. Added broad unit, API, integration, Spark, streaming,
database, MLOps, monitoring, security, regression, failure-recovery, and
performance-marked coverage with local and service-backed execution guidance.

## Phase 16 — Performance

Status: complete. Added bounded benchmark suites for inference, API, and Redis
paths with reproducible configuration, environment metadata, raw samples, and
summary statistics. Heavy external-service benchmarks remain opt-in.

## Phase 17 — Docker deployment

Status: complete. Added pinned custom images and a health-gated Compose stack
for Kafka, Kafka UI, Spark, PostgreSQL, Redis, MLflow, FastAPI, Streamlit,
Prometheus, and Grafana with persistent volumes and documented lifecycle.

## Phase 18 — CI/CD

Status: complete. Added pull-request, main-branch, security, CodeQL, Docker,
documentation, Dependabot, and manually gated release workflows. Local checks
are shared through Makefile and uv commands; image publishing is opt-in.

## Phase 19 — Research experiments

Status: complete. Added deterministic temporal experiment configuration,
dataset/reproducibility metadata, model comparisons, configurable score-source
ablations, fraud-cost and calibration analysis, bootstrap intervals, masked
error/segment reports, optional plots, and MLflow logging.

## Phase 20 — Documentation and final demonstration

Status: complete. Added architecture, setup, API, model card, dataset card,
feature, CI/CD, research, screenshot, and full demonstration guides; refreshed
the README, contribution/security policy, changelog, and offline demo CLI.
