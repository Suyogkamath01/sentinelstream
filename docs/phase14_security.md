# Phase 14 — Security

Phase 14 extends the existing FastAPI and streaming implementation rather than
creating a second security stack.

## Configuration

Security settings are under security and rate_limit in configs/base.yaml and
can be overridden with SENTINELSTREAM_SECURITY__* and
SENTINELSTREAM_RATE_LIMIT__* variables. Production requires
SENTINELSTREAM_SECRET_KEY with at least 32 characters and
SENTINELSTREAM_DATABASE__URL.

## Roles and permissions

src/sentinelstream/api/security.py defines the role-to-permission mapping.
Endpoints depend on permissions, so a dashboard control cannot bypass backend
authorisation. service_account is deliberately limited to prediction and
monitoring capabilities.

## Operational limitations

Redis is required for shared token revocation and rate limiting in a
multi-instance deployment. The local fallback is bounded for development and
single-process recovery only. Token rotation, asymmetric signing, external
identity federation, broker ACL provisioning, and encrypted-at-rest policy are
deployment responsibilities.
