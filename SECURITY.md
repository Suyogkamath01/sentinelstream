# SentinelStream security model

SentinelStream is a local-first portfolio project. It provides production-style
controls, but it is not a certified banking system and must be threat-modelled
again before processing regulated data.

## Supported versions

The current `main` branch is the only actively maintained development line.
Releases, when created, should document their support window in the changelog.
Local Compose credentials and the development authenticator are not supported
for public exposure.

## Reporting a vulnerability

Do not open a public issue with exploit details, credentials, tokens, or
personal data. Use a private security-advisory channel or maintainer contact
configured by the repository owner. If that channel has not been set up, send
the owner a high-level report through the repository's private contact process
and request a secure follow-up channel. Include the affected component,
impact, reproduction conditions, and a minimal safe proof only after a private
channel is established. Do not test against systems or data you do not own.

## Controls

- API access tokens are HS256 JWTs with short configurable expiry, optional
  issuer and audience validation, unique token IDs, and Redis-backed
  revocation. When Redis is unavailable, revocation and rate-limit state use a
  bounded process-local fallback; multi-instance deployments therefore require
  healthy Redis for shared enforcement.
- Permissions are enforced in FastAPI dependencies and are independent of the
  Streamlit UI. Roles include administrator, fraud analyst, investigator,
  operator, read-only viewer, and service account. Sensitive identifiers
  require the explicit `sensitive:read` permission; read-only responses are
  pseudonymised when PII masking is enabled.
- Rate limits are fixed-window, configurable, and expose standard limit headers.
  Authentication endpoints have a separate, stricter window.
- Secrets come from environment-backed settings. Production requires a
  32-character signing secret and an explicit database URL.
- Structured logs redact credentials, bearer tokens, raw payloads, and
  pseudonymise common identifiers, including exception text. Model input
  remains unchanged; masking is applied only at logging and presentation
  boundaries.
- SQLAlchemy repositories use parameterised ORM queries. API payloads are
  bounded and JSON content types are required for JSON endpoints.
- Container images use pinned tags, non-root custom service users, health checks,
  internal networking, persistent volumes, and no source-controlled secrets.

## Practical threat model

| Threat / scenario | Asset and boundary | Impact | Current mitigation | Residual risk / next control |
| --- | --- | --- | --- | --- |
| Credential theft or forged JWT | API boundary, signing secret | Account takeover and fraud actions | Signature, expiry, issuer/audience checks, role permissions, revocation | HS256 secret rotation and an external identity provider are still future work |
| Brute-force or API abuse | Authentication and API endpoints | Denial of service or credential guessing | Redis-backed per-client windows, bounded fallback, 413/415 validation | Add a gateway/WAF and distributed identity-aware limits |
| PII leakage in logs or dashboard | API-to-operator boundary | Privacy breach | Recursive redaction and stable display pseudonyms | Encrypt storage and apply field-level retention policies |
| Poisoned or malformed Kafka event | Kafka-to-validation boundary | Bad state, failed batches, or downstream contamination | Typed contracts, retries, dead-letter topics, manual offsets | Broker ACLs, TLS/SASL, and schema registry policy must be configured per environment |
| Unauthorised alert change | Analyst/API boundary | Fraud case tampering | Backend permission checks, lifecycle constraints, audit records | Add immutable external audit storage and stronger analyst identity proofing |
| Model or feature manipulation | Training/MLflow boundary | Incorrect risk decisions | Compatibility checks, promotion gates, model metadata | Sign artifacts and restrict registry writes with service identities |
| Dashboard exposure | Browser-to-API boundary | Sensitive operational data disclosure | API authentication, masked table fields, no embedded credentials | Deploy behind SSO, CSP, and a private network |
| Dependency or image compromise | Build and deployment boundary | Code execution or data theft | Bandit and pip-audit targets, pinned image tags, non-root services | Enforce CI scans, SBOM review, and digest pinning |
| Kafka misuse or replay | Producer/consumer boundary | Duplicate actions or event flooding | Idempotent producer, partition keys, offsets, replay mode, deduplication | Broker ACLs and replay authorisation are required in production |
| Database/Redis outage | Persistence boundary | Unavailable or stale operations | Health/readiness checks, transaction rollback, cache fallback | Use managed HA services and explicit stale-cache policy |
| Audit-log tampering | Audit topic and database | Loss of investigation evidence | Audit events and restricted service roles | Append-only/WORM storage and integrity chaining |

Report suspected vulnerabilities privately rather than opening a public issue
with exploit details.
