"""JWT authentication and role-based access control helpers."""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import jwt

from sentinelstream.database.redis_cache import RedisCache


class Role(StrEnum):
    """Roles understood by the REST API."""

    ADMIN = "admin"
    ANALYST = "analyst"
    INVESTIGATOR = "investigator"
    OPERATOR = "operator"
    READONLY = "readonly"
    SERVICE_ACCOUNT = "service_account"


class Permission(StrEnum):
    """Backend capabilities assigned to one or more roles."""

    PREDICT = "predictions:write"
    VIEW_ALERTS = "alerts:read"
    ACK_ALERT = "alerts:acknowledge"
    RESOLVE_ALERT = "alerts:resolve"
    SUBMIT_FEEDBACK = "feedback:write"
    VIEW_HISTORY = "history:read"
    MODEL_ADMIN = "models:admin"
    MONITORING = "monitoring:read"
    SYSTEM_ADMIN = "system:admin"
    VIEW_SENSITIVE_DATA = "sensitive:read"


ROLE_PERMISSIONS: dict[Role, frozenset[Permission]] = {
    Role.ADMIN: frozenset(Permission),
    Role.ANALYST: frozenset(
        {
            Permission.PREDICT,
            Permission.VIEW_ALERTS,
            Permission.ACK_ALERT,
            Permission.RESOLVE_ALERT,
            Permission.SUBMIT_FEEDBACK,
            Permission.VIEW_HISTORY,
            Permission.MONITORING,
            Permission.VIEW_SENSITIVE_DATA,
        }
    ),
    Role.INVESTIGATOR: frozenset(
        {
            Permission.PREDICT,
            Permission.VIEW_ALERTS,
            Permission.ACK_ALERT,
            Permission.VIEW_HISTORY,
            Permission.MONITORING,
            Permission.VIEW_SENSITIVE_DATA,
        }
    ),
    Role.OPERATOR: frozenset(
        {
            Permission.PREDICT,
            Permission.VIEW_ALERTS,
            Permission.VIEW_HISTORY,
            Permission.VIEW_SENSITIVE_DATA,
        }
    ),
    Role.READONLY: frozenset(
        {Permission.VIEW_ALERTS, Permission.VIEW_HISTORY, Permission.MONITORING}
    ),
    Role.SERVICE_ACCOUNT: frozenset(
        {Permission.PREDICT, Permission.MONITORING, Permission.VIEW_SENSITIVE_DATA}
    ),
}


class TokenRevocationStore:
    """Persist token revocations in Redis with the shared bounded fallback."""

    def __init__(self, cache: RedisCache, *, key_prefix: str = "auth:revoked") -> None:
        self.cache = cache
        self.key_prefix = key_prefix

    def revoke(self, user: TokenUser) -> None:
        if not user.jti or user.expires_at is None:
            return
        ttl = max(1, int((user.expires_at - datetime.now(UTC)).total_seconds()))
        self.cache.set_json(f"{self.key_prefix}:{user.jti}", {"revoked": True}, ttl_seconds=ttl)

    def is_revoked(self, user: TokenUser) -> bool:
        return bool(user.jti and self.cache.get_json(f"{self.key_prefix}:{user.jti}"))


@dataclass(frozen=True, slots=True)
class TokenUser:
    """Authenticated subject and its granted API roles."""

    subject: str
    roles: frozenset[Role]
    jti: str | None = None
    expires_at: datetime | None = None

    def has_any(self, allowed: set[Role]) -> bool:
        return bool(self.roles & allowed)

    def has_permission(self, permission: Permission) -> bool:
        return any(permission in ROLE_PERMISSIONS[role] for role in self.roles)


def create_access_token(
    subject: str,
    roles: set[Role] | frozenset[Role] | list[Role],
    *,
    secret: str,
    expires_minutes: int = 30,
    now: datetime | None = None,
    issuer: str | None = None,
    audience: str | None = None,
    jti: str | None = None,
) -> str:
    """Create a short-lived signed JWT for an authenticated user."""

    if not subject.strip() or not secret:
        raise ValueError("subject and signing secret are required")
    if expires_minutes < 1:
        raise ValueError("expires_minutes must be positive")
    issued_at = (now or datetime.now(UTC)).astimezone(UTC)
    payload = {
        "sub": subject,
        "roles": sorted(Role(role).value for role in roles),
        "iat": issued_at,
        "exp": issued_at + timedelta(minutes=expires_minutes),
        "jti": jti or secrets.token_urlsafe(18),
    }
    if issuer:
        payload["iss"] = issuer
    if audience:
        payload["aud"] = audience
    return jwt.encode(payload, secret, algorithm="HS256")


def decode_access_token(
    token: str,
    *,
    secret: str,
    issuer: str | None = None,
    audience: str | None = None,
) -> TokenUser:
    """Verify signature and expiry before returning a typed principal."""

    if not token or not secret:
        raise ValueError("token and signing secret are required")
    payload: dict[str, Any] = jwt.decode(
        token,
        secret,
        algorithms=["HS256"],
        options={"require": ["sub", "roles", "exp", "iat"]},
        issuer=issuer,
        audience=audience,
    )
    subject = payload.get("sub")
    roles = payload.get("roles")
    jti = payload.get("jti")
    if not isinstance(subject, str) or not subject.strip() or not isinstance(roles, list):
        raise ValueError("invalid JWT claims")
    if jti is not None and (not isinstance(jti, str) or not jti.strip()):
        raise ValueError("invalid JWT identifier")
    try:
        parsed_roles = frozenset(Role(role) for role in roles)
    except ValueError as exc:
        raise ValueError("invalid JWT role") from exc
    expires_at = payload.get("exp")
    expires_datetime = (
        datetime.fromtimestamp(float(expires_at), tz=UTC) if expires_at is not None else None
    )
    return TokenUser(
        subject=subject,
        roles=parsed_roles,
        jti=jti,
        expires_at=expires_datetime,
    )
