"""Dependency health checks with explicit unknown and degraded states."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Literal

import httpx
from sqlalchemy import select

from sentinelstream.database.engine import Database
from sentinelstream.database.redis_cache import RedisCache

HealthState = Literal["healthy", "degraded", "unavailable", "unknown"]
HealthCallable = Callable[[], tuple[HealthState, str]]


@dataclass(frozen=True, slots=True)
class ComponentHealth:
    """Result of one dependency check."""

    name: str
    status: HealthState
    detail: str
    latency_ms: float | None
    checked_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class HealthSummary:
    """Aggregated dependency health without hiding individual failures."""

    status: HealthState
    components: tuple[ComponentHealth, ...]
    checked_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "checked_at": self.checked_at,
            "components": [component.to_dict() for component in self.components],
        }


class HealthChecker:
    """Run named checks and preserve a failure in the component result."""

    def __init__(
        self, checks: dict[str, HealthCallable], logger: logging.Logger | None = None
    ) -> None:
        self.checks = checks
        self.logger = logger or logging.getLogger(__name__)

    def check(self) -> HealthSummary:
        results: list[ComponentHealth] = []
        for name, check in self.checks.items():
            started = perf_counter()
            try:
                state, detail = check()
            except Exception as exc:  # dependency checks must never break the caller
                self.logger.warning(
                    "health_check_failed", extra={"component": name, "error": str(exc)}
                )
                state, detail = "unavailable", str(exc)
            results.append(
                ComponentHealth(
                    name=name,
                    status=state,
                    detail=detail,
                    latency_ms=(perf_counter() - started) * 1_000,
                    checked_at=datetime.now(UTC).isoformat(),
                )
            )
        states = {component.status for component in results}
        if not results or states == {"healthy"}:
            status: HealthState = "healthy" if results else "unknown"
        elif "unavailable" in states:
            status = "unavailable"
        elif "degraded" in states or "unknown" in states:
            status = "degraded"
        else:
            status = "degraded"
        return HealthSummary(status, tuple(results), datetime.now(UTC).isoformat())


def database_check(database: Database) -> tuple[HealthState, str]:
    with database.engine.connect() as connection:
        connection.execute(select(1))
    return "healthy", "connection succeeded"


def redis_check(cache: RedisCache) -> tuple[HealthState, str]:
    return (
        ("healthy", "connection succeeded")
        if cache.healthcheck()
        else ("degraded", "fallback cache active")
    )


def http_check(url: str | None, timeout_seconds: float) -> tuple[HealthState, str]:
    if not url:
        return "unknown", "no health URL configured"
    response = httpx.get(url, timeout=timeout_seconds)
    if 200 <= response.status_code < 300:
        return "healthy", f"HTTP {response.status_code}"
    return "degraded", f"HTTP {response.status_code}"


def model_file_check(path: Path | None) -> tuple[HealthState, str]:
    if path is None:
        return "unknown", "no model path configured"
    return (
        ("healthy", "model artifact exists")
        if path.is_file()
        else ("unavailable", "model artifact missing")
    )


def build_health_checker(
    database: Database,
    cache: RedisCache,
    *,
    timeout_seconds: float = 2.0,
    urls: dict[str, str | None] | None = None,
    model_path: Path | None = None,
) -> HealthChecker:
    """Build checks for local dependencies and optionally configured services."""

    checks: dict[str, HealthCallable] = {
        "postgresql": lambda: database_check(database),
        "redis": lambda: redis_check(cache),
        "model": lambda: model_file_check(model_path),
    }
    for name, url in (urls or {}).items():

        def configured_check(url: str | None = url) -> tuple[HealthState, str]:
            return http_check(url, timeout_seconds)

        checks[name] = configured_check
    return HealthChecker(checks)
