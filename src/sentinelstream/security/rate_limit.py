"""Redis-backed fixed-window rate limiting with bounded local fallback."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from time import time

from sentinelstream.config.settings import RateLimitSettings
from sentinelstream.database.redis_cache import RedisCache


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    """Decision and headers for one fixed-window request."""

    allowed: bool
    limit: int
    remaining: int
    retry_after: int
    reset_epoch: int


class RateLimiter:
    """Apply a low-cardinality fixed-window limit using the shared cache."""

    def __init__(self, cache: RedisCache, settings: RateLimitSettings) -> None:
        self.cache = cache
        self.settings = settings

    def check(
        self,
        identity: str,
        *,
        scope: str = "api",
        limit: int | None = None,
        window_seconds: int | None = None,
    ) -> RateLimitResult:
        selected_limit = limit or self.settings.requests
        selected_window = window_seconds or self.settings.window_seconds
        now = int(time())
        reset_epoch = ((now // selected_window) + 1) * selected_window
        retry_after = max(1, reset_epoch - now)
        digest = sha256(identity.encode("utf-8")).hexdigest()[:24]
        key = f"{self.settings.key_prefix}:{scope}:{digest}:{now // selected_window}"
        count = self.cache.increment(key, selected_window + 1)
        remaining = max(0, selected_limit - count)
        return RateLimitResult(
            allowed=count <= selected_limit,
            limit=selected_limit,
            remaining=remaining,
            retry_after=retry_after,
            reset_epoch=reset_epoch,
        )

    def check_authentication(self, identity: str) -> RateLimitResult:
        return self.check(
            identity,
            scope="authentication",
            limit=self.settings.authentication_requests,
            window_seconds=self.settings.authentication_window_seconds,
        )
