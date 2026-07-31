"""Redis cache with bounded local fallback and explicit invalidation."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Mapping
from typing import Any

from sentinelstream.config.settings import RedisSettings


class RedisCache:
    """Cache JSON values in Redis while remaining available during outages."""

    def __init__(self, settings: RedisSettings | None = None) -> None:
        self.settings = settings or RedisSettings()
        self._logger = logging.getLogger(__name__)
        self._client: Any | None = None
        self._fallback: dict[str, tuple[float, str]] = {}
        self._lock = threading.RLock()
        if self.settings.enabled:
            try:
                import redis

                client = redis.Redis.from_url(
                    self.settings.url,
                    decode_responses=True,
                    socket_timeout=self.settings.socket_timeout_seconds,
                )
                client.ping()
                self._client = client
            except Exception as exc:
                self._logger.warning("redis_unavailable_using_fallback", extra={"error": str(exc)})

    @property
    def backend_available(self) -> bool:
        return self._client is not None

    def get_json(self, key: str) -> Any | None:
        try:
            raw = self._client.get(key) if self._client else self._fallback_get(key)
            return json.loads(raw) if raw is not None else None
        except Exception as exc:
            self._logger.warning("redis_read_failed", extra={"error": str(exc)})
            return self._fallback_value(key)

    def set_json(self, key: str, value: Any, *, ttl_seconds: int | None = None) -> None:
        ttl = ttl_seconds or self.settings.default_ttl_seconds
        encoded = json.dumps(value, sort_keys=True, default=str)
        try:
            if self._client:
                self._client.setex(key, ttl, encoded)
            else:
                with self._lock:
                    self._prune_fallback_locked()
                    self._fallback[key] = (time.monotonic() + ttl, encoded)
        except Exception as exc:
            self._logger.warning("redis_write_failed", extra={"error": str(exc)})
            with self._lock:
                self._prune_fallback_locked()
                self._fallback[key] = (time.monotonic() + ttl, encoded)

    def increment(self, key: str, ttl_seconds: int) -> int:
        """Increment a counter and retain it for one rate-limit window."""

        if ttl_seconds < 1:
            raise ValueError("ttl_seconds must be positive")
        try:
            if self._client:
                count = int(self._client.incr(key))
                if count == 1:
                    self._client.expire(key, ttl_seconds)
                return count
            return self._fallback_increment(key, ttl_seconds)
        except Exception as exc:
            self._logger.warning("redis_increment_failed", extra={"error": str(exc)})
            return self._fallback_increment(key, ttl_seconds)

    def delete(self, *keys: str) -> None:
        if not keys:
            return
        try:
            if self._client:
                self._client.delete(*keys)
            else:
                with self._lock:
                    for key in keys:
                        self._fallback.pop(key, None)
        except Exception as exc:
            self._logger.warning("redis_delete_failed", extra={"error": str(exc)})
            with self._lock:
                for key in keys:
                    self._fallback.pop(key, None)

    def cache_customer_risk(self, customer_id: str, value: Mapping[str, Any]) -> None:
        self.set_json(f"risk:customer:{customer_id}", dict(value))

    def cache_merchant_risk(self, merchant_id: str, value: Mapping[str, Any]) -> None:
        self.set_json(f"risk:merchant:{merchant_id}", dict(value))

    def cache_device_risk(self, device_id: str, value: Mapping[str, Any]) -> None:
        self.set_json(f"risk:device:{device_id}", dict(value))

    def cache_prediction(self, prediction_id: str, value: Mapping[str, Any]) -> None:
        self.set_json(f"prediction:{prediction_id}", dict(value))

    def cache_session(
        self, session_id: str, value: Mapping[str, Any], *, ttl_seconds: int | None = None
    ) -> None:
        self.set_json(f"session:{session_id}", dict(value), ttl_seconds=ttl_seconds)

    def invalidate_customer(self, customer_id: str) -> None:
        self.delete(f"risk:customer:{customer_id}")

    def invalidate_merchant(self, merchant_id: str) -> None:
        self.delete(f"risk:merchant:{merchant_id}")

    def invalidate_device(self, device_id: str) -> None:
        self.delete(f"risk:device:{device_id}")

    def healthcheck(self) -> bool:
        if not self.settings.enabled:
            return True
        try:
            return bool(self._client and self._client.ping())
        except Exception:
            return False

    def _fallback_get(self, key: str) -> str | None:
        with self._lock:
            item = self._fallback.get(key)
            if item is None:
                return None
            if item[0] <= time.monotonic():
                self._fallback.pop(key, None)
                return None
            return item[1]

    def _fallback_increment(self, key: str, ttl_seconds: int) -> int:
        with self._lock:
            self._prune_fallback_locked()
            current = self._fallback.get(key)
            if current is None or current[0] <= time.monotonic():
                count = 1
                expiry = time.monotonic() + ttl_seconds
            else:
                count = int(json.loads(current[1])) + 1
                expiry = current[0]
            self._fallback[key] = (expiry, json.dumps(count))
            return count

    def _prune_fallback_locked(self) -> None:
        now = time.monotonic()
        expired = [key for key, (expiry, _) in self._fallback.items() if expiry <= now]
        for key in expired:
            self._fallback.pop(key, None)
        maximum = self.settings.fallback_max_entries
        while len(self._fallback) >= maximum:
            oldest = min(self._fallback, key=lambda item: self._fallback[item][0])
            self._fallback.pop(oldest, None)

    def _fallback_value(self, key: str) -> Any | None:
        raw = self._fallback_get(key)
        return json.loads(raw) if raw is not None else None
