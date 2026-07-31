from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy.exc import IntegrityError

from sentinelstream.config import AppSettings, RedisSettings
from sentinelstream.database import RedisCache, create_database
from sentinelstream.database.repositories import TransactionRepository
from sentinelstream.monitoring.health import model_file_check


@pytest.mark.integration
@pytest.mark.database
def test_database_constraint_failure_rolls_back_partial_write(tmp_path: Path) -> None:
    database = create_database(AppSettings(), url=f"sqlite:///{tmp_path / 'rollback.db'}")
    database.create_all()
    session = database.session()
    repository = TransactionRepository(session)
    kwargs = {
        "transaction_id": "transaction-1",
        "event_id": "event-1",
        "customer_id": "customer-1",
        "merchant_id": "merchant-1",
        "device_id": "device-1",
        "country": "US",
        "transaction_amount": 10.0,
        "currency": "USD",
        "transaction_status": "approved",
        "event_time": datetime.now(UTC),
        "payload": {},
    }
    repository.add(**kwargs)
    session.commit()
    with pytest.raises(IntegrityError):
        repository.add(**kwargs)
    session.rollback()

    assert repository.get_by_transaction_id("transaction-1") is not None
    assert len(repository.history(customer_id="customer-1")) == 1
    session.close()
    database.close()


@pytest.mark.integration
@pytest.mark.database
def test_redis_unavailability_falls_back_without_losing_cache_semantics() -> None:
    cache = RedisCache(RedisSettings(enabled=False, fallback_max_entries=100))

    cache.set_json("risk:customer:test", {"risk_score": 0.8}, ttl_seconds=60)
    assert cache.get_json("risk:customer:test") == {"risk_score": 0.8}
    assert cache.increment("counter", 60) == 1
    assert cache.increment("counter", 60) == 2
    cache.delete("risk:customer:test", "counter")
    assert cache.get_json("risk:customer:test") is None


@pytest.mark.integration
def test_missing_model_is_reported_as_unavailable() -> None:
    state, message = model_file_check(Path("/tmp/sentinelstream-missing-model"))

    assert state == "unavailable"
    assert "missing" in message
