from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect

from sentinelstream.config import AppSettings, RedisSettings
from sentinelstream.database.engine import create_database
from sentinelstream.database.redis_cache import RedisCache
from sentinelstream.database.repositories import (
    AlertRepository,
    AuditRepository,
    FeedbackRepository,
    PredictionRepository,
    ProfileRepository,
    TransactionRepository,
)


def _migration_config(database_url: str) -> Config:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    config.set_main_option("script_location", "migrations")
    return config


def test_alembic_upgrade_and_downgrade_create_the_schema(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'migration.db'}"
    config = _migration_config(database_url)

    command.upgrade(config, "head")
    database = create_database(AppSettings(), url=database_url)
    with database.engine.connect() as connection:
        tables = set(inspect(connection).get_table_names())
    assert {"transactions", "predictions", "alerts", "audit_records"} <= tables
    command.downgrade(config, "base")
    with database.engine.connect() as connection:
        assert inspect(connection).get_table_names() == ["alembic_version"]
    database.close()


def test_repositories_and_cache_persist_and_invalidate(tmp_path: Path) -> None:
    database = create_database(
        AppSettings(),
        url=f"sqlite:///{tmp_path / 'repositories.db'}",
    )
    database.create_all()
    session = database.session()
    transaction_id = str(uuid4())
    event_id = str(uuid4())
    TransactionRepository(session).add(
        transaction_id=transaction_id,
        event_id=event_id,
        customer_id="customer-001",
        merchant_id="merchant-001",
        device_id="device-001",
        country="US",
        transaction_amount=100.0,
        currency="USD",
        transaction_status="approved",
        event_time=datetime.now(UTC),
        payload={"event_id": event_id},
    )
    prediction = PredictionRepository(session).add(
        prediction_message_id=str(uuid4()),
        event_id=event_id,
        transaction_id=transaction_id,
        customer_id="customer-001",
        final_risk_score=0.9,
        confidence=0.8,
        risk_tier="critical",
        action="block",
        model_probability=0.9,
        anomaly_score=0.8,
        rule_score=0.7,
        model_version="test",
        calibration_version="test",
        feature_version="test",
        reason_codes=["high_risk"],
        source_scores={"supervised": 0.9},
    )
    alert = AlertRepository(session).add(
        alert_id=str(uuid4()),
        prediction_id=prediction.id,
        event_id=event_id,
        transaction_id=transaction_id,
        customer_id="customer-001",
        risk_tier="critical",
        action="block",
        status="open",
        priority=120,
        reason_codes=["high_risk"],
    )
    feedback = FeedbackRepository(session).add(
        feedback_id=str(uuid4()),
        event_id=event_id,
        transaction_id=transaction_id,
        analyst_id="analyst-001",
        outcome="confirmed_fraud",
        note="reviewed",
    )
    ProfileRepository(session).upsert_customer(
        "customer-001", risk_score=0.7, confidence=0.9, observations=10
    )
    AuditRepository(session).add(
        message_id=str(uuid4()),
        event_type="predicted",
        status="block",
        source_topic="sentinelstream.predictions.v1",
        event_id=event_id,
        transaction_id=transaction_id,
        details={"source": "test"},
    )
    session.commit()

    assert PredictionRepository(session).get(prediction.prediction_message_id) is not None
    assert len(TransactionRepository(session).history(customer_id="customer-001")) == 1
    assert AlertRepository(session).acknowledge(alert, "analyst-001").status == "acknowledged"
    assert AlertRepository(session).resolve(alert, "analyst-001").status == "resolved"
    assert FeedbackRepository(session).list_for_transaction(transaction_id)[0].id == feedback.id

    cache = RedisCache(RedisSettings(enabled=False, default_ttl_seconds=10))
    cache.set_json("key", {"value": 1})
    assert cache.get_json("key") == {"value": 1}
    cache.delete("key")
    assert cache.get_json("key") is None
    session.close()
    database.close()
