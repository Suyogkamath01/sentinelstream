"""Application services joining request validation, hybrid scoring, and storage."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

import pandas as pd
from sqlalchemy.orm import Session

from sentinelstream.api.schemas import PredictionRequest
from sentinelstream.database.models import PredictionRecord
from sentinelstream.database.redis_cache import RedisCache
from sentinelstream.database.repositories import (
    AlertRepository,
    AuditRepository,
    PredictionRepository,
    ProfileRepository,
    TransactionRepository,
)
from sentinelstream.features.behavioural import calculate_behavioural_features
from sentinelstream.models.baselines import rule_scores
from sentinelstream.models.hybrid import (
    HybridFraudEngine,
    HybridSignals,
    ProfileRisk,
)


@dataclass(slots=True)
class PredictionService:
    """Create an auditable prediction and associated alert atomically."""

    session: Session
    cache: RedisCache
    engine: HybridFraudEngine

    def predict(self, request: PredictionRequest) -> PredictionRecord:
        event = request.event
        existing = (
            self.session.query(PredictionRecord).filter_by(event_id=str(event.event_id)).first()
        )
        if existing is not None:
            return existing
        history = calculate_behavioural_features(event, [])
        features = {**history, "transaction_amount": event.transaction_amount}
        calculated_rule = float(rule_scores(pd.DataFrame([features]))[0])
        profiles = ProfileRepository(self.session)
        customer = self._profile("customer", event.customer_id, profiles)
        merchant = self._profile("merchant", event.merchant_id, profiles)
        device = self._profile("device", event.device_id, profiles)
        result = self.engine.score(
            HybridSignals(
                supervised_probability=request.supervised_probability,
                anomaly_score=request.anomaly_score,
                rule_score=request.rule_score
                if request.rule_score is not None
                else calculated_rule,
                customer_profile=customer,
                merchant_profile=merchant,
                device_profile=device,
                model_version="phase8-api-model",
                calibration_version="phase8-api-calibration",
            )
        )
        transaction = TransactionRepository(self.session).add(
            transaction_id=str(event.transaction_id),
            event_id=str(event.event_id),
            customer_id=event.customer_id,
            merchant_id=event.merchant_id,
            device_id=event.device_id,
            country=event.country,
            transaction_amount=event.transaction_amount,
            currency=event.currency,
            transaction_status=event.transaction_status.value,
            event_time=event.timestamp,
            payload=event.model_dump(mode="json"),
        )
        prediction = PredictionRepository(self.session).add(
            prediction_message_id=str(uuid4()),
            event_id=str(event.event_id),
            transaction_id=transaction.transaction_id,
            customer_id=event.customer_id,
            final_risk_score=result.final_score,
            confidence=result.confidence,
            risk_tier=result.decision.risk_tier.value,
            action=result.decision.action.value,
            model_probability=request.supervised_probability,
            anomaly_score=request.anomaly_score,
            rule_score=result.source_scores.get("rules"),
            model_version="phase8-api-model",
            calibration_version="phase8-api-calibration",
            feature_version="phase8-v1",
            reason_codes=list(result.reason_codes),
            source_scores=result.source_scores,
        )
        if result.is_alert:
            AlertRepository(self.session).add(
                alert_id=str(uuid4()),
                prediction_id=prediction.id,
                event_id=str(event.event_id),
                transaction_id=str(event.transaction_id),
                customer_id=event.customer_id,
                risk_tier=result.decision.risk_tier.value,
                action=result.decision.action.value,
                status="open",
                priority=result.alert_priority,
                reason_codes=list(result.reason_codes),
            )
        AuditRepository(self.session).add(
            message_id=str(uuid4()),
            correlation_id=prediction.prediction_message_id,
            event_type="predicted",
            status=result.decision.action.value,
            source_topic="sentinelstream.predictions.v1",
            event_id=str(event.event_id),
            transaction_id=str(event.transaction_id),
            details={"source_scores": result.source_scores},
        )
        self.session.flush()
        self.cache.cache_prediction(
            prediction.prediction_message_id,
            {
                "prediction_message_id": prediction.prediction_message_id,
                "final_risk_score": prediction.final_risk_score,
                "action": prediction.action,
            },
        )
        return prediction

    def _profile(
        self, kind: str, entity_id: str, repository: ProfileRepository
    ) -> ProfileRisk | None:
        cached = self.cache.get_json(f"risk:{kind}:{entity_id}")
        if cached:
            return ProfileRisk(**cached)
        record = {
            "customer": repository.customer,
            "merchant": repository.merchant,
            "device": repository.device,
        }[kind](entity_id)
        if record is None:
            return None
        profile = ProfileRisk(
            entity_id=entity_id,
            risk_score=record.risk_score,
            confidence=record.confidence,
            observations=record.observations,
            version=record.version,
            reason_codes=tuple(record.reason_codes),
        )
        self.cache.set_json(
            f"risk:{kind}:{entity_id}",
            profile.__dict__
            if hasattr(profile, "__dict__")
            else {
                "entity_id": profile.entity_id,
                "risk_score": profile.risk_score,
                "confidence": profile.confidence,
                "observations": profile.observations,
                "version": profile.version,
                "reason_codes": list(profile.reason_codes),
            },
        )
        return profile
