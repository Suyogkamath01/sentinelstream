"""Online transaction flow built on the existing feature and decision contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import pandas as pd

from sentinelstream.data.schemas import TransactionEvent
from sentinelstream.features.registry import FEATURE_VERSION
from sentinelstream.features.streaming import StreamingFeatureCalculator
from sentinelstream.models.baselines import rule_scores
from sentinelstream.models.decision import (
    PredictionSignals,
    RecommendedAction,
    RiskDecision,
    ThresholdPolicy,
    make_decision,
)
from sentinelstream.streaming.contracts import (
    AlertMessage,
    AuditEventType,
    AuditMessage,
    DecisionMessage,
    FeedbackMessage,
    PredictionMessage,
    TransactionMessage,
    ValidatedTransactionMessage,
)
from sentinelstream.streaming.producer import KafkaMessageProducer, PublishResult
from sentinelstream.streaming.topics import TopicName


class PredictionFunction(Protocol):
    def __call__(
        self,
        event: TransactionEvent,
        features: dict[str, float | int],
    ) -> PredictionSignals: ...


class ExplanationFunction(Protocol):
    def __call__(
        self,
        event: TransactionEvent,
        features: dict[str, float | int],
        signals: PredictionSignals,
    ) -> dict[str, Any] | None: ...


@dataclass(frozen=True, slots=True)
class PipelineResult:
    """Outcome and broker metadata for one transaction operation."""

    event_id: str
    accepted: bool
    reason: str | None
    prediction: PublishResult | None = None
    alert: PublishResult | None = None
    audit: PublishResult | None = None


@dataclass(slots=True)
class RuleBasedPredictor:
    """Transparent fallback predictor for a local pipeline without a model artifact."""

    model_version: str = "phase6-rules-v1"
    calibration_version: str = "phase6-uncalibrated-v1"

    def __call__(
        self,
        event: TransactionEvent,
        features: dict[str, float | int],
    ) -> PredictionSignals:
        row = {"transaction_amount": event.transaction_amount, **features}
        score = float(rule_scores(pd.DataFrame([row]))[0])
        return PredictionSignals(
            calibrated_probability=score,
            anomaly_score=None,
            rule_score=score,
            confidence=2.0 * abs(score - 0.5),
            model_version=self.model_version,
            calibration_version=self.calibration_version,
        )


@dataclass(slots=True)
class StreamingPipeline:
    """Validate, feature, score, decide, alert, and audit one transaction."""

    producer: KafkaMessageProducer
    predictor: PredictionFunction = field(default_factory=RuleBasedPredictor)
    policy: ThresholdPolicy = field(default_factory=ThresholdPolicy)
    feature_calculator: StreamingFeatureCalculator = field(
        default_factory=StreamingFeatureCalculator
    )
    explanation_function: ExplanationFunction | None = None

    def process_transaction(self, message: TransactionMessage) -> PipelineResult:
        """Process a schema-valid transaction and publish its downstream events."""

        event = message.event
        validated = ValidatedTransactionMessage(
            event=event,
            correlation_id=message.message_id,
        )
        self.producer.publish(validated, topic=TopicName.VALIDATED_TRANSACTIONS)
        feature_result = self.feature_calculator.process(event)
        if not feature_result.accepted:
            audit = self._publish_audit(
                AuditEventType.IGNORED,
                status=feature_result.reason or "ignored",
                source_topic=TopicName.VALIDATED_TRANSACTIONS,
                event=event,
                details={"watermark": feature_result.watermark},
                correlation_id=message.message_id,
            )
            return PipelineResult(
                event_id=str(event.event_id),
                accepted=False,
                reason=feature_result.reason,
                audit=audit,
            )

        signals = self.predictor(event, feature_result.features)
        explanation = (
            self.explanation_function(event, feature_result.features, signals)
            if self.explanation_function is not None
            else None
        )
        prediction_message = PredictionMessage(
            event_id=event.event_id,
            transaction_id=event.transaction_id,
            customer_id=event.customer_id,
            features=feature_result.features,
            calibrated_probability=signals.calibrated_probability,
            anomaly_score=signals.anomaly_score,
            rule_score=signals.rule_score,
            confidence=signals.confidence
            if signals.confidence is not None
            else 2.0 * abs(signals.calibrated_probability - 0.5),
            model_version=signals.model_version,
            calibration_version=signals.calibration_version,
            feature_version=FEATURE_VERSION,
            explanation=explanation,
            correlation_id=message.message_id,
        )
        prediction = self.producer.publish(prediction_message, topic=TopicName.PREDICTIONS)
        decision = make_decision(signals, self.policy)
        alert = None
        if decision.action in {RecommendedAction.REVIEW, RecommendedAction.BLOCK}:
            alert = self.producer.publish(
                self._alert_message(event, decision, prediction_message, message.message_id),
                topic=TopicName.ALERTS,
            )
        audit = self._publish_audit(
            AuditEventType.ALERTED if alert else AuditEventType.PREDICTED,
            status=decision.action.value,
            source_topic=TopicName.PREDICTIONS,
            event=event,
            details={
                "prediction_message_id": prediction.message_id,
                "risk_tier": decision.risk_tier.value,
                "abstained": decision.abstained,
            },
            correlation_id=message.message_id,
        )
        return PipelineResult(
            event_id=str(event.event_id),
            accepted=True,
            reason=None,
            prediction=prediction,
            alert=alert,
            audit=audit,
        )

    def process_feedback(self, message: FeedbackMessage) -> PublishResult:
        """Record analyst feedback without mutating model state in this phase."""

        return self._publish_audit(
            AuditEventType.FEEDBACK_RECEIVED,
            status=message.outcome.value,
            source_topic=TopicName.FEEDBACK,
            event_id=message.event_id,
            transaction_id=message.transaction_id,
            details={"analyst_id": message.analyst_id},
            correlation_id=message.message_id,
        )

    def _alert_message(
        self,
        event: TransactionEvent,
        decision: RiskDecision,
        prediction: PredictionMessage,
        correlation_id: Any,
    ) -> AlertMessage:
        decision_message = DecisionMessage(
            calibrated_probability=decision.calibrated_probability,
            confidence=decision.confidence,
            abstained=decision.abstained,
            risk_tier=decision.risk_tier,
            action=decision.action,
            review_threshold=decision.review_threshold,
            block_threshold=decision.block_threshold,
            policy_version=decision.policy_version,
            model_version=decision.model_version,
            calibration_version=decision.calibration_version,
            reason_codes=decision.reason_codes,
        )
        return AlertMessage(
            event_id=event.event_id,
            transaction_id=event.transaction_id,
            customer_id=event.customer_id,
            decision=decision_message,
            prediction_message_id=prediction.message_id,
            reason_codes=decision.reason_codes,
            explanation=prediction.explanation,
            correlation_id=correlation_id,
        )

    def _publish_audit(
        self,
        event_type: AuditEventType,
        *,
        status: str,
        source_topic: TopicName,
        event: TransactionEvent | None = None,
        event_id: Any | None = None,
        transaction_id: Any | None = None,
        details: dict[str, Any] | None = None,
        correlation_id: Any | None = None,
    ) -> PublishResult:
        audit = AuditMessage(
            event_type=event_type,
            status=status,
            source_topic=source_topic.value,
            event_id=event.event_id if event else event_id,
            transaction_id=event.transaction_id if event else transaction_id,
            details=details or {},
            correlation_id=correlation_id,
        )
        return self.producer.publish(audit, topic=TopicName.AUDIT)
