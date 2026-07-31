"""Low-cardinality Prometheus metrics used by the API and pipelines."""

from __future__ import annotations

from time import perf_counter
from typing import Any, Literal

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

RiskTierLabel = Literal["low", "medium", "high", "critical", "unknown"]


class PrometheusMetrics:
    """Own an isolated registry so tests and application instances do not collide."""

    def __init__(self, registry: CollectorRegistry, namespace: str = "sentinelstream") -> None:
        self.registry = registry
        self.namespace = namespace
        self.api_requests = Counter(
            "api_requests_total",
            "HTTP requests handled by the API",
            ("method", "route", "status"),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.api_duration = Histogram(
            "api_request_duration_seconds",
            "HTTP request duration in seconds",
            ("method", "route"),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.authentication_failures = Counter(
            "authentication_failures_total",
            "Rejected authentication attempts",
            namespace=self.namespace,
            registry=self.registry,
        )
        self.rate_limit_events = Counter(
            "rate_limit_events_total",
            "Requests rejected by rate limiting",
            ("scope",),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.predictions = Counter(
            "predictions_generated_total",
            "Fraud predictions generated",
            ("risk_tier", "action", "model_version"),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.alerts = Counter(
            "fraud_alerts_generated_total",
            "Fraud alerts generated",
            ("risk_tier", "priority"),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.feedback = Counter(
            "analyst_feedback_total",
            "Analyst feedback outcomes",
            ("outcome",),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.streaming_events = Counter(
            "streaming_events_total",
            "Streaming events by outcome",
            ("component", "outcome"),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.streaming_batch_duration = Histogram(
            "streaming_batch_duration_seconds",
            "Streaming micro-batch duration",
            ("component",),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.consumer_lag = Gauge(
            "kafka_consumer_lag",
            "Reported consumer lag when available",
            ("consumer_group", "topic"),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.data_quality = Counter(
            "data_quality_events_total",
            "Data-quality events",
            ("kind",),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.drift_score = Gauge(
            "drift_score",
            "Latest drift score by monitored field",
            ("kind", "feature"),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.model_info = Gauge(
            "model_info",
            "Active model metadata; value is always one",
            ("model_version", "feature_version", "threshold_version"),
            namespace=self.namespace,
            registry=self.registry,
        )
        self.last_batch_timestamp = Gauge(
            "streaming_last_batch_timestamp_seconds",
            "Unix timestamp of the last completed streaming batch",
            ("component",),
            namespace=self.namespace,
            registry=self.registry,
        )

    def observe_api(self, method: str, route: str, status: int, duration_seconds: float) -> None:
        self.api_requests.labels(method, route, str(status)).inc()
        self.api_duration.labels(method, route).observe(max(duration_seconds, 0.0))

    def record_prediction(
        self,
        *,
        risk_tier: str,
        action: str,
        model_version: str,
        alert: bool = False,
        priority: int | None = None,
    ) -> None:
        tier = (
            risk_tier.lower()
            if risk_tier.lower() in {"low", "medium", "high", "critical"}
            else "unknown"
        )
        self.predictions.labels(tier, action, model_version).inc()
        if alert:
            self.alerts.labels(tier, str(priority or 0)).inc()

    def record_rate_limit(self, scope: str) -> None:
        self.rate_limit_events.labels(scope).inc()

    def record_feedback(self, outcome: str) -> None:
        self.feedback.labels(outcome).inc()

    def record_streaming_event(self, component: str, outcome: str, count: int = 1) -> None:
        self.streaming_events.labels(component, outcome).inc(max(count, 0))

    def record_streaming_batch(self, component: str, duration_seconds: float) -> None:
        self.streaming_batch_duration.labels(component).observe(max(duration_seconds, 0.0))

    def set_consumer_lag(self, consumer_group: str, topic: str, lag: float) -> None:
        self.consumer_lag.labels(consumer_group, topic).set(max(lag, 0.0))

    def record_data_quality(self, kind: str, count: int = 1) -> None:
        self.data_quality.labels(kind).inc(max(count, 0))

    def set_drift(self, kind: str, feature: str, score: float) -> None:
        self.drift_score.labels(kind, feature).set(max(score, 0.0))

    def record_drift_report(self, report: Any) -> None:
        """Publish the bounded summary of a drift report without raw identifiers."""

        for metric in report.feature_metrics:
            self.set_drift("feature", metric.feature, metric.score)
        if report.prediction_metric is not None:
            self.set_drift(
                "prediction", report.prediction_metric.feature, report.prediction_metric.score
            )

    def set_model_info(
        self, model_version: str, feature_version: str, threshold_version: str
    ) -> None:
        self.model_info.labels(model_version, feature_version, threshold_version).set(1.0)

    def render(self) -> bytes:
        """Render the registry in Prometheus exposition format."""

        return generate_latest(self.registry)

    def timer(self) -> _MetricsTimer:
        return _MetricsTimer(self)


class _MetricsTimer:
    def __init__(self, metrics: PrometheusMetrics) -> None:
        self.metrics = metrics
        self.started = 0.0

    def __enter__(self) -> _MetricsTimer:
        self.started = perf_counter()
        return self

    def __exit__(self, *_: object) -> None:
        self.metrics.record_streaming_batch("application", perf_counter() - self.started)


def create_metrics(namespace: str = "sentinelstream") -> PrometheusMetrics:
    """Create an application metrics registry."""

    return PrometheusMetrics(CollectorRegistry(), namespace=namespace)
