"""FastAPI application factory for prediction, alert, feedback, and history APIs."""

from __future__ import annotations

import logging
import secrets
from collections.abc import Callable, Generator, Sequence
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from prometheus_client import CONTENT_TYPE_LATEST
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from sentinelstream.api.schemas import (
    AlertDetailResponse,
    AlertResponse,
    DashboardTransactionResponse,
    ExplanationResponse,
    FeedbackRequest,
    FeedbackResponse,
    HealthResponse,
    MetricsResponse,
    PredictionRequest,
    PredictionResponse,
    ProfileResponse,
    TokenResponse,
    TransactionResponse,
)
from sentinelstream.api.security import (
    Permission,
    Role,
    TokenRevocationStore,
    TokenUser,
    create_access_token,
    decode_access_token,
)
from sentinelstream.api.service import PredictionService
from sentinelstream.config.settings import AppSettings
from sentinelstream.database.engine import Database, create_database
from sentinelstream.database.models import (
    AlertRecord,
    FeedbackRecord,
    PredictionRecord,
    TransactionRecord,
)
from sentinelstream.database.redis_cache import RedisCache
from sentinelstream.database.repositories import (
    AlertRepository,
    FeedbackRepository,
    PredictionRepository,
    ProfileRepository,
    TransactionRepository,
)
from sentinelstream.models.decision import ThresholdPolicy
from sentinelstream.models.hybrid import HybridFraudEngine, HybridWeights
from sentinelstream.monitoring.health import build_health_checker
from sentinelstream.monitoring.metrics import create_metrics
from sentinelstream.security.pii import mask_identifier
from sentinelstream.security.rate_limit import RateLimiter
from sentinelstream.streaming.observability import log_event

Authenticator = Callable[[str, str], Sequence[Role] | None]


def create_app(
    settings: AppSettings | None = None,
    *,
    database: Database | None = None,
    cache: RedisCache | None = None,
    hybrid_engine: HybridFraudEngine | None = None,
    authenticator: Authenticator | None = None,
    initialize_database: bool = True,
) -> FastAPI:
    """Build a configured API application without requiring external services in tests."""

    settings = settings or AppSettings()
    database = database or create_database(settings)
    if initialize_database:
        database.create_all()
    cache = cache or RedisCache(settings.redis)
    engine = hybrid_engine or HybridFraudEngine(
        weights=HybridWeights(
            supervised=settings.hybrid.supervised_weight,
            anomaly=settings.hybrid.anomaly_weight,
            rules=settings.hybrid.rules_weight,
            customer_profile=settings.hybrid.customer_profile_weight,
            merchant_profile=settings.hybrid.merchant_profile_weight,
            device_profile=settings.hybrid.device_profile_weight,
        ),
        policy=ThresholdPolicy(
            version=settings.hybrid.policy_version,
            review_threshold=settings.hybrid.review_threshold,
            block_threshold=settings.hybrid.block_threshold,
            min_confidence=settings.hybrid.min_confidence,
        ),
    )
    jwt_secret = (
        settings.secret_key.get_secret_value()
        if settings.secret_key is not None
        else secrets.token_urlsafe(32)
    )
    app = FastAPI(
        title="SentinelStream Fraud API",
        version="0.1.0",
        description="Authenticated fraud prediction, alert, feedback, and history service.",
    )
    app.state.database = database
    app.state.cache = cache
    app.state.hybrid_engine = engine
    app.state.jwt_secret = jwt_secret
    app.state.authenticator = authenticator
    app.state.settings = settings
    app.state.logger = logging.getLogger("sentinelstream.api")
    app.state.revocation_store = TokenRevocationStore(cache)
    app.state.rate_limiter = RateLimiter(cache, settings.rate_limit)
    app.state.prometheus = create_metrics(settings.monitoring.prometheus_namespace)
    app.state.health_checker = build_health_checker(
        database,
        cache,
        timeout_seconds=settings.monitoring.health_timeout_seconds,
        urls={
            "kafka": settings.monitoring.kafka_health_url,
            "spark": settings.monitoring.spark_health_url,
            "mlflow": settings.monitoring.mlflow_health_url,
            "prometheus": None,
            "grafana": settings.monitoring.grafana_url,
        },
        model_path=settings.spark.model_path,
    )
    app.state.drift_report_dir = settings.monitoring.drift_report_dir

    @app.exception_handler(RequestValidationError)
    async def safe_validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {
                "location": [str(part) for part in error.get("loc", ())],
                "type": str(error.get("type", "validation_error")),
            }
            for error in exc.errors()
        ]
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={"detail": "request validation failed", "errors": errors},
        )

    @app.middleware("http")
    async def observe_requests(request: Request, call_next: Callable[..., Any]) -> Response:
        started = perf_counter()
        supplied_correlation_id = request.headers.get("X-Correlation-ID", "")
        correlation_id = (
            supplied_correlation_id
            if supplied_correlation_id
            and len(supplied_correlation_id) <= 64
            and all(
                character.isalnum() or character in "-_" for character in supplied_correlation_id
            )
            else str(uuid4())
        )
        request.state.correlation_id = correlation_id
        correlation_headers = {"X-Correlation-ID": correlation_id}
        scope = "authentication" if request.url.path == "/v1/auth/token" else "api"
        if settings.rate_limit.enabled:
            client_host = request.client.host if request.client else "unknown"
            identity = f"{client_host}:{request.headers.get('authorization', '')}"
            limit_result = (
                app.state.rate_limiter.check_authentication(identity)
                if scope == "authentication"
                else app.state.rate_limiter.check(identity)
            )
            rate_headers = {
                "X-RateLimit-Limit": str(limit_result.limit),
                "X-RateLimit-Remaining": str(limit_result.remaining),
                "X-RateLimit-Reset": str(limit_result.reset_epoch),
            }
            if not limit_result.allowed:
                app.state.prometheus.record_rate_limit(scope)
                rate_headers["Retry-After"] = str(limit_result.retry_after)
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={"detail": "rate limit exceeded"},
                    headers={**rate_headers, **correlation_headers},
                )
        else:
            rate_headers = {}

        content_length = request.headers.get("content-length")
        try:
            content_length_value = int(content_length) if content_length is not None else 0
        except ValueError:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "invalid content length"},
                headers=correlation_headers,
            )
        if content_length_value > settings.api.max_request_bytes:
            return JSONResponse(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                content={"detail": "request body exceeds the configured size limit"},
                headers=correlation_headers,
            )
        if request.method in {"POST", "PUT", "PATCH"}:
            body = await request.body()
            if len(body) > settings.api.max_request_bytes:
                return JSONResponse(
                    status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                    content={"detail": "request body exceeds the configured size limit"},
                    headers=correlation_headers,
                )
            content_type = request.headers.get("content-type", "").split(";", 1)[0].lower()
            if body and request.url.path != "/v1/auth/token" and content_type != "application/json":
                return JSONResponse(
                    status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                    content={"detail": "application/json content type is required"},
                    headers=correlation_headers,
                )
        response = await call_next(request)
        response.headers.update(rate_headers)
        response.headers.update(correlation_headers)
        route = getattr(request.scope.get("route"), "path", "/unmatched")
        app.state.prometheus.observe_api(
            request.method,
            route,
            response.status_code,
            perf_counter() - started,
        )
        return response

    if settings.api.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(settings.api.cors_origins),
            allow_credentials=False,
            allow_methods=["GET", "POST", "PATCH"],
            allow_headers=["Authorization", "Content-Type"],
        )

    oauth2 = OAuth2PasswordBearer(tokenUrl="/v1/auth/token")

    def get_session(request: Request) -> Generator[Session, None, None]:
        session = request.app.state.database.session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def current_user(request: Request, token: str = Depends(oauth2)) -> TokenUser:
        try:
            user = decode_access_token(
                token,
                secret=request.app.state.jwt_secret,
                issuer=settings.security.issuer,
                audience=settings.security.audience,
            )
            if settings.security.token_revocation_enabled and app.state.revocation_store.is_revoked(
                user
            ):
                raise ValueError("token has been revoked")
            return user
        except Exception as exc:
            request.app.state.prometheus.authentication_failures.inc()
            log_event(
                request.app.state.logger,
                logging.WARNING,
                "authentication_failed",
                path=request.url.path,
                correlation_id=getattr(request.state, "correlation_id", "unknown"),
                error_type=type(exc).__name__,
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid or expired credentials",
                headers={"WWW-Authenticate": "Bearer"},
            ) from exc

    def require_roles(*allowed: Role) -> Callable[..., TokenUser]:
        allowed_roles = set(allowed)

        def dependency(user: TokenUser = Depends(current_user)) -> TokenUser:
            if not user.has_any(allowed_roles):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="insufficient permissions"
                )
            return user

        return dependency

    def require_permissions(*allowed: Permission) -> Callable[..., TokenUser]:
        permissions = set(allowed)

        def dependency(user: TokenUser = Depends(current_user)) -> TokenUser:
            if not any(user.has_permission(permission) for permission in permissions):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="insufficient permissions"
                )
            return user

        return dependency

    def _visible_identifier(value: str, user: TokenUser) -> str:
        if not settings.security.pii_masking_enabled or user.has_permission(
            Permission.VIEW_SENSITIVE_DATA
        ):
            return value
        return str(mask_identifier(value))

    def _present(
        record: Any,
        response_model: Any,
        user: TokenUser,
        *sensitive_fields: str,
    ) -> dict[str, Any]:
        payload = response_model.model_validate(record, from_attributes=True).model_dump()
        for field in sensitive_fields:
            if payload.get(field) is not None:
                payload[field] = _visible_identifier(str(payload[field]), user)
        return payload

    @app.post("/v1/auth/token", response_model=TokenResponse, tags=["system"])
    def issue_token(form: OAuth2PasswordRequestForm = Depends()) -> TokenResponse:
        if app.state.authenticator is None:
            raise HTTPException(status_code=503, detail="authentication provider is not configured")
        roles = app.state.authenticator(form.username, form.password)
        if not roles:
            raise HTTPException(status_code=401, detail="invalid credentials")
        return TokenResponse(
            access_token=create_access_token(
                form.username,
                list(roles),
                secret=app.state.jwt_secret,
                expires_minutes=settings.api.access_token_minutes,
                issuer=settings.security.issuer,
                audience=settings.security.audience,
            ),
            expires_in=settings.api.access_token_minutes * 60,
        )

    @app.post("/v1/auth/revoke", tags=["system"])
    def revoke_token(user: TokenUser = Depends(current_user)) -> dict[str, bool]:
        if settings.security.token_revocation_enabled:
            app.state.revocation_store.revoke(user)
        return {"revoked": True}

    @app.post("/v1/predictions", response_model=PredictionResponse, tags=["predictions"])
    def submit_prediction(
        request_data: PredictionRequest,
        user: TokenUser = Depends(require_permissions(Permission.PREDICT)),
        session: Session = Depends(get_session),
    ) -> dict[str, Any]:
        try:
            prediction = PredictionService(
                session, app.state.cache, app.state.hybrid_engine
            ).predict(request_data)
            app.state.prometheus.record_prediction(
                risk_tier=prediction.risk_tier,
                action=prediction.action,
                model_version=prediction.model_version,
                alert=prediction.action in {"review", "block"},
            )
            return _present(prediction, PredictionResponse, user, "customer_id")
        except IntegrityError as exc:
            raise HTTPException(status_code=409, detail="transaction already exists") from exc

    @app.get(
        "/v1/predictions/{prediction_id}", response_model=PredictionResponse, tags=["predictions"]
    )
    def get_prediction(
        prediction_id: str,
        user: TokenUser = Depends(require_permissions(Permission.VIEW_HISTORY)),
        session: Session = Depends(get_session),
    ) -> dict[str, Any]:
        record = PredictionRepository(session).get(prediction_id)
        if record is None:
            raise HTTPException(status_code=404, detail="prediction not found")
        return _present(record, PredictionResponse, user, "customer_id")

    @app.get(
        "/v1/predictions/{prediction_id}/explanation",
        response_model=ExplanationResponse,
        tags=["predictions"],
    )
    def get_explanation(
        prediction_id: str,
        _: TokenUser = Depends(require_permissions(Permission.VIEW_HISTORY)),
        session: Session = Depends(get_session),
    ) -> ExplanationResponse:
        record = PredictionRepository(session).get(prediction_id)
        if record is None:
            raise HTTPException(status_code=404, detail="prediction not found")
        return ExplanationResponse(
            prediction_message_id=record.prediction_message_id,
            reason_codes=list(record.reason_codes),
            source_scores=dict(record.source_scores),
            feature_version=record.feature_version,
        )

    @app.get("/v1/alerts", response_model=list[AlertResponse], tags=["alerts"])
    def list_alerts(
        status_filter: str | None = Query(default=None, alias="status"),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=50, ge=1, le=200),
        user: TokenUser = Depends(require_permissions(Permission.VIEW_ALERTS)),
        session: Session = Depends(get_session),
    ) -> list[dict[str, Any]]:
        return [
            _present(record, AlertResponse, user, "customer_id")
            for record in AlertRepository(session).list(
                status=status_filter, page=page, page_size=page_size
            )
        ]

    @app.get("/v1/alerts/{alert_id}", response_model=AlertDetailResponse, tags=["alerts"])
    def get_alert(
        alert_id: str,
        user: TokenUser = Depends(require_permissions(Permission.VIEW_ALERTS)),
        session: Session = Depends(get_session),
    ) -> AlertDetailResponse:
        record = AlertRepository(session).get(alert_id)
        if record is None:
            raise HTTPException(status_code=404, detail="alert not found")
        prediction = record.prediction
        return AlertDetailResponse(
            alert_id=record.alert_id,
            prediction_id=record.prediction_id,
            event_id=record.event_id,
            transaction_id=record.transaction_id,
            customer_id=_visible_identifier(record.customer_id, user),
            risk_tier=record.risk_tier,
            action=record.action,
            status=record.status,
            priority=record.priority,
            reason_codes=list(record.reason_codes),
            acknowledged_by=record.acknowledged_by,
            resolved_by=record.resolved_by,
            created_at=record.created_at,
            final_risk_score=prediction.final_risk_score,
            confidence=prediction.confidence,
            model_probability=prediction.model_probability,
            anomaly_score=prediction.anomaly_score,
            rule_score=prediction.rule_score,
            source_scores=dict(prediction.source_scores),
        )

    @app.post("/v1/alerts/{alert_id}/acknowledge", response_model=AlertResponse, tags=["alerts"])
    def acknowledge_alert(
        alert_id: str,
        user: TokenUser = Depends(require_permissions(Permission.ACK_ALERT)),
        session: Session = Depends(get_session),
    ) -> dict[str, Any]:
        record = AlertRepository(session).get(alert_id)
        if record is None:
            raise HTTPException(status_code=404, detail="alert not found")
        try:
            acknowledged = AlertRepository(session).acknowledge(record, user.subject)
            return _present(acknowledged, AlertResponse, user, "customer_id")
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/v1/alerts/{alert_id}/resolve", response_model=AlertResponse, tags=["alerts"])
    def resolve_alert(
        alert_id: str,
        user: TokenUser = Depends(require_permissions(Permission.RESOLVE_ALERT)),
        session: Session = Depends(get_session),
    ) -> dict[str, Any]:
        record = AlertRepository(session).get(alert_id)
        if record is None:
            raise HTTPException(status_code=404, detail="alert not found")
        resolved = AlertRepository(session).resolve(record, user.subject)
        return _present(resolved, AlertResponse, user, "customer_id")

    @app.post("/v1/feedback", response_model=FeedbackResponse, tags=["feedback"])
    def submit_feedback(
        request_data: FeedbackRequest,
        user: TokenUser = Depends(require_permissions(Permission.SUBMIT_FEEDBACK)),
        session: Session = Depends(get_session),
    ) -> dict[str, Any]:
        feedback = FeedbackRepository(session).add(
            feedback_id=str(uuid4()),
            event_id=request_data.event_id,
            transaction_id=request_data.transaction_id,
            analyst_id=user.subject,
            outcome=request_data.outcome,
            note=request_data.note,
        )
        app.state.prometheus.record_feedback(request_data.outcome)
        return _present(feedback, FeedbackResponse, user, "analyst_id")

    @app.get(
        "/v1/feedback/{transaction_id}", response_model=list[FeedbackResponse], tags=["feedback"]
    )
    def list_feedback(
        transaction_id: str,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=50, ge=1, le=200),
        user: TokenUser = Depends(require_permissions(Permission.VIEW_HISTORY)),
        session: Session = Depends(get_session),
    ) -> list[dict[str, Any]]:
        return [
            _present(record, FeedbackResponse, user, "analyst_id")
            for record in FeedbackRepository(session).list_for_transaction(
                transaction_id, page=page, page_size=page_size
            )
        ]

    @app.get(
        "/v1/history/customers/{customer_id}",
        response_model=list[TransactionResponse],
        tags=["history"],
    )
    def customer_history(
        customer_id: str,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=50, ge=1, le=200),
        user: TokenUser = Depends(require_permissions(Permission.VIEW_HISTORY)),
        session: Session = Depends(get_session),
    ) -> Sequence[Any]:
        return [
            _present(record, TransactionResponse, user, "customer_id", "merchant_id", "device_id")
            for record in TransactionRepository(session).history(
                customer_id=customer_id, page=page, page_size=page_size
            )
        ]

    @app.get(
        "/v1/history/merchants/{merchant_id}",
        response_model=list[TransactionResponse],
        tags=["history"],
    )
    def merchant_history(
        merchant_id: str,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=50, ge=1, le=200),
        user: TokenUser = Depends(require_permissions(Permission.VIEW_HISTORY)),
        session: Session = Depends(get_session),
    ) -> Sequence[Any]:
        return [
            _present(record, TransactionResponse, user, "customer_id", "merchant_id", "device_id")
            for record in TransactionRepository(session).history(
                merchant_id=merchant_id, page=page, page_size=page_size
            )
        ]

    @app.get(
        "/v1/profiles/customers/{customer_id}",
        response_model=ProfileResponse,
        tags=["profiles"],
    )
    def customer_profile(
        customer_id: str,
        user: TokenUser = Depends(require_permissions(Permission.VIEW_HISTORY)),
        session: Session = Depends(get_session),
    ) -> ProfileResponse:
        profile = ProfileRepository(session).customer(customer_id)
        if profile is None:
            raise HTTPException(status_code=404, detail="customer profile not found")
        return ProfileResponse(
            entity_id=_visible_identifier(profile.customer_id, user),
            risk_score=profile.risk_score,
            confidence=profile.confidence,
            observations=profile.observations,
            version=profile.version,
            reason_codes=list(profile.reason_codes),
            updated_at=profile.updated_at,
        )

    @app.get(
        "/v1/profiles/merchants/{merchant_id}",
        response_model=ProfileResponse,
        tags=["profiles"],
    )
    def merchant_profile(
        merchant_id: str,
        user: TokenUser = Depends(require_permissions(Permission.VIEW_HISTORY)),
        session: Session = Depends(get_session),
    ) -> ProfileResponse:
        profile = ProfileRepository(session).merchant(merchant_id)
        if profile is None:
            raise HTTPException(status_code=404, detail="merchant profile not found")
        return ProfileResponse(
            entity_id=_visible_identifier(profile.merchant_id, user),
            risk_score=profile.risk_score,
            confidence=profile.confidence,
            observations=profile.observations,
            version=profile.version,
            reason_codes=list(profile.reason_codes),
            updated_at=profile.updated_at,
        )

    @app.get("/v1/history/transactions", response_model=list[TransactionResponse], tags=["history"])
    def search_transactions(
        q: str | None = None,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=50, ge=1, le=200),
        user: TokenUser = Depends(require_permissions(Permission.VIEW_HISTORY)),
        session: Session = Depends(get_session),
    ) -> Sequence[Any]:
        return [
            _present(record, TransactionResponse, user, "customer_id", "merchant_id", "device_id")
            for record in TransactionRepository(session).history(
                search=q, page=page, page_size=page_size
            )
        ]

    @app.get(
        "/v1/dashboard/transactions",
        response_model=list[DashboardTransactionResponse],
        tags=["dashboard"],
    )
    def dashboard_transactions(
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=50, ge=1, le=200),
        user: TokenUser = Depends(require_permissions(Permission.MONITORING)),
        session: Session = Depends(get_session),
    ) -> list[dict[str, Any]]:
        rows = session.execute(
            select(TransactionRecord, PredictionRecord, AlertRecord)
            .outerjoin(
                PredictionRecord,
                PredictionRecord.transaction_id == TransactionRecord.transaction_id,
            )
            .outerjoin(AlertRecord, AlertRecord.prediction_id == PredictionRecord.id)
            .order_by(TransactionRecord.event_time.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).all()
        return [
            {
                "transaction_id": transaction.transaction_id,
                "event_id": transaction.event_id,
                "customer_id": _visible_identifier(transaction.customer_id, user),
                "merchant_id": _visible_identifier(transaction.merchant_id, user),
                "device_id": _visible_identifier(transaction.device_id, user),
                "country": transaction.country,
                "transaction_amount": transaction.transaction_amount,
                "currency": transaction.currency,
                "transaction_status": transaction.transaction_status,
                "event_time": transaction.event_time,
                "prediction_message_id": prediction.prediction_message_id if prediction else None,
                "fraud_probability": prediction.model_probability if prediction else None,
                "final_risk_score": prediction.final_risk_score if prediction else None,
                "confidence": prediction.confidence if prediction else None,
                "risk_tier": prediction.risk_tier if prediction else None,
                "decision": prediction.action if prediction else None,
                "alert_status": alert.status if alert else None,
            }
            for transaction, prediction, alert in rows
        ]

    def health_payload() -> HealthResponse:
        database_ok = False
        try:
            with database.engine.connect() as connection:
                connection.execute(select(1))
            database_ok = True
        except Exception:
            database_ok = False
        redis_ok = app.state.cache.healthcheck()
        return HealthResponse(
            status="ok" if database_ok and redis_ok else "degraded",
            database=database_ok,
            redis=redis_ok,
        )

    @app.get("/health", response_model=HealthResponse, tags=["system"])
    @app.get("/v1/system/health", response_model=HealthResponse, tags=["system"])
    def health() -> HealthResponse:
        return health_payload()

    @app.get("/ready", response_model=HealthResponse, tags=["system"])
    @app.get("/v1/system/readiness", response_model=HealthResponse, tags=["system"])
    def readiness(response: Response) -> HealthResponse:
        payload = health_payload()
        if payload.status != "ok":
            payload.status = "not_ready"
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return payload

    @app.get("/live", response_model=HealthResponse, tags=["system"])
    @app.get("/v1/system/liveness", response_model=HealthResponse, tags=["system"])
    def liveness() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get("/metrics", response_model=MetricsResponse, tags=["system"])
    @app.get("/v1/system/metrics", response_model=MetricsResponse, tags=["system"])
    def metrics(
        _: TokenUser = Depends(require_permissions(Permission.MONITORING)),
        session: Session = Depends(get_session),
    ) -> MetricsResponse:
        risk_rows = session.execute(
            select(PredictionRecord.risk_tier, func.count()).group_by(PredictionRecord.risk_tier)
        ).all()
        average_probability = session.scalar(select(func.avg(PredictionRecord.model_probability)))
        return MetricsResponse(
            predictions_created=int(
                session.scalar(select(func.count()).select_from(PredictionRecord)) or 0
            ),
            alerts_created=int(session.scalar(select(func.count()).select_from(AlertRecord)) or 0),
            feedback_created=int(
                session.scalar(select(func.count()).select_from(FeedbackRecord)) or 0
            ),
            transactions_created=int(
                session.scalar(select(func.count()).select_from(TransactionRecord)) or 0
            ),
            unresolved_alerts=int(
                session.scalar(
                    select(func.count())
                    .select_from(AlertRecord)
                    .where(AlertRecord.status != "resolved")
                )
                or 0
            ),
            reviewed_alerts=int(
                session.scalar(
                    select(func.count())
                    .select_from(AlertRecord)
                    .where(AlertRecord.status.in_(("acknowledged", "resolved")))
                )
                or 0
            ),
            blocked_transactions=int(
                session.scalar(
                    select(func.count())
                    .select_from(PredictionRecord)
                    .where(PredictionRecord.action == "block")
                )
                or 0
            ),
            average_fraud_probability=(
                float(average_probability) if average_probability is not None else None
            ),
            risk_tier_distribution={str(tier): int(count) for tier, count in risk_rows},
        )

    @app.get("/prometheus/metrics", include_in_schema=False)
    def prometheus_metrics() -> Response:
        return Response(content=app.state.prometheus.render(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/v1/system/dependencies", tags=["system"])
    def dependency_health(
        _: TokenUser = Depends(require_permissions(Permission.MONITORING)),
    ) -> dict[str, Any]:
        return app.state.health_checker.check().to_dict()

    @app.get("/v1/system/model/evaluation", tags=["system"])
    def model_evaluation(
        _: TokenUser = Depends(require_permissions(Permission.MONITORING)),
    ) -> dict[str, Any]:
        import json

        path = Path(settings.mlops.artifact_dir) / "baseline_metrics.json"
        if not path.is_file():
            raise HTTPException(status_code=404, detail="no offline evaluation artifact available")
        return json.loads(path.read_text(encoding="utf-8"))

    @app.get("/v1/system/drift/latest", tags=["system"])
    def latest_drift(
        _: TokenUser = Depends(require_permissions(Permission.MONITORING)),
    ) -> dict[str, Any]:
        report_dir: Path = app.state.drift_report_dir
        reports = sorted(
            report_dir.glob("*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not reports:
            raise HTTPException(status_code=404, detail="no drift report available")
        import json

        return json.loads(reports[0].read_text(encoding="utf-8"))

    return app
