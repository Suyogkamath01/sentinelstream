"""Small repository objects for transaction, risk, alert, feedback, and audit data."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, TypeVar

from sqlalchemy import Select, desc, or_, select
from sqlalchemy.orm import Session

from sentinelstream.database.models import (
    AlertRecord,
    AuditRecord,
    CustomerProfile,
    DeviceProfile,
    FeedbackRecord,
    MerchantProfile,
    PredictionRecord,
    TransactionRecord,
)

ModelT = TypeVar("ModelT")


def _page(session: Session, query: Select[Any], *, page: int, page_size: int) -> Sequence[Any]:
    if page < 1 or not 1 <= page_size <= 200:
        raise ValueError("page must be positive and page_size must be between 1 and 200")
    return session.scalars(query.offset((page - 1) * page_size).limit(page_size)).all()


class TransactionRepository:
    """Queries for transaction history and search."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, **values: Any) -> TransactionRecord:
        record = TransactionRecord(**values)
        self.session.add(record)
        self.session.flush()
        return record

    def get_by_transaction_id(self, transaction_id: str) -> TransactionRecord | None:
        return self.session.scalar(
            select(TransactionRecord).where(TransactionRecord.transaction_id == transaction_id)
        )

    def history(
        self,
        *,
        customer_id: str | None = None,
        merchant_id: str | None = None,
        search: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> Sequence[TransactionRecord]:
        query = select(TransactionRecord).order_by(desc(TransactionRecord.event_time))
        filters = []
        if customer_id:
            filters.append(TransactionRecord.customer_id == customer_id)
        if merchant_id:
            filters.append(TransactionRecord.merchant_id == merchant_id)
        if search:
            pattern = f"%{search}%"
            filters.append(
                or_(
                    TransactionRecord.transaction_id.like(pattern),
                    TransactionRecord.event_id.like(pattern),
                    TransactionRecord.customer_id.like(pattern),
                    TransactionRecord.merchant_id.like(pattern),
                )
            )
        return _page(self.session, query.where(*filters), page=page, page_size=page_size)


class PredictionRepository:
    """Create and retrieve persisted risk predictions."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, **values: Any) -> PredictionRecord:
        record = PredictionRecord(**values)
        self.session.add(record)
        self.session.flush()
        return record

    def get(self, prediction_message_id: str) -> PredictionRecord | None:
        return self.session.scalar(
            select(PredictionRecord).where(
                PredictionRecord.prediction_message_id == prediction_message_id
            )
        )

    def list_for_customer(
        self, customer_id: str, *, page: int = 1, page_size: int = 50
    ) -> Sequence[PredictionRecord]:
        query = (
            select(PredictionRecord)
            .where(PredictionRecord.customer_id == customer_id)
            .order_by(desc(PredictionRecord.created_at))
        )
        return _page(self.session, query, page=page, page_size=page_size)


class AlertRepository:
    """Alert queue queries and lifecycle transitions."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, **values: Any) -> AlertRecord:
        record = AlertRecord(**values)
        self.session.add(record)
        self.session.flush()
        return record

    def get(self, alert_id: str) -> AlertRecord | None:
        return self.session.scalar(select(AlertRecord).where(AlertRecord.alert_id == alert_id))

    def list(
        self,
        *,
        status: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> Sequence[AlertRecord]:
        query = select(AlertRecord).order_by(
            desc(AlertRecord.priority), desc(AlertRecord.created_at)
        )
        if status:
            query = query.where(AlertRecord.status == status)
        return _page(self.session, query, page=page, page_size=page_size)

    def acknowledge(self, alert: AlertRecord, actor: str) -> AlertRecord:
        if alert.status == "resolved":
            raise ValueError("resolved alerts cannot be acknowledged")
        alert.status = "acknowledged"
        alert.acknowledged_by = actor
        alert.acknowledged_at = datetime.now(UTC)
        self.session.flush()
        return alert

    def resolve(self, alert: AlertRecord, actor: str) -> AlertRecord:
        alert.status = "resolved"
        alert.resolved_by = actor
        alert.resolved_at = datetime.now(UTC)
        self.session.flush()
        return alert


class FeedbackRepository:
    """Analyst feedback persistence."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, **values: Any) -> FeedbackRecord:
        record = FeedbackRecord(**values)
        self.session.add(record)
        self.session.flush()
        return record

    def list_for_transaction(
        self, transaction_id: str, *, page: int = 1, page_size: int = 50
    ) -> Sequence[FeedbackRecord]:
        query = (
            select(FeedbackRecord)
            .where(FeedbackRecord.transaction_id == transaction_id)
            .order_by(desc(FeedbackRecord.created_at))
        )
        return _page(self.session, query, page=page, page_size=page_size)


class ProfileRepository:
    """Upsert and retrieve the three profile types used by hybrid scoring."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def customer(self, customer_id: str) -> CustomerProfile | None:
        return self.session.get(CustomerProfile, customer_id)

    def merchant(self, merchant_id: str) -> MerchantProfile | None:
        return self.session.get(MerchantProfile, merchant_id)

    def device(self, device_id: str) -> DeviceProfile | None:
        return self.session.get(DeviceProfile, device_id)

    def upsert_customer(self, customer_id: str, **values: Any) -> CustomerProfile:
        profile = self.customer(customer_id) or CustomerProfile(customer_id=customer_id)
        for key, value in values.items():
            setattr(profile, key, value)
        self.session.add(profile)
        self.session.flush()
        return profile

    def upsert_merchant(self, merchant_id: str, **values: Any) -> MerchantProfile:
        profile = self.merchant(merchant_id) or MerchantProfile(merchant_id=merchant_id)
        for key, value in values.items():
            setattr(profile, key, value)
        self.session.add(profile)
        self.session.flush()
        return profile

    def upsert_device(self, device_id: str, **values: Any) -> DeviceProfile:
        profile = self.device(device_id) or DeviceProfile(device_id=device_id)
        for key, value in values.items():
            setattr(profile, key, value)
        self.session.add(profile)
        self.session.flush()
        return profile


class AuditRepository:
    """Append-only audit persistence."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, **values: Any) -> AuditRecord:
        record = AuditRecord(**values)
        self.session.add(record)
        self.session.flush()
        return record

    def list(self, *, page: int = 1, page_size: int = 50) -> Sequence[AuditRecord]:
        return _page(
            self.session,
            select(AuditRecord).order_by(desc(AuditRecord.created_at)),
            page=page,
            page_size=page_size,
        )
