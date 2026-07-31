"""Reproducible synthetic transaction event generator."""

from __future__ import annotations

import copy
import math
import random
from collections.abc import Iterable
from datetime import timedelta
from ipaddress import IPv4Address
from typing import Any
from uuid import UUID, uuid5

from sentinelstream.config.settings import SimulationSettings
from sentinelstream.data.schemas import (
    AuthenticationMethod,
    Channel,
    FraudScenario,
    SimulatedTransaction,
    SimulationGroundTruth,
    TransactionEvent,
    TransactionStatus,
    TransactionType,
)
from sentinelstream.data.serialization import GeneratedRecord
from sentinelstream.simulation.event_clock import EventClock
from sentinelstream.simulation.fraud_scenarios import SCENARIOS, apply_fraud_scenario
from sentinelstream.simulation.personas import (
    CustomerProfile,
    MerchantProfile,
    build_customers,
    build_merchants,
)

_ID_NAMESPACE = UUID("c4f5d43d-a7e8-4d1c-ae34-1ee1e60fca31")


class TransactionGenerator:
    """Generate transaction records and controlled event-quality anomalies."""

    def __init__(self, settings: SimulationSettings) -> None:
        self.settings = settings
        self.rng = random.Random(settings.random_seed)  # nosec B311
        self.customers = build_customers(settings.customer_count, self.rng)
        self.merchants = build_merchants(settings.merchant_count, self.rng)
        self.clock = EventClock(settings.start_time, settings.duration_days, self.rng)

    def generate(
        self,
        count: int,
        *,
        scenarios: Iterable[FraudScenario] | None = None,
    ) -> list[GeneratedRecord]:
        """Generate `count` base records and configured anomaly records."""

        if count < 1:
            raise ValueError("count must be positive")
        scenario_plan = list(scenarios or ())
        fraud_count = max(len(scenario_plan), round(count * self.settings.fraud_ratio))
        fraud_count = min(fraud_count, count)
        fraud_indices = set(self.rng.sample(range(count), fraud_count))
        assigned_scenarios = self._scenario_assignments(fraud_count, scenario_plan)

        records: list[SimulatedTransaction] = []
        for index in range(count):
            scenario = assigned_scenarios.pop(0) if index in fraud_indices else None
            record = self._build_record(index, scenario, total_count=count)
            records.append(record)

        output: list[GeneratedRecord] = list(records)
        output.extend(self._duplicate_records(records))
        output.extend(self._malformed_records(records))
        if self.settings.replay:
            output.extend(copy.deepcopy(records))
        return self._apply_ordering(output)

    def generate_scenario(self, scenario: FraudScenario) -> SimulatedTransaction:
        """Generate one valid record for a requested fraud scenario."""

        return self._build_record(0, scenario, total_count=1)

    def _scenario_assignments(
        self,
        fraud_count: int,
        requested: list[FraudScenario],
    ) -> list[FraudScenario]:
        assignments = requested[:fraud_count] if requested else []
        while len(assignments) < fraud_count:
            assignments.append(SCENARIOS[len(assignments) % len(SCENARIOS)])
        self.rng.shuffle(assignments)
        if requested:
            assignments[: len(requested)] = requested[:fraud_count]
        return assignments

    def _build_record(
        self,
        index: int,
        scenario: FraudScenario | None,
        *,
        total_count: int,
    ) -> SimulatedTransaction:
        customer = self.rng.choice(self.customers)
        merchant = self.rng.choice(self.merchants)
        burst = self.rng.random() < self.settings.burst_probability
        event_time = self.clock.event_time(index, burst=burst)
        event = self._build_event(index, customer, merchant, event_time)
        if self.settings.drift_enabled and index >= int(
            self.settings.drift_after_fraction * total_count
        ):
            event = event.model_copy(
                update={
                    "transaction_amount": round(
                        min(event.transaction_amount * 1.35, 10_000_000.0), 2
                    ),
                    "channel": Channel.MOBILE,
                }
            )
        if scenario is not None:
            event = apply_fraud_scenario(
                event, scenario, seed=f"{self.settings.random_seed}-{index}"
            )
        if self.rng.random() < self.settings.late_event_rate:
            delay = self.rng.randint(1, self.settings.max_delay_seconds)
            event = event.model_copy(
                update={"timestamp": event.timestamp - timedelta(seconds=delay)}
            )
        return SimulatedTransaction(
            event=event,
            ground_truth=SimulationGroundTruth(
                fraud_label=scenario is not None, fraud_type=scenario
            ),
        )

    def _build_event(
        self,
        index: int,
        customer: CustomerProfile,
        merchant: MerchantProfile,
        event_time: Any,
    ) -> TransactionEvent:
        transaction_id = uuid5(_ID_NAMESPACE, f"transaction-{self.settings.random_seed}-{index}")
        event_id = uuid5(_ID_NAMESPACE, f"event-{self.settings.random_seed}-{index}")
        channel = self.rng.choice(customer.preferred_channels)
        amount = self.rng.lognormvariate(
            mu=math.log(customer.average_amount),
            sigma=0.65,
        )
        amount = round(min(max(amount, self.settings.min_amount), self.settings.max_amount), 2)
        transaction_type = TransactionType.PURCHASE
        if channel is Channel.ATM:
            transaction_type = TransactionType.CASH_WITHDRAWAL
        elif channel is Channel.BANK_TRANSFER:
            transaction_type = TransactionType.TRANSFER
        elif self.rng.random() < 0.03:
            transaction_type = TransactionType.REFUND
        card_present = channel in {Channel.POS, Channel.ATM}
        authentication = self.rng.choice(
            [
                AuthenticationMethod.THREE_DS,
                AuthenticationMethod.PIN,
                AuthenticationMethod.BIOMETRIC,
                AuthenticationMethod.OTP,
            ]
        )
        if channel is Channel.ATM:
            authentication = AuthenticationMethod.PIN
        ingestion_time = self.clock.ingestion_time(event_time, index)
        latitude = customer.latitude + self.rng.uniform(-0.15, 0.15)
        longitude = customer.longitude + self.rng.uniform(-0.15, 0.15)
        country = customer.home_country
        city = customer.home_city
        if self.rng.random() < 0.2:
            country, city = merchant.country, merchant.city
            latitude, longitude = merchant.latitude, merchant.longitude
        return TransactionEvent(
            transaction_id=transaction_id,
            event_id=event_id,
            customer_id=customer.customer_id,
            account_id=customer.account_id,
            card_id=customer.card_id,
            merchant_id=merchant.merchant_id,
            merchant_category=merchant.category,
            transaction_amount=amount,
            currency=customer.currency,
            transaction_type=transaction_type,
            channel=channel,
            timestamp=event_time,
            country=country,
            city=city,
            latitude=max(-90.0, min(90.0, latitude)),
            longitude=max(-180.0, min(180.0, longitude)),
            device_id=customer.device_id,
            ip_address=IPv4Address(f"192.0.2.{(index % 250) + 1}"),
            card_present=card_present,
            authentication_method=authentication,
            transaction_status=TransactionStatus.APPROVED,
            event_version=1,
            ingestion_timestamp=ingestion_time,
        )

    def _duplicate_records(self, records: list[SimulatedTransaction]) -> list[SimulatedTransaction]:
        duplicate_count = round(len(records) * self.settings.duplicate_rate)
        if duplicate_count == 0:
            return []
        selected = self.rng.sample(records, duplicate_count)
        return copy.deepcopy(selected)

    def _malformed_records(self, records: list[SimulatedTransaction]) -> list[dict[str, Any]]:
        malformed_count = round(len(records) * self.settings.malformed_rate)
        malformed: list[dict[str, Any]] = []
        for record in self.rng.sample(records, malformed_count):
            raw = record.to_json_record()
            raw["event"].pop("transaction_id", None)
            raw["malformed_reason"] = "missing transaction_id"
            malformed.append(raw)
        return malformed

    def _apply_ordering(self, records: list[GeneratedRecord]) -> list[GeneratedRecord]:
        if self.settings.out_of_order_rate == 0.0 or len(records) < 2:
            return records
        if self.settings.out_of_order_rate >= 1.0:
            records.reverse()
            return records
        selected = [
            index
            for index in range(len(records))
            if self.rng.random() < self.settings.out_of_order_rate
        ]
        selected_records = [records[index] for index in selected]
        self.rng.shuffle(selected_records)
        for index, record in zip(selected, selected_records, strict=True):
            records[index] = record
        return records
