from collections import Counter

from sentinelstream.config.settings import SimulationSettings
from sentinelstream.data.schemas import FraudScenario, SimulatedTransaction
from sentinelstream.simulation.generator import TransactionGenerator


def settings(**updates: object) -> SimulationSettings:
    return SimulationSettings(
        customer_count=25,
        merchant_count=10,
        **updates,
    )


def test_generation_is_reproducible() -> None:
    first = TransactionGenerator(settings(random_seed=123)).generate(100)
    second = TransactionGenerator(settings(random_seed=123)).generate(100)

    assert [
        record.model_dump(mode="json") if isinstance(record, SimulatedTransaction) else record
        for record in first
    ] == [
        record.model_dump(mode="json") if isinstance(record, SimulatedTransaction) else record
        for record in second
    ]


def test_valid_records_have_unique_identifiers() -> None:
    records = TransactionGenerator(settings()).generate(200)
    valid = [record for record in records if isinstance(record, SimulatedTransaction)]

    assert len({record.event.transaction_id for record in valid}) == len(valid)
    assert len({record.event.event_id for record in valid}) == len(valid)


def test_fraud_ratio_is_configurable() -> None:
    records = TransactionGenerator(settings(fraud_ratio=0.20)).generate(100)
    valid = [record for record in records if isinstance(record, SimulatedTransaction)]

    assert sum(record.ground_truth.fraud_label for record in valid) == 20
    assert all(record.event.transaction_amount > 0 for record in valid)


def test_each_fraud_scenario_produces_a_valid_labelled_event() -> None:
    generator = TransactionGenerator(settings())

    generated = [generator.generate_scenario(scenario) for scenario in FraudScenario]

    assert {record.ground_truth.fraud_type for record in generated} == set(FraudScenario)
    assert all(record.ground_truth.fraud_label for record in generated)


def test_duplicate_event_generation_preserves_event_id() -> None:
    records = TransactionGenerator(settings(duplicate_rate=1.0)).generate(30)
    valid = [record for record in records if isinstance(record, SimulatedTransaction)]
    counts = Counter(record.event.event_id for record in valid)

    assert len(valid) == 60
    assert all(count == 2 for count in counts.values())


def test_malformed_event_generation_is_quarantinable() -> None:
    records = TransactionGenerator(settings(malformed_rate=1.0)).generate(30)

    malformed = [record for record in records if not isinstance(record, SimulatedTransaction)]

    assert len(malformed) == 30
    assert all(record["malformed_reason"] == "missing transaction_id" for record in malformed)


def test_late_and_out_of_order_events_are_generated() -> None:
    records = TransactionGenerator(
        settings(late_event_rate=1.0, max_delay_seconds=3_600, out_of_order_rate=1.0)
    ).generate(25)
    valid = [record for record in records if isinstance(record, SimulatedTransaction)]
    timestamps = [record.event.timestamp for record in valid]

    assert any(record.event.timestamp < settings().start_time for record in valid)
    assert timestamps != sorted(timestamps)


def test_replay_mode_reuses_event_identifiers() -> None:
    records = TransactionGenerator(settings(replay=True)).generate(20)
    valid = [record for record in records if isinstance(record, SimulatedTransaction)]

    assert len(valid) == 40
    assert len({record.event.event_id for record in valid}) == 20
