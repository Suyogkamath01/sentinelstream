import numpy as np
import pandas as pd

from sentinelstream.config.settings import SimulationSettings
from sentinelstream.features.batch import build_batch_features
from sentinelstream.simulation.generator import TransactionGenerator


def source_frame(count: int = 40) -> pd.DataFrame:
    generator = TransactionGenerator(
        SimulationSettings(customer_count=4, merchant_count=4, random_seed=14, fraud_ratio=0.0)
    )
    records = generator.generate(count)
    return pd.DataFrame([record.to_json_record()["event"] for record in records])


def test_prior_features_start_empty_and_advance_with_history() -> None:
    features = build_batch_features(source_frame())
    first_by_customer = features.groupby("customer_id", sort=False).head(1)
    second_by_customer = features.groupby("customer_id", sort=False).nth(1)

    assert (first_by_customer["customer_prior_transaction_count"] == 0).all()
    assert (second_by_customer["customer_prior_transaction_count"] == 1).all()
    assert (first_by_customer["customer_prior_amount_mean"] == 0).all()


def test_future_transaction_changes_do_not_change_prior_features() -> None:
    source = source_frame()
    baseline = build_batch_features(source)
    last_event_id = source.sort_values("timestamp").iloc[-1]["event_id"]
    changed = source.copy()
    changed.loc[changed["event_id"] == last_event_id, "transaction_amount"] *= 100
    updated = build_batch_features(changed)
    feature_columns = [
        "customer_prior_transaction_count",
        "customer_prior_amount_mean",
        "customer_prior_amount_max",
        "customer_seconds_since_previous",
        "amount_z_score",
    ]
    baseline_prior = baseline[baseline["event_id"] != last_event_id].sort_values("event_id")
    updated_prior = updated[updated["event_id"] != last_event_id].sort_values("event_id")

    np.testing.assert_allclose(
        baseline_prior[feature_columns].to_numpy(dtype=float),
        updated_prior[feature_columns].to_numpy(dtype=float),
    )
