import pandas as pd
import pytest

from sentinelstream.data.splitting import temporal_split


def test_temporal_split_is_ordered_and_disjoint() -> None:
    frame = pd.DataFrame(
        {
            "transaction_id": [f"tx-{i}" for i in range(20)],
            "timestamp": pd.date_range("2025-01-01", periods=20, freq="h"),
            "fraud_label": [i % 5 == 0 for i in range(20)],
        }
    ).sample(frac=1.0, random_state=4)

    split = temporal_split(frame)

    assert split.train["timestamp"].is_monotonic_increasing
    assert split.validation["timestamp"].is_monotonic_increasing
    assert split.test["timestamp"].is_monotonic_increasing
    ids = [set(part["transaction_id"]) for part in (split.train, split.validation, split.test)]
    assert ids[0].isdisjoint(ids[1])
    assert ids[1].isdisjoint(ids[2])


@pytest.mark.parametrize(
    "kwargs",
    [
        {"train_fraction": 0.0},
        {"validation_fraction": 0.0},
        {"train_fraction": 0.8, "validation_fraction": 0.3},
    ],
)
def test_temporal_split_rejects_invalid_fractions(kwargs: dict[str, float]) -> None:
    frame = pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=5, freq="h")})

    with pytest.raises(ValueError):
        temporal_split(frame, **kwargs)
