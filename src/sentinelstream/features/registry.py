"""Feature metadata used to document offline and online parity."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    """Metadata for one versioned feature contract."""

    name: str
    version: str
    source: str
    point_in_time_safe: bool
    online_supported: bool


FEATURE_VERSION = "phase3-v1"
FEATURE_DEFINITIONS = tuple(
    FeatureDefinition(
        name=name,
        version=FEATURE_VERSION,
        source="transaction_event_and_customer_history",
        point_in_time_safe=True,
        online_supported=True,
    )
    for name in (
        "customer_prior_transaction_count",
        "customer_prior_amount_mean",
        "recent_transaction_count_10m",
        "recent_amount_sum_1h",
        "merchant_novelty",
        "device_novelty",
        "country_novelty",
        "geographic_velocity_kmh",
        "impossible_travel",
    )
)
