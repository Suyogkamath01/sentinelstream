"""Deterministic customer and merchant profiles for simulation."""

from __future__ import annotations

from dataclasses import dataclass
from random import Random

from sentinelstream.data.schemas import Channel


@dataclass(frozen=True, slots=True)
class CustomerProfile:
    customer_id: str
    account_id: str
    card_id: str
    device_id: str
    home_country: str
    home_city: str
    latitude: float
    longitude: float
    currency: str
    average_amount: float
    preferred_channels: tuple[Channel, ...]
    salary_day: int


@dataclass(frozen=True, slots=True)
class MerchantProfile:
    merchant_id: str
    category: str
    country: str
    city: str
    latitude: float
    longitude: float
    currency: str
    risk_level: str


LOCATIONS = (
    ("US", "New York", 40.7128, -74.0060, "USD"),
    ("GB", "London", 51.5074, -0.1278, "GBP"),
    ("IN", "Mumbai", 19.0760, 72.8777, "INR"),
    ("SG", "Singapore", 1.3521, 103.8198, "SGD"),
    ("DE", "Berlin", 52.5200, 13.4050, "EUR"),
    ("AU", "Sydney", -33.8688, 151.2093, "AUD"),
    ("BR", "Sao Paulo", -23.5505, -46.6333, "BRL"),
    ("CA", "Toronto", 43.6532, -79.3832, "CAD"),
    ("JP", "Tokyo", 35.6762, 139.6503, "JPY"),
)

MERCHANT_CATEGORIES = (
    "grocery",
    "fuel",
    "restaurant",
    "travel",
    "electronics",
    "digital_goods",
    "healthcare",
    "utilities",
    "fashion",
    "gaming",
    "gambling",
    "cash_advance",
)


def build_customers(count: int, rng: Random) -> list[CustomerProfile]:
    """Create stable customer profiles from a seeded random generator."""

    customers: list[CustomerProfile] = []
    for index in range(count):
        country, city, latitude, longitude, currency = rng.choice(LOCATIONS)
        channels = tuple(rng.sample(list(Channel), k=rng.randint(1, 3)))
        customers.append(
            CustomerProfile(
                customer_id=f"customer-{index:06d}",
                account_id=f"account-{index:06d}",
                card_id=f"card-{index:06d}",
                device_id=f"device-{index:06d}",
                home_country=country,
                home_city=city,
                latitude=latitude,
                longitude=longitude,
                currency=currency,
                average_amount=round(rng.uniform(15.0, 450.0), 2),
                preferred_channels=channels,
                salary_day=rng.randint(1, 28),
            )
        )
    return customers


def build_merchants(count: int, rng: Random) -> list[MerchantProfile]:
    """Create merchant profiles with geographic and risk attributes."""

    merchants: list[MerchantProfile] = []
    for index in range(count):
        country, city, latitude, longitude, currency = rng.choice(LOCATIONS)
        risk_level = rng.choices(("low", "medium", "high"), weights=(70, 25, 5), k=1)[0]
        merchants.append(
            MerchantProfile(
                merchant_id=f"merchant-{index:06d}",
                category=rng.choice(MERCHANT_CATEGORIES),
                country=country,
                city=city,
                latitude=latitude,
                longitude=longitude,
                currency=currency,
                risk_level=risk_level,
            )
        )
    return merchants
