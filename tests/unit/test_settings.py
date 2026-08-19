from pathlib import Path

import pytest
from pydantic import ValidationError

from sentinelstream.config.settings import AppSettings, Environment, load_settings


def test_yaml_profile_loads() -> None:
    settings = load_settings(environment=Environment.TESTING)

    assert settings.environment is Environment.TESTING
    assert settings.simulation.random_seed == 7
    assert settings.simulation.fraud_ratio == 0.10


def test_environment_values_override_yaml(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SENTINELSTREAM_SIMULATION__FRAUD_RATIO", "0.25")
    monkeypatch.setenv("SENTINELSTREAM_LOG_LEVEL", "DEBUG")

    settings = load_settings(environment="testing")

    assert settings.simulation.fraud_ratio == 0.25
    assert settings.log_level == "DEBUG"


def test_invalid_simulation_range_is_rejected() -> None:
    with pytest.raises(ValidationError):
        AppSettings(simulation={"min_amount": 10, "max_amount": 1})


def test_production_requires_secret_key() -> None:
    with pytest.raises(ValidationError):
        AppSettings(environment=Environment.PRODUCTION)


def test_production_rejects_development_secret_placeholder() -> None:
    with pytest.raises(ValidationError, match="development placeholder"):
        AppSettings(
            environment=Environment.PRODUCTION,
            secret_key="local-development-secret-change-me-32-chars",
            database_url="sqlite:///production-isolated-test.db",
        )


def test_missing_config_path_is_rejected() -> None:
    with pytest.raises(FileNotFoundError):
        load_settings(Path("configs/does-not-exist.yaml"))
