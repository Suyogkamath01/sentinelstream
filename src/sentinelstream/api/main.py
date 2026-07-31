"""Uvicorn entry point for the local or environment-configured API service."""

from __future__ import annotations

from sentinelstream.api.app import Authenticator, create_app
from sentinelstream.api.security import Role
from sentinelstream.config.settings import AppSettings, load_settings


def _environment_authenticator(settings: AppSettings) -> Authenticator | None:
    username = settings.dashboard.auth_username
    password = settings.dashboard.auth_password

    if username is None or password is None:
        return None

    def authenticate(candidate: str, candidate_password: str) -> list[Role] | None:
        if candidate == username and candidate_password == password.get_secret_value():
            return [Role.ADMIN]
        return None

    return authenticate


settings = load_settings()
app = create_app(
    settings,
    authenticator=_environment_authenticator(settings),
    initialize_database=True,
)
