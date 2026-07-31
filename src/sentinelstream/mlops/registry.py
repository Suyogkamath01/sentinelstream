"""Model registry adapter using MLflow aliases for reversible promotion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sentinelstream.mlops.tracking import MLflowUnavailableError


@dataclass(frozen=True, slots=True)
class RegisteredModelVersion:
    name: str
    version: str
    run_id: str | None
    aliases: tuple[str, ...]
    tags: dict[str, str]


class ModelRegistry:
    """Thin, testable wrapper around the MLflow model registry client."""

    def __init__(self, *, tracking_uri: str, registry_uri: str | None = None) -> None:
        self.tracking_uri = tracking_uri
        self.registry_uri = registry_uri

    def _client(self) -> Any:
        try:
            import mlflow
            from mlflow import MlflowClient
        except ImportError as exc:
            raise MLflowUnavailableError(
                "mlflow-skinny is required for model registry operations"
            ) from exc
        mlflow.set_tracking_uri(self.tracking_uri)
        if self.registry_uri:
            mlflow.set_registry_uri(self.registry_uri)
        return MlflowClient(tracking_uri=self.registry_uri or self.tracking_uri)

    def register(
        self, model_uri: str, *, name: str, tags: dict[str, str] | None = None
    ) -> RegisteredModelVersion:
        try:
            client = self._client()
            try:
                client.get_registered_model(name)
            except Exception:
                client.create_registered_model(name)
            result = client.create_model_version(name=name, source=model_uri, run_id=None)
            for key, value in (tags or {}).items():
                client.set_model_version_tag(name, result.version, key, value)
            return self.get(name, str(result.version))
        except Exception as exc:
            raise MLflowUnavailableError(f"model registration failed: {exc}") from exc

    def get(self, name: str, version: str) -> RegisteredModelVersion:
        try:
            model_version = self._client().get_model_version(name, version)
            aliases = tuple(sorted(getattr(model_version, "aliases", []) or []))
            tags = dict(getattr(model_version, "tags", {}) or {})
            return RegisteredModelVersion(
                name=name,
                version=str(model_version.version),
                run_id=getattr(model_version, "run_id", None),
                aliases=aliases,
                tags=tags,
            )
        except Exception as exc:
            raise MLflowUnavailableError(f"model lookup failed: {exc}") from exc

    def set_alias(self, name: str, version: str, alias: str) -> RegisteredModelVersion:
        if alias not in {"development", "staging", "production", "archived"}:
            raise ValueError("unsupported model lifecycle alias")
        try:
            client = self._client()
            client.set_registered_model_alias(name, alias, version)
            return self.get(name, version)
        except Exception as exc:
            raise MLflowUnavailableError(f"model alias update failed: {exc}") from exc

    def load(self, name: str, alias: str = "production") -> Any:
        try:
            import mlflow

            mlflow.set_tracking_uri(self.tracking_uri)
            return mlflow.pyfunc.load_model(f"models:/{name}@{alias}")
        except Exception as exc:
            raise MLflowUnavailableError(f"model loading failed: {exc}") from exc
