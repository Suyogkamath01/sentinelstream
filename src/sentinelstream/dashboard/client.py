"""Small authenticated HTTP client used by the Streamlit dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


class DashboardAPIError(RuntimeError):
    """Raised for an API response the dashboard cannot use."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(slots=True)
class DashboardClient:
    """Authenticated API client with no domain logic beyond endpoint routing."""

    base_url: str
    timeout_seconds: float = 5.0
    verify_tls: bool = True
    token: str | None = None
    transport: httpx.BaseTransport | None = None

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url.rstrip("/"),
            timeout=self.timeout_seconds,
            verify=self.verify_tls,
            transport=self.transport,
        )

    def authenticate(self, username: str, password: str) -> int:
        try:
            with self._client() as client:
                response = client.post(
                    "/v1/auth/token",
                    data={"username": username, "password": password},
                )
        except httpx.HTTPError as exc:
            raise DashboardAPIError(f"API connection failed: {exc}") from exc
        if response.status_code >= 400:
            raise DashboardAPIError("authentication failed", status_code=response.status_code)
        payload = response.json()
        self.token = str(payload["access_token"])
        return int(payload.get("expires_in", 0))

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = dict(kwargs.pop("headers", {}))
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            with self._client() as client:
                response = client.request(method, path, headers=headers, **kwargs)
        except httpx.HTTPError as exc:
            raise DashboardAPIError(f"API connection failed: {exc}") from exc
        if response.status_code >= 400:
            detail = response.text[:300] or "API request failed"
            raise DashboardAPIError(detail, status_code=response.status_code)
        if not response.content:
            return None
        return response.json()

    def get(self, path: str, **params: Any) -> Any:
        return self.request(
            "GET", path, params={key: value for key, value in params.items() if value is not None}
        )

    def post(self, path: str, payload: dict[str, Any] | None = None) -> Any:
        return self.request("POST", path, json=payload or {})

    def metrics(self) -> dict[str, Any]:
        return self.get("/v1/system/metrics")

    def transactions(self, *, page: int = 1, page_size: int = 50) -> list[dict[str, Any]]:
        return self.get("/v1/dashboard/transactions", page=page, page_size=page_size)

    def alerts(
        self, *, status: str | None = None, page: int = 1, page_size: int = 50
    ) -> list[dict[str, Any]]:
        return self.get("/v1/alerts", status=status, page=page, page_size=page_size)

    def alert(self, alert_id: str) -> dict[str, Any]:
        return self.get(f"/v1/alerts/{alert_id}")

    def acknowledge_alert(self, alert_id: str) -> dict[str, Any]:
        return self.post(f"/v1/alerts/{alert_id}/acknowledge")

    def resolve_alert(self, alert_id: str) -> dict[str, Any]:
        return self.post(f"/v1/alerts/{alert_id}/resolve")

    def prediction(self, prediction_id: str) -> dict[str, Any]:
        return self.get(f"/v1/predictions/{prediction_id}")

    def explanation(self, prediction_id: str) -> dict[str, Any]:
        return self.get(f"/v1/predictions/{prediction_id}/explanation")

    def customer_history(self, customer_id: str, *, page_size: int = 50) -> list[dict[str, Any]]:
        return self.get(f"/v1/history/customers/{customer_id}", page_size=page_size)

    def merchant_history(self, merchant_id: str, *, page_size: int = 50) -> list[dict[str, Any]]:
        return self.get(f"/v1/history/merchants/{merchant_id}", page_size=page_size)

    def customer_profile(self, customer_id: str) -> dict[str, Any]:
        return self.get(f"/v1/profiles/customers/{customer_id}")

    def merchant_profile(self, merchant_id: str) -> dict[str, Any]:
        return self.get(f"/v1/profiles/merchants/{merchant_id}")

    def health(self) -> dict[str, Any]:
        return self.get("/v1/system/health")

    def dependencies(self) -> dict[str, Any]:
        return self.get("/v1/system/dependencies")

    def latest_drift(self) -> dict[str, Any]:
        return self.get("/v1/system/drift/latest")

    def model_evaluation(self) -> dict[str, Any]:
        return self.get("/v1/system/model/evaluation")

    def feedback(self, transaction_id: str) -> list[dict[str, Any]]:
        return self.get(f"/v1/feedback/{transaction_id}")

    def submit_feedback(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.post("/v1/feedback", payload)
