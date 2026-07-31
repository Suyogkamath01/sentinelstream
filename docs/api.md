# API guide

FastAPI publishes interactive OpenAPI documentation at
`http://127.0.0.1:8000/docs` and the schema at `/openapi.json`.

## Authentication

Request a short-lived access token from the form endpoint:

```bash
curl -s -X POST http://127.0.0.1:8000/v1/auth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data 'username=analyst&password=sentinelstream-demo-password'
```

Use the returned bearer token in subsequent calls. Roles and permissions are
enforced in FastAPI dependencies, not just in Streamlit. The local demo
authenticator is for development only.

## Prediction

`POST /v1/predictions` accepts a `PredictionRequest` containing a typed
`TransactionEvent` and optional bounded model, anomaly, and rule scores. The
response contains `final_risk_score`, `confidence`, `risk_tier`, `action`,
source scores, reason codes, model/calibration/feature versions, and the
persisted message identifier. `GET /v1/predictions/{prediction_id}` retrieves
it; `/explanation` returns stored explanation evidence.

```json
{
  "event": {
    "transaction_id": "00000000-0000-0000-0000-000000000001",
    "event_id": "00000000-0000-0000-0000-000000000002",
    "customer_id": "customer-001",
    "account_id": "account-001",
    "card_id": "card-001",
    "merchant_id": "merchant-001",
    "merchant_category": "online_retail",
    "transaction_amount": 125.50,
    "currency": "USD",
    "transaction_type": "purchase",
    "channel": "web",
    "timestamp": "2025-01-01T12:00:00Z",
    "country": "US",
    "city": "New York",
    "latitude": 40.7,
    "longitude": -74.0,
    "device_id": "device-001",
    "ip_address": "192.0.2.10",
    "card_present": false,
    "authentication_method": "three_ds",
    "transaction_status": "approved",
    "event_version": 1,
    "ingestion_timestamp": "2025-01-01T12:00:01Z"
  },
  "supervised_probability": 0.72,
  "anomaly_score": 0.41,
  "rule_score": 0.60
}
```

## Alerts and feedback

- `GET /v1/alerts?status=open&page=1&page_size=50`
- `GET /v1/alerts/{alert_id}`
- `POST /v1/alerts/{alert_id}/acknowledge`
- `POST /v1/alerts/{alert_id}/resolve`
- `POST /v1/feedback`
- `GET /v1/feedback/{transaction_id}`

Feedback accepts `confirmed_fraud`, `not_fraud`, `escalated`, or `no_action`,
with an optional note up to 2,000 characters.

## History and operations

- `GET /v1/history/customers/{customer_id}`
- `GET /v1/history/merchants/{merchant_id}`
- `GET /v1/history/transactions?q=...`
- `GET /v1/dashboard/transactions`
- `GET /health`, `/ready`, and `/live`
- `GET /metrics` for bounded JSON operational metrics
- `GET /prometheus/metrics` for Prometheus exposition

Pagination is bounded by validation (`page >= 1`, `1 <= page_size <= 200`). API
errors avoid raw SQL, credentials, tokens, and request bodies. Sensitive
identifiers should be masked at presentation boundaries; authorised backend
access is still required for history queries.
