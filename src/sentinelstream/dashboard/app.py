"""SentinelStream Streamlit dashboard entry point."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd
import streamlit as st

from sentinelstream.config.settings import AppSettings, load_settings
from sentinelstream.dashboard.client import DashboardAPIError, DashboardClient
from sentinelstream.dashboard.components import render_error, render_kpis, render_records
from sentinelstream.dashboard.transforms import (
    filter_transactions,
    format_amount,
    format_probability,
    health_label,
    mask_records_for_display,
    records_frame,
    sort_frame,
)


def _settings() -> AppSettings:
    return load_settings()


def _client(settings: AppSettings) -> DashboardClient:
    client = st.session_state.get("dashboard_client")
    if client is None or client.base_url != settings.dashboard.api_url:
        client = DashboardClient(
            settings.dashboard.api_url,
            timeout_seconds=settings.dashboard.request_timeout_seconds,
            verify_tls=settings.dashboard.verify_tls,
        )
        st.session_state.dashboard_client = client
    client.token = st.session_state.get("dashboard_token")
    return client


def _load(
    client: DashboardClient,
    operation: Callable[[], Any],
    fallback: Any,
    *,
    silent_status: frozenset[int] = frozenset(),
) -> Any:
    try:
        return operation()
    except DashboardAPIError as exc:
        if exc.status_code == 401:
            st.session_state.pop("dashboard_token", None)
        if exc.status_code not in silent_status:
            render_error(str(exc))
        return fallback


def _login(client: DashboardClient) -> bool:
    if client.token:
        return True
    st.title("SentinelStream")
    st.caption("Authenticated fraud operations dashboard")
    with st.form("login"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign in", type="primary")
    if submitted:
        try:
            client.authenticate(username, password)
            st.session_state.dashboard_token = client.token
            st.rerun()
        except DashboardAPIError as exc:
            render_error(str(exc))
    return False


def _overview(client: DashboardClient) -> None:
    st.header("Overview")
    metrics = _load(client, client.metrics, {})
    render_kpis(metrics)
    left, right = st.columns(2)
    with left:
        st.subheader("Risk-tier distribution")
        distribution = metrics.get("risk_tier_distribution", {})
        if distribution:
            st.bar_chart(pd.Series(distribution, name="predictions"))
        else:
            st.info("No predictions are available yet.")
    with right:
        st.subheader("Recent activity")
        render_records(_load(client, lambda: client.transactions(page_size=10), []), height=300)


def _live_transactions(client: DashboardClient, settings: AppSettings) -> None:
    st.header("Live transactions")
    st.caption(f"Refresh interval configured to {settings.dashboard.refresh_seconds:g} seconds.")
    if st.button("Refresh now"):
        st.rerun()
    records = _load(client, lambda: client.transactions(page_size=settings.dashboard.page_size), [])
    query, tier, status_filter, country = st.columns(4)
    filtered = filter_transactions(
        records,
        query=query.text_input("Search"),
        risk_tier=tier.selectbox("Risk tier", ["All", "low", "medium", "high", "critical"]),
        alert_status=status_filter.selectbox(
            "Alert status", ["All", "open", "acknowledged", "resolved"]
        ),
        country=country.selectbox(
            "Country",
            ["All"] + sorted({str(row.get("country")) for row in records if row.get("country")}),
        ),
    )
    sort_by = st.selectbox("Sort by", ["event_time", "transaction_amount", "final_risk_score"])
    render_records(sort_frame(filtered, sort_by), height=520)


def _alerts(client: DashboardClient) -> None:
    st.header("Fraud alerts")
    status_filter = st.selectbox("Status", ["All", "open", "acknowledged", "resolved"])
    records = _load(
        client, lambda: client.alerts(status=None if status_filter == "All" else status_filter), []
    )
    frame = records_frame(records)
    if not frame.empty:
        tier_filter = st.multiselect(
            "Risk tiers", sorted(frame["risk_tier"].dropna().unique()), default=[]
        )
        if tier_filter:
            frame = frame[frame["risk_tier"].isin(tier_filter)]
        render_records(frame.to_dict("records"))
        selected = st.selectbox(
            "Inspect alert", [str(value) for value in frame["alert_id"].tolist()]
        )
        detail = _load(client, lambda: client.alert(selected), {})
        st.json(mask_records_for_display([detail])[0] if detail else {})
        action_left, action_right = st.columns(2)
        if action_left.button("Acknowledge", disabled=detail.get("status") == "resolved"):
            _load(client, lambda: client.acknowledge_alert(selected), None)
            st.success("Alert acknowledged.")
            st.rerun()
        if action_right.button("Resolve", disabled=detail.get("status") == "resolved"):
            _load(client, lambda: client.resolve_alert(selected), None)
            st.success("Alert resolved.")
            st.rerun()
    else:
        st.info("No alerts match the selected status.")


def _investigation(client: DashboardClient, settings: AppSettings) -> None:
    st.header("Transaction investigation")
    records = _load(client, lambda: client.transactions(page_size=settings.dashboard.page_size), [])
    if not records:
        st.info("No transactions are available for investigation.")
        return
    selected = st.selectbox("Transaction", [row["transaction_id"] for row in records])
    row = next(row for row in records if row["transaction_id"] == selected)
    st.json(mask_records_for_display([row])[0])
    prediction_id = row.get("prediction_message_id")
    if not prediction_id:
        st.info("This transaction has not produced a prediction.")
        return
    prediction = _load(client, lambda: client.prediction(prediction_id), {})
    explanation = _load(client, lambda: client.explanation(prediction_id), {})
    first, second = st.columns(2)
    first.metric("Fraud probability", format_probability(prediction.get("model_probability")))
    second.metric("Final fused risk", format_probability(prediction.get("final_risk_score")))
    st.subheader("Decision evidence")
    st.write({"risk_tier": prediction.get("risk_tier"), "decision": prediction.get("action")})
    st.write("Reason codes", prediction.get("reason_codes", []))
    st.json({"source_scores": prediction.get("source_scores", {}), "explanation": explanation})


def _history(client: DashboardClient, entity: str) -> None:
    label = "Customer ID" if entity == "customer" else "Merchant ID"
    st.header(f"{label} history")
    entity_id = st.text_input(label)
    if not entity_id:
        st.info(f"Enter a {entity} identifier to view persisted history.")
        return
    profile = _load(
        client,
        lambda: (
            client.customer_profile(entity_id)
            if entity == "customer"
            else client.merchant_profile(entity_id)
        ),
        None,
    )
    if profile:
        st.subheader("Profile summary")
        st.json(mask_records_for_display([profile])[0])
    records = _load(
        client,
        lambda: (
            client.customer_history(entity_id)
            if entity == "customer"
            else client.merchant_history(entity_id)
        ),
        [],
    )
    render_records(records)
    if records:
        frame = records_frame(records)
        st.metric(
            "Average transaction value", format_amount(frame["transaction_amount"].mean(), "")
        )


def _search(client: DashboardClient, settings: AppSettings) -> None:
    st.header("Transaction search")
    query = st.text_input("Transaction, event, customer, or merchant ID")
    records = _load(client, lambda: client.transactions(page_size=settings.dashboard.page_size), [])
    filtered = filter_transactions(records, query=query)
    render_records(filtered.to_dict("records"))


def _performance(client: DashboardClient) -> None:
    st.header("Model performance")
    evaluation = _load(client, client.model_evaluation, None, silent_status=frozenset({404}))
    if not evaluation:
        st.info("No executed offline evaluation artifact is available yet.")
        return
    st.caption("Offline evaluation metrics from an executed labelled evaluation run.")
    render_records(evaluation.get("metrics", []), height=520)


def _drift(client: DashboardClient) -> None:
    st.header("Drift monitoring")
    report = _load(client, client.latest_drift, None, silent_status=frozenset({404}))
    if report:
        st.write(
            {
                key: report.get(key)
                for key in (
                    "report_id",
                    "generated_at",
                    "reference_version",
                    "current_version",
                    "model_version",
                    "drifted_features",
                )
            }
        )
        render_records(report.get("feature_metrics", []))
        st.caption("Drift is a signal for investigation, not proof of model failure.")
    else:
        st.info("No drift report has been generated yet.")


def _system_health(client: DashboardClient) -> None:
    st.header("System health")
    health = _load(client, client.health, {})
    st.metric("API health", health_label(health.get("status")))
    dependencies = _load(client, client.dependencies, {})
    components = dependencies.get("components", []) if dependencies else []
    if components:
        render_records(components)
    else:
        st.info("No dependency health report is available.")


def _feedback(client: DashboardClient) -> None:
    st.header("Analyst feedback")
    transaction_id = st.text_input("Transaction ID")
    if transaction_id:
        render_records(_load(client, lambda: client.feedback(transaction_id), []))
        with st.form("feedback"):
            event_id = st.text_input("Event ID")
            outcome = st.selectbox(
                "Outcome", ["confirmed_fraud", "not_fraud", "escalated", "no_action"]
            )
            note = st.text_area("Analyst note", max_chars=2_000)
            if st.form_submit_button("Submit feedback"):
                result = _load(
                    client,
                    lambda: client.submit_feedback(
                        {
                            "event_id": event_id,
                            "transaction_id": transaction_id,
                            "outcome": outcome,
                            "note": note or None,
                        }
                    ),
                    None,
                )
                if result is not None:
                    st.success("Feedback submitted.")


def main() -> None:
    settings = _settings()
    st.set_page_config(page_title="SentinelStream", page_icon="🛡️", layout="wide")
    client = _client(settings)
    if not _login(client):
        return
    st.sidebar.title("SentinelStream")
    page = st.sidebar.radio(
        "Navigate",
        [
            "Overview",
            "Live transactions",
            "Fraud alerts",
            "Investigation",
            "Customer history",
            "Merchant history",
            "Transaction search",
            "Model performance",
            "Drift monitoring",
            "System health",
            "Analyst feedback",
        ],
    )
    if st.sidebar.button("Sign out"):
        st.session_state.pop("dashboard_token", None)
        client.token = None
        st.rerun()
    pages: dict[str, Callable[[], None]] = {
        "Overview": lambda: _overview(client),
        "Live transactions": lambda: _live_transactions(client, settings),
        "Fraud alerts": lambda: _alerts(client),
        "Investigation": lambda: _investigation(client, settings),
        "Customer history": lambda: _history(client, "customer"),
        "Merchant history": lambda: _history(client, "merchant"),
        "Transaction search": lambda: _search(client, settings),
        "Model performance": lambda: _performance(client),
        "Drift monitoring": lambda: _drift(client),
        "System health": lambda: _system_health(client),
        "Analyst feedback": lambda: _feedback(client),
    }
    if page == "Live transactions" and st.sidebar.checkbox("Auto-refresh live feed", value=False):
        fragment = getattr(st, "fragment", None)
        if fragment is not None:
            render_live = fragment(run_every=settings.dashboard.refresh_seconds)(pages[page])
            render_live()
        else:
            pages[page]()
    else:
        pages[page]()


if __name__ == "__main__":
    main()
