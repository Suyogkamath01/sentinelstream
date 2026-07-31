"""Reusable Streamlit presentation components."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from sentinelstream.dashboard.transforms import (
    format_probability,
    mask_records_for_display,
    metric_value,
)


def render_kpis(metrics: dict[str, Any]) -> None:
    columns = st.columns(5)
    values = (
        ("Transactions", metric_value(metrics, "transactions_created", 0)),
        ("Alerts", metric_value(metrics, "alerts_created", 0)),
        ("Unresolved", metric_value(metrics, "unresolved_alerts", 0)),
        ("Blocked", metric_value(metrics, "blocked_transactions", 0)),
        ("Average risk", format_probability(metrics.get("average_fraud_probability"))),
    )
    for column, (label, value) in zip(columns, values, strict=True):
        column.metric(label, value)


def render_records(records: list[dict[str, Any]], *, height: int = 360) -> None:
    if records:
        st.dataframe(
            pd.DataFrame(mask_records_for_display(records)),
            use_container_width=True,
            height=height,
        )
    else:
        st.info("No records are available for the selected filters.")


def render_error(message: str) -> None:
    st.error(message)
