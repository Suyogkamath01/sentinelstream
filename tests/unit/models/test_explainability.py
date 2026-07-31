import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression

from sentinelstream.models.explainability import (
    ExplainerUnavailableError,
    ExplanationStore,
    FeatureContribution,
    build_reason_codes,
    explain_global,
    explain_local,
    explanation_stability,
    generate_counterfactuals,
)


def training_data() -> tuple[pd.DataFrame, np.ndarray]:
    frame = pd.DataFrame(
        {
            "amount_z_score": [0.1, 3.5, 0.2, 4.2, 0.4, 5.0, 0.3, 2.8],
            "device_novelty": [0, 1, 0, 1, 0, 1, 0, 0],
            "recent_transaction_count_10m": [0, 6, 1, 8, 0, 7, 1, 2],
            "impossible_travel": [0, 1, 0, 1, 0, 1, 0, 0],
        }
    )
    labels = np.array([0, 1, 0, 1, 0, 1, 0, 0])
    return frame, labels


def fitted_model() -> tuple[LogisticRegression, pd.DataFrame, np.ndarray]:
    frame, labels = training_data()
    model = LogisticRegression(max_iter=500).fit(frame, labels)
    return model, frame, labels


def test_reason_codes_are_human_readable_and_ranked() -> None:
    contributions = (
        FeatureContribution("device_novelty", 1.0, 0.8, "ablation"),
        FeatureContribution("amount_z_score", 4.0, 0.6, "ablation"),
        FeatureContribution("impossible_travel", 1.0, 0.4, "ablation"),
    )

    reasons = build_reason_codes(contributions, max_reasons=2)

    assert [reason.code for reason in reasons] == [
        "first_transaction_from_device",
        "unusually_high_transaction_amount",
    ]
    assert all(reason.message[0].isupper() for reason in reasons)


def test_local_explanation_uses_fallback_and_persists(tmp_path: Path) -> None:
    model, frame, _ = fitted_model()
    explanation = explain_local(
        model,
        frame.iloc[1],
        background=frame,
        method="ablation",
        threshold=0.8,
    )

    assert explanation.version == "phase5-v1"
    assert explanation.method == "ablation"
    assert 0.0 <= explanation.predicted_probability <= 1.0
    assert explanation.contributions
    assert explanation.summary.startswith("Risk probability")
    store = ExplanationStore(tmp_path / "explanations.jsonl")
    store.append(explanation)
    rows = store.read()
    assert rows[0]["version"] == "phase5-v1"
    assert rows[0]["reason_codes"] == [reason.to_dict() for reason in explanation.reason_codes]


def test_auto_explanation_is_available_without_shap() -> None:
    model, frame, _ = fitted_model()

    explanation = explain_local(model, frame.iloc[1], background=frame, method="auto")

    assert explanation.method in {"ablation", "shap"}


def test_explicit_shap_reports_missing_optional_dependency() -> None:
    if importlib.util.find_spec("shap") is not None:
        pytest.skip("SHAP is installed in this environment")
    model, frame, _ = fitted_model()

    with pytest.raises(ExplainerUnavailableError):
        explain_local(model, frame.iloc[1], background=frame, method="shap")


def test_global_permutation_importance_and_stability() -> None:
    model, frame, labels = fitted_model()

    global_explanation = explain_global(
        model,
        frame,
        labels,
        method="permutation",
        n_repeats=2,
    )
    local = explain_local(model, frame.iloc[1], background=frame, method="ablation")

    assert global_explanation.version == "phase5-v1"
    assert global_explanation.method == "permutation"
    assert global_explanation.importances[0].rank == 1
    assert explanation_stability(local, local) == 1.0


def test_counterfactuals_only_report_lower_scores() -> None:
    model, frame, _ = fitted_model()
    current = frame.iloc[1]
    current_probability = float(model.predict_proba(current.to_frame().T)[:, 1][0])

    counterfactuals = generate_counterfactuals(model, current, frame)

    assert all(item.resulting_probability < current_probability for item in counterfactuals)
