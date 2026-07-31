# Phase 4 — Calibration and cost-aware decisioning

Phase 4 turns model scores into auditable serving decisions. It keeps model
training, calibration, threshold selection, and policy evaluation separate so
each boundary can be tested and versioned.

## Calibration

ProbabilityCalibrator supports Platt scaling and isotonic regression. It must
be fitted on a time-separated calibration partition; the test partition is
reserved for the final calibration report. The transform clips scores to
[0, 1], and calibration_report reports Brier score, expected calibration
error, predicted risk, and observed fraud rate.

## Financial threshold selection

CostModel makes the operating objective explicit:

- false-positive review cost;
- fraud amount missed, adjusted by an optional recovered-funds rate;
- customer friction and intervention cost.

optimize_threshold evaluates score thresholds against labelled amounts and can
enforce a maximum analyst-alert capacity. It returns the selected threshold,
expected cost, alert count, detected fraud amount, and missed fraud amount.
Thresholds should be selected on validation data and measured once on a
time-separated test partition.

## Serving policy

ThresholdPolicy provides global review/block thresholds and optional
segment-specific review thresholds. Segment thresholds fall back to the global
policy until the segment has enough observations.

make_decision returns:

- allow, review, or block;
- low, medium, high, or critical risk tier;
- calibrated probability and confidence;
- policy, model, and calibration versions;
- reason codes for low confidence and model/rule/anomaly disagreement.

Uncertain or materially disagreeing signals abstain to review. The policy does
not silently convert an abstention into an allow decision.

## Example

~~~python
from sentinelstream.models import (
    CostModel,
    PredictionSignals,
    ProbabilityCalibrator,
    ThresholdPolicy,
    make_decision,
    optimize_threshold,
)

calibrator = ProbabilityCalibrator(method="isotonic").fit(
    validation_scores,
    validation_labels,
)
test_probabilities = calibrator.transform(test_scores)
operating_point = optimize_threshold(
    validation_labels,
    calibrator.transform(validation_scores),
    validation_amounts,
    cost_model=CostModel(),
    max_alerts=250,
)
policy = ThresholdPolicy(review_threshold=operating_point.threshold)
decision = make_decision(
    PredictionSignals(
        calibrated_probability=float(test_probabilities[0]),
        model_version="baseline-v1",
        calibration_version=calibrator.version,
    ),
    policy,
)
~~~

The current policy object is process-local. Durable model registry metadata,
online serving, and distributed state are introduced in later phases.
