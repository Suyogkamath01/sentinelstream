# Model card

## Purpose

SentinelStream estimates transaction fraud risk to support allow, review, and
block recommendations. It is a decision-support component, not an autonomous
legal, credit, or identity decision-maker.

## Intended and out-of-scope use

Intended use is investigation prioritisation and transaction-risk triage in the
configured serving environment. It is not validated for a particular bank,
jurisdiction, customer segment, or payment network without a separate
evaluation. It must not be used to deny services solely on its score.

## Data and features

Training can use validated historical or simulated events with a fraud label.
Features include transaction attributes, event-time behavioural windows,
customer/merchant/device profiles, anomaly signals, and transparent rules.
Synthetic labels are simulation metadata and are not serving inputs. See
[`docs/dataset_card.md`](dataset_card.md) and [`docs/features.md`](features.md).

## Modelling and evaluation

The repository includes logistic-regression and tree-based baselines, an
isolation-forest anomaly path, calibration utilities, cost-aware evaluation,
and the configurable hybrid engine. Evaluation uses a temporal train,
validation, and test split and reports precision, recall, PR-AUC, calibration,
and financial-cost measures when cost assumptions are supplied. No metric is
reported here because results depend on the dataset, version, split, and model
run actually executed.

## Decisioning and explainability

Thresholds and policy versions are configuration. The hybrid result includes
source scores, confidence, risk tier, action, and reason codes. SHAP or
permutation explanations are optional and have deterministic fallbacks when
optional explainability dependencies are unavailable.

## Monitoring and lifecycle

Model, calibration, feature-set, and dataset versions should be recorded in
MLflow and promotion reports. Prometheus operational metrics and drift reports
are investigation signals. Retraining requires a new temporal evaluation,
calibration review, cost review, compatibility check, and rollback plan.

## Limitations and ethics

Simulated or historical fraud labels may not represent current fraud. Class
imbalance, concept drift, proxy variables, geographic effects, and selective
feedback can create unequal error rates. Operators must inspect false positives,
false negatives, segment sample sizes, and data-quality changes before changing
thresholds. Sensitive identifiers are masked in logs and reports where
possible, but deployment owners remain responsible for retention and access
controls.
