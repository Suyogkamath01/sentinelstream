# Phase 5 — Explainability

Phase 5 adds versioned explanations without changing the point-in-time feature
contract or the serving decision policy.

## Local explanations

explain_local uses SHAP when the optional SHAP dependency is installed and the
estimator supports it. The default auto mode falls back to deterministic
feature ablation: each feature is replaced with its training-background
baseline and the resulting probability change is recorded.

Every local explanation contains:

- the explanation version and method;
- predicted probability and baseline value;
- ranked feature contributions and directions;
- concise fraud reason codes;
- a human-readable summary;
- prototype counterfactuals that cross the configured threshold when possible.

The ablation output is intentionally labelled as ablation rather than being
presented as SHAP. This keeps analyst evidence honest when a model-specific
SHAP explainer is unavailable.

## Global explanations

explain_global supports SHAP mean absolute importance and permutation
importance. Auto mode uses SHAP when available and otherwise uses
average-precision permutation importance on a bounded evaluation sample. The
sample, repeat count, method, and explanation version should be logged with
the model run.

## Reason codes

Reason codes map risk-increasing contributions and behavioural values to
stable, human-readable indicators such as:

- unusually high transaction amount;
- first transaction from this device;
- geographically impossible travel;
- excessive recent transaction velocity;
- new merchant or country;
- high prior payment-failure rate;
- rapid repeat transaction.

The mapping is deliberately conservative: a feature only receives a domain
reason when its value meets the relevant fraud-domain condition.

## Persistence and stability

ExplanationStore appends JSON Lines records for downstream alert and analyst
workflows. explanation_stability compares top-feature overlap and contribution
direction between repeated explanations. This provides a compact regression
signal for explanation drift without claiming that explanation stability proves
model correctness.

The current store is local-file based. Database-backed explanation persistence
and dashboard rendering are later phases.
