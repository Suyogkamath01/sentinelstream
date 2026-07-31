# Research experiments

`sentinelstream.research` provides a reproducible evaluation layer rather than
a checked-in claim about model quality. It uses a fixed temporal split, shared
batch features, configured model names, dataset manifests, deterministic seeds,
configurable score-source ablations, calibration/threshold tables, fraud-cost
metrics, bootstrap intervals, segment filters, bounded error analysis, and
optional MLflow tracking.

## Run an experiment

```bash
make generate-sample
uv run python scripts/run_research.py \
  --input data/samples/transactions.parquet \
  --experiment-id baseline-comparison \
  --dataset-version sample-v1 \
  --models logistic_regression,rules,isolation_forest \
  --output reports/research
```

The standard `make install` command includes the optional plotting dependency.
If selecting groups manually, include all application groups as well as
`research` and omit only `ml` on platforms where its native dependencies are
not available:

```bash
uv sync --group dev --group spark --group database --group api \
  --group dashboard --group mlops --group monitoring --group security \
  --group research --no-group ml
```

To log actual outputs to MLflow, provide a tracking URI and opt in:

```bash
uv run python scripts/run_research.py \
  --input data/samples/transactions.parquet \
  --experiment-id mlflow-run \
  --tracking-uri http://127.0.0.1:5000 \
  --log-mlflow
```

For a local MLflow 3.x backend use a SQLite tracking URI such as
`sqlite:///mlflow.db` or the Compose HTTP server. The filesystem tracking
backend is retained only for older compatible MLflow installations because
newer MLflow versions place it in maintenance mode.

Outputs include configuration, metadata, dataset manifest, model metrics,
ablation metrics, threshold tables, reliability tables, masked error tables,
segment tables, a Markdown report, and optional plots. Generated results under `reports/research/` are
ignored by Git. No result, run ID, conclusion, or benchmark number is valid
until the command is actually executed against a named dataset.

The default ablation catalog includes supervised-only, anomaly-only,
rules-only, supervised-plus-anomaly, supervised-plus-rules, full hybrid, and
profile/behavioural exclusions. A combination is recorded as skipped when its
required score sources are unavailable; no missing source is replaced with a
fabricated value. Statistical helpers include bootstrap intervals and
threshold/reliability analysis. Small segments are omitted rather than
presented as stable findings. Financial conclusions require real, documented
cost assumptions.
