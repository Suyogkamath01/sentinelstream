# Performance benchmarks

src/sentinelstream/performance/benchmarks.py contains bounded, reproducible
benchmark suites. Each report stores the environment, configuration, workload,
raw samples, and measured summary statistics. Generated reports should be
written under reports/benchmarks/, which is ignored by Git.

Inference and Redis fallback suites are local:

    uv run python scripts/benchmark.py \
      --suite inference --iterations 100 --warmup 10 \
      --output reports/benchmarks/inference.json
    uv run python scripts/benchmark.py \
      --suite redis --iterations 100 --warmup 10 \
      --output reports/benchmarks/redis.json

An API suite measures an already-running endpoint and does not write the
bearer token to the report:

    uv run python scripts/benchmark.py \
      --suite api --url http://127.0.0.1:8000/live \
      --iterations 50 --warmup 5 --concurrency 4 \
      --output reports/benchmarks/api-live.json

Kafka, Spark, and PostgreSQL end-to-end measurements require the relevant
services. Their external-service latency must be reported separately from
the local inference and fallback measurements; the default test suite never
starts those services or runs a heavy workload.
