from __future__ import annotations

import pytest

from sentinelstream.performance.benchmarks import (
    run_inference_benchmark,
    run_redis_benchmark,
    summarize_samples,
)


@pytest.mark.performance
def test_summary_statistics_are_measured_from_samples() -> None:
    summary = summarize_samples([0.001, 0.002, 0.003], wall_seconds=0.006)

    assert summary.samples == 3
    assert summary.minimum_seconds == 0.001
    assert summary.median_seconds == 0.002
    assert summary.throughput_per_second == 500.0


@pytest.mark.performance
def test_inference_benchmark_is_bounded_and_reproducible_shape() -> None:
    report = run_inference_benchmark(iterations=3, warmup_iterations=1, random_seed=19)

    assert report.suite == "inference"
    assert report.statistics.samples == 3
    assert len(report.samples_seconds) == 3
    assert report.workload["operation"]


@pytest.mark.performance
def test_redis_benchmark_supports_local_fallback() -> None:
    report = run_redis_benchmark(iterations=2, warmup_iterations=0)

    assert report.suite == "redis"
    assert report.observations["successful_operations"] == 2
