"""Measured benchmark suites for local inference and service dependencies."""

from __future__ import annotations

import json
import platform
import statistics
import sys
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

import httpx

from sentinelstream.config import AppSettings, load_settings
from sentinelstream.config.settings import SimulationSettings
from sentinelstream.data.schemas import TransactionEvent
from sentinelstream.database.redis_cache import RedisCache
from sentinelstream.features.behavioural import calculate_behavioural_features
from sentinelstream.models.hybrid import HybridFraudEngine, HybridSignals
from sentinelstream.simulation.generator import TransactionGenerator


@dataclass(frozen=True, slots=True)
class BenchmarkStats:
    """Summary statistics measured from one bounded workload."""

    samples: int
    minimum_seconds: float
    mean_seconds: float
    median_seconds: float
    p95_seconds: float
    p99_seconds: float
    throughput_per_second: float


@dataclass(frozen=True, slots=True)
class BenchmarkReport:
    """Self-describing benchmark output suitable for versioned artifacts."""

    suite: str
    created_at: str
    environment: dict[str, str]
    configuration: dict[str, Any]
    workload: dict[str, Any]
    statistics: BenchmarkStats
    samples_seconds: tuple[float, ...]
    observations: dict[str, Any]


def summarize_samples(
    samples: Sequence[float], *, wall_seconds: float | None = None
) -> BenchmarkStats:
    if not samples:
        raise ValueError("at least one benchmark sample is required")
    ordered = sorted(float(sample) for sample in samples)

    def percentile(fraction: float) -> float:
        index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
        return ordered[index]

    elapsed = wall_seconds or sum(ordered)
    return BenchmarkStats(
        samples=len(ordered),
        minimum_seconds=ordered[0],
        mean_seconds=statistics.fmean(ordered),
        median_seconds=statistics.median(ordered),
        p95_seconds=percentile(0.95),
        p99_seconds=percentile(0.99),
        throughput_per_second=len(ordered) / max(elapsed, 1e-12),
    )


def _environment() -> dict[str, str]:
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
    }


def _measure(
    operation: Callable[[], Any],
    *,
    iterations: int,
    warmup_iterations: int,
    concurrent_workers: int = 1,
) -> tuple[list[float], float, list[Any]]:
    for _ in range(warmup_iterations):
        operation()
    started = perf_counter()
    if concurrent_workers <= 1:
        observations = []
        samples = []
        for _ in range(iterations):
            sample_started = perf_counter()
            observations.append(operation())
            samples.append(perf_counter() - sample_started)
    else:

        def timed_operation() -> tuple[float, Any]:
            sample_started = perf_counter()
            result = operation()
            return perf_counter() - sample_started, result

        with ThreadPoolExecutor(max_workers=concurrent_workers) as executor:
            futures = [executor.submit(timed_operation) for _ in range(iterations)]
            observations = []
            samples = []
            for future in futures:
                sample, observation = future.result()
                samples.append(sample)
                observations.append(observation)
    return samples, perf_counter() - started, observations


def run_inference_benchmark(
    *,
    iterations: int = 100,
    warmup_iterations: int = 10,
    random_seed: int = 42,
) -> BenchmarkReport:
    """Measure feature preparation plus hybrid decision latency."""

    if iterations < 1 or warmup_iterations < 0:
        raise ValueError("benchmark iteration counts are invalid")
    record = TransactionGenerator(
        SimulationSettings(
            customer_count=1,
            merchant_count=1,
            random_seed=random_seed,
            fraud_ratio=0.0,
        )
    ).generate(1)[0]
    event = (
        TransactionEvent.model_validate(record["event"])
        if isinstance(record, Mapping)
        else record.event
    )
    engine = HybridFraudEngine()
    signals = HybridSignals(
        supervised_probability=0.35,
        anomaly_score=0.20,
        rule_score=0.10,
        model_version="benchmark",
        calibration_version="benchmark",
    )

    def operation() -> tuple[dict[str, float], float]:
        features = calculate_behavioural_features(event, [])
        result = engine.score(signals)
        return features, result.final_score

    samples, wall_seconds, _ = _measure(
        operation,
        iterations=iterations,
        warmup_iterations=warmup_iterations,
    )
    return BenchmarkReport(
        suite="inference",
        created_at=datetime.now(UTC).isoformat(),
        environment=_environment(),
        configuration={
            "iterations": iterations,
            "warmup_iterations": warmup_iterations,
            "random_seed": random_seed,
        },
        workload={"operation": "transaction feature preparation and hybrid score fusion"},
        statistics=summarize_samples(samples, wall_seconds=wall_seconds),
        samples_seconds=tuple(samples),
        observations={"event_type": type(event).__name__},
    )


def run_api_benchmark(
    *,
    url: str,
    iterations: int = 20,
    warmup_iterations: int = 2,
    concurrency: int = 1,
    timeout_seconds: float = 5.0,
    token: str | None = None,
) -> BenchmarkReport:
    """Measure an already-running API endpoint without storing credentials."""

    if not url.startswith(("http://", "https://")):
        raise ValueError("API benchmark URL must use HTTP or HTTPS")
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    def operation() -> int:
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.get(url, headers=headers)
        response.raise_for_status()
        return response.status_code

    samples, wall_seconds, observations = _measure(
        operation,
        iterations=iterations,
        warmup_iterations=warmup_iterations,
        concurrent_workers=concurrency,
    )
    return BenchmarkReport(
        suite="api",
        created_at=datetime.now(UTC).isoformat(),
        environment=_environment(),
        configuration={
            "iterations": iterations,
            "warmup_iterations": warmup_iterations,
            "concurrency": concurrency,
            "timeout_seconds": timeout_seconds,
        },
        workload={"url": url, "authentication": bool(token)},
        statistics=summarize_samples(samples, wall_seconds=wall_seconds),
        samples_seconds=tuple(samples),
        observations={"status_codes": sorted({int(status) for status in observations})},
    )


def run_redis_benchmark(
    settings: AppSettings | None = None,
    *,
    iterations: int = 100,
    warmup_iterations: int = 10,
) -> BenchmarkReport:
    """Measure cache writes and reads, including the configured fallback path."""

    runtime = settings or load_settings()
    cache = RedisCache(runtime.redis)
    counter = 0

    def operation() -> bool:
        nonlocal counter
        counter += 1
        key = f"benchmark:{counter}"
        cache.set_json(key, {"value": counter}, ttl_seconds=60)
        return cache.get_json(key) == {"value": counter}

    samples, wall_seconds, observations = _measure(
        operation,
        iterations=iterations,
        warmup_iterations=warmup_iterations,
    )
    return BenchmarkReport(
        suite="redis",
        created_at=datetime.now(UTC).isoformat(),
        environment=_environment(),
        configuration={"iterations": iterations, "warmup_iterations": warmup_iterations},
        workload={
            "operation": "set_json/get_json",
            "backend": "redis" if cache.backend_available else "fallback",
        },
        statistics=summarize_samples(samples, wall_seconds=wall_seconds),
        samples_seconds=tuple(samples),
        observations={"successful_operations": sum(bool(item) for item in observations)},
    )


def write_report(report: BenchmarkReport, path: Path) -> None:
    """Write raw samples and measured metadata without fabricating summaries."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), indent=2, sort_keys=True), encoding="utf-8")
