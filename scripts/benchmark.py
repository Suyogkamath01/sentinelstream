#!/usr/bin/env python
"""CLI for bounded local performance measurements."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from sentinelstream.config import load_settings
from sentinelstream.performance.benchmarks import (
    run_api_benchmark,
    run_inference_benchmark,
    run_redis_benchmark,
    write_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("inference", "api", "redis"), default="inference")
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--warmup", type=int)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--url", help="running API URL for the api suite")
    parser.add_argument("--token", help="optional API bearer token; never written to output")
    parser.add_argument("--concurrency", type=int)
    args = parser.parse_args()
    settings = load_settings()
    iterations = args.iterations or settings.benchmark.iterations
    warmup = settings.benchmark.warmup_iterations if args.warmup is None else args.warmup

    if args.suite == "inference":
        report = run_inference_benchmark(
            iterations=iterations,
            warmup_iterations=warmup,
            random_seed=settings.benchmark.random_seed,
        )
    elif args.suite == "api":
        report = run_api_benchmark(
            url=args.url or settings.benchmark.api_url,
            iterations=iterations,
            warmup_iterations=warmup,
            concurrency=args.concurrency or settings.benchmark.concurrency,
            timeout_seconds=settings.benchmark.timeout_seconds,
            token=args.token,
        )
    else:
        report = run_redis_benchmark(
            settings,
            iterations=iterations,
            warmup_iterations=warmup,
        )

    if args.output:
        write_report(report, args.output)
    print(json.dumps(asdict(report), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
