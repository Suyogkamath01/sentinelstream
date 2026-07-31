#!/usr/bin/env python
"""Publish deterministic serving-safe transactions to the Kafka ingress topic."""

from __future__ import annotations

import argparse

from sentinelstream.config.settings import load_settings
from sentinelstream.data.schemas import SimulatedTransaction
from sentinelstream.simulation.generator import TransactionGenerator
from sentinelstream.streaming.producer import KafkaMessageProducer
from sentinelstream.streaming.topics import TopicName


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=20, help="Number of events to publish")
    parser.add_argument(
        "--seed", type=int, default=None, help="Optional deterministic generator seed"
    )
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")

    settings = load_settings()
    simulation = settings.simulation
    if args.seed is not None:
        simulation = simulation.model_copy(update={"random_seed": args.seed})

    producer = KafkaMessageProducer(settings.kafka)
    published = 0
    try:
        for record in TransactionGenerator(simulation).generate(args.count):
            if not isinstance(record, SimulatedTransaction):
                continue
            producer.publish_transaction(record.event)
            published += 1
        producer.flush()
    finally:
        producer.close()
    print(f"published={published} topic={TopicName.TRANSACTIONS.value}")


if __name__ == "__main__":
    main()
