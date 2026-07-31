#!/usr/bin/env python
"""Run the retrying transaction-to-validated Kafka worker."""

from sentinelstream.config.settings import load_settings
from sentinelstream.streaming.consumer import KafkaMessageConsumer
from sentinelstream.streaming.producer import KafkaMessageProducer
from sentinelstream.streaming.topics import TopicName
from sentinelstream.streaming.validator import TransactionValidator


def main() -> None:
    settings = load_settings()
    producer = KafkaMessageProducer(settings.kafka)
    consumer = KafkaMessageConsumer(
        TopicName.TRANSACTIONS,
        TransactionValidator(producer),
        settings=settings.kafka,
        dead_letter_producer=producer,
    )
    try:
        consumer.run()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
