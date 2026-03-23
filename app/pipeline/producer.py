from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from typing import Dict, Iterator

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from app.config.settings import settings
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)


def _build_producer() -> KafkaProducer:
    last_error: NoBrokersAvailable | None = None
    for attempt in range(1, 16):
        try:
            producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                client_id=settings.kafka_client_id,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                linger_ms=50,
                acks="all",
            )
            if attempt > 1:
                logger.info("Kafka broker ready after retries=%s", attempt - 1)
            return producer
        except NoBrokersAvailable as exc:
            last_error = exc
            logger.warning(
                "Kafka broker not ready yet bootstrap=%s attempt=%s/15; retrying in 2s",
                settings.kafka_bootstrap_servers,
                attempt,
            )
            try:
                time.sleep(2)
            except KeyboardInterrupt:
                logger.info("Producer interrupted while waiting for Kafka broker")
                raise

    raise NoBrokersAvailable(
        f"Could not connect to Kafka broker at {settings.kafka_bootstrap_servers} after 15 attempts"
    ) from last_error


def _simulate_stream(symbol: str, rate_per_second: int) -> Iterator[Dict]:
    price = 100.0
    while True:
        for _ in range(max(rate_per_second, 1)):
            shock = random.gauss(mu=0.0, sigma=0.35)
            drift = 0.02
            new_price = max(1.0, price + drift + shock)
            high = max(price, new_price) + random.random() * 0.2
            low = min(price, new_price) - random.random() * 0.2
            payload = {
                "event_time": datetime.now(timezone.utc).isoformat(),
                "symbol": symbol,
                "open": round(price, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(new_price, 4),
                "volume": float(random.randint(100, 5000)),
                "source": "simulated",
            }
            price = new_price
            yield payload
        time.sleep(1)

def run() -> None:
    try:
        producer = _build_producer()
    except KeyboardInterrupt:
        logger.info("Producer stopped before Kafka connection was established")
        return
    logger.info(
        "Producer started mode=simulate-only topic=%s bootstrap=%s target_rate=%s msg/s",
        settings.kafka_topic,
        settings.kafka_bootstrap_servers,
        settings.producer_rate_per_second,
    )
    sent_messages = 0
    try:
        batch_size = max(settings.producer_rate_per_second, 1)
        batch_started_at = time.perf_counter()
        for event in _simulate_stream(settings.alpha_symbol, settings.producer_rate_per_second):
            sent_messages += 1
            producer.send(settings.kafka_topic, event)
            if sent_messages % batch_size == 0:
                producer.flush()
                elapsed = time.perf_counter() - batch_started_at
                achieved_rate = batch_size / elapsed if elapsed > 0 else float(batch_size)
                logger.info(
                    "Published messages=%s topic=%s batch_size=%s elapsed=%.3fs achieved_rate=%.1f msg/s",
                    sent_messages,
                    settings.kafka_topic,
                    batch_size,
                    elapsed,
                    achieved_rate,
                )
                batch_started_at = time.perf_counter()
    except KeyboardInterrupt:
        logger.info("Producer interrupted; flushing pending messages=%s", sent_messages)
    except KafkaError:
        logger.exception("Kafka publish failed topic=%s", settings.kafka_topic)
        raise
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()
