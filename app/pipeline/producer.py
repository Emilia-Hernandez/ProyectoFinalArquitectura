from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from typing import Dict, Iterator

from kafka import KafkaProducer

from app.config.settings import settings
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)


def _build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=settings.kafka_client_id,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        linger_ms=50,
    )


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
    producer = _build_producer()
    logger.info(
        "Producer started mode=simulate-only topic=%s bootstrap=%s",
        settings.kafka_topic,
        settings.kafka_bootstrap_servers,
    )
    for event in _simulate_stream(settings.alpha_symbol, settings.producer_rate_per_second):
        producer.send(settings.kafka_topic, event)


if __name__ == "__main__":
    run()
