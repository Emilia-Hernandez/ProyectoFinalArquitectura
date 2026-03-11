from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from typing import Dict, Iterator, List

import requests
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


def _fetch_alphavantage(symbol: str, interval: str) -> List[Dict]:
    if not settings.alpha_api_key:
        logger.warning("No ALPHAVANTAGE_API_KEY configured. Falling back to simulation.")
        return []

    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "apikey": settings.alpha_api_key,
        "outputsize": "compact",
    }
    response = requests.get("https://www.alphavantage.co/query", params=params, timeout=20)
    response.raise_for_status()
    data = response.json()

    key = f"Time Series ({interval})"
    if key not in data:
        logger.warning("Alpha Vantage did not return expected time series key: %s", data)
        return []

    rows = []
    for ts, values in data[key].items():
        event_time = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).isoformat()
        rows.append(
            {
                "event_time": event_time,
                "symbol": symbol,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": float(values["5. volume"]),
                "source": "alphavantage",
            }
        )

    rows.sort(key=lambda x: x["event_time"])
    return rows


def run() -> None:
    producer = _build_producer()
    logger.info(
        "Producer started mode=%s topic=%s bootstrap=%s",
        settings.producer_mode,
        settings.kafka_topic,
        settings.kafka_bootstrap_servers,
    )

    if settings.producer_mode == "live":
        while True:
            batch = _fetch_alphavantage(settings.alpha_symbol, settings.alpha_interval)
            if not batch:
                logger.info("No API data returned, emitting simulated events to keep pipeline active.")
                for event in _simulate_stream(settings.alpha_symbol, settings.producer_rate_per_second):
                    producer.send(settings.kafka_topic, event)
                    logger.info("sent simulated event ts=%s close=%.4f", event["event_time"], event["close"])
                    break
            else:
                for event in batch[-30:]:
                    producer.send(settings.kafka_topic, event)
                producer.flush()
                logger.info("sent %s live events from Alpha Vantage", len(batch[-30:]))
            time.sleep(settings.producer_poll_seconds)
    else:
        for event in _simulate_stream(settings.alpha_symbol, settings.producer_rate_per_second):
            producer.send(settings.kafka_topic, event)


if __name__ == "__main__":
    run()
