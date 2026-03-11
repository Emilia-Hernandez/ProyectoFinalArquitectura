from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    alpha_api_key: str = os.getenv("ALPHAVANTAGE_API_KEY", "")
    alpha_symbol: str = os.getenv("ALPHAVANTAGE_SYMBOL", "IBM")
    alpha_interval: str = os.getenv("ALPHAVANTAGE_INTERVAL", "1min")

    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "market_ticks")
    kafka_client_id: str = os.getenv("KAFKA_CLIENT_ID", "alpha-producer")

    stream_window_seconds: int = int(os.getenv("STREAM_WINDOW_SECONDS", "30"))
    stream_slide_seconds: int = int(os.getenv("STREAM_SLIDE_SECONDS", "10"))
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")

    producer_mode: str = os.getenv("PRODUCER_MODE", "simulate")
    producer_rate_per_second: int = int(os.getenv("PRODUCER_RATE_PER_SECOND", "30"))
    producer_poll_seconds: int = int(os.getenv("PRODUCER_POLL_SECONDS", "20"))

    output_dir: Path = Path(os.getenv("OUTPUT_DIR", "output"))
    checkpoint_dir: Path = Path(os.getenv("CHECKPOINT_DIR", "output/checkpoints"))


settings = Settings()
