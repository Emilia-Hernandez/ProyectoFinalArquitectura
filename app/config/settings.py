from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _detect_spark_home() -> Path | None:
    spark_home = os.getenv("SPARK_HOME")
    if not spark_home:
        return None
    path = Path(spark_home)
    return path if path.exists() else None


def _detect_spark_version(default: str = "3.5.1") -> str:
    env_version = os.getenv("SPARK_KAFKA_VERSION")
    if env_version:
        return env_version

    spark_home = _detect_spark_home()
    if not spark_home:
        return default

    release_file = spark_home / "RELEASE"
    if not release_file.exists():
        return default

    text = release_file.read_text(encoding="utf-8", errors="ignore")
    match = re.search(r"Spark\\s+([0-9]+\\.[0-9]+\\.[0-9]+)", text)
    return match.group(1) if match else default


def _detect_scala_binary(default: str = "2.12") -> str:
    env_scala = os.getenv("SPARK_SCALA_BINARY_VERSION")
    if env_scala:
        return env_scala

    spark_home = _detect_spark_home()
    if not spark_home:
        return default

    jars_dir = spark_home / "jars"
    if not jars_dir.exists():
        return default

    candidates = sorted(jars_dir.glob("scala-library-*.jar"))
    if not candidates:
        return default

    match = re.search(r"scala-library-([0-9]+\\.[0-9]+)\\.", candidates[0].name)
    return match.group(1) if match else default


def _resolve_spark_kafka_package() -> str:
    package_override = os.getenv("SPARK_KAFKA_PACKAGE")
    if package_override:
        return package_override

    spark_version = _detect_spark_version()
    scala_binary = _detect_scala_binary()
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala_binary}:{spark_version}"


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
    spark_kafka_package: str = field(default_factory=_resolve_spark_kafka_package)

    producer_mode: str = os.getenv("PRODUCER_MODE", "simulate")
    producer_rate_per_second: int = int(os.getenv("PRODUCER_RATE_PER_SECOND", "30"))
    producer_poll_seconds: int = int(os.getenv("PRODUCER_POLL_SECONDS", "20"))

    output_dir: Path = Path(os.getenv("OUTPUT_DIR", "output"))
    checkpoint_dir: Path = Path(os.getenv("CHECKPOINT_DIR", "output/checkpoints"))


settings = Settings()
