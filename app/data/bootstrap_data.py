from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from app.config.settings import settings
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

FEATURES = ["open", "high", "low", "close", "volume", "hl_spread"]
TARGET = "next_close"


def _safe_read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        if path.is_file():
            if path.stat().st_size == 0:
                logger.warning("Skipping empty parquet file: %s", path)
                return pd.DataFrame()
            return pd.read_parquet(path)

        parquet_files = [p for p in path.rglob("*.parquet") if p.is_file() and p.stat().st_size > 0]
        if not parquet_files:
            logger.warning("No non-empty parquet files found under: %s", path)
            return pd.DataFrame()

        return pd.read_parquet(parquet_files)
    except Exception as exc:
        logger.warning("Could not read parquet from %s: %s", path, exc)
        return pd.DataFrame()


def _alpha_vantage_daily(symbol: str) -> pd.DataFrame:
    if not settings.alpha_api_key:
        return pd.DataFrame()

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": settings.alpha_api_key,
        "outputsize": "full",
    }
    try:
        response = requests.get("https://www.alphavantage.co/query", params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        logger.warning("Alpha Vantage request failed: %s", exc)
        return pd.DataFrame()

    key = "Time Series (Daily)"
    if key not in payload:
        logger.warning("Alpha Vantage daily endpoint returned: %s", payload)
        return pd.DataFrame()

    rows = []
    for day, values in payload[key].items():
        open_ = float(values["1. open"])
        high = float(values["2. high"])
        low = float(values["3. low"])
        close = float(values["4. close"])
        volume = float(values["5. volume"])
        rows.append(
            {
                "event_time": pd.Timestamp(day, tz="UTC"),
                "symbol": symbol,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "source": "alphavantage_daily",
            }
        )

    df = pd.DataFrame(rows).sort_values("event_time").reset_index(drop=True)
    return df


def _synthetic_history(symbol: str, periods: int = 365) -> pd.DataFrame:
    base = 100.0
    rows = []
    now = datetime.now(timezone.utc)
    for i in range(periods):
        ts = now - timedelta(days=periods - i)
        drift = 0.03 * i / 20.0
        seasonal = 2.2 * ((i % 30) / 30.0)
        noise = (i % 7) * 0.11
        close = max(1.0, base + drift + seasonal + noise)
        open_ = close - 0.4
        high = close + 0.7
        low = close - 0.8
        volume = 1000 + (i % 100) * 25
        rows.append(
            {
                "event_time": ts,
                "symbol": symbol,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": float(volume),
                "source": "synthetic_bootstrap",
            }
        )

    return pd.DataFrame(rows)


def get_bootstrap_dataset(min_rows: int = 60) -> pd.DataFrame:
    output_path = settings.output_dir / "bootstrap"
    output_path.mkdir(parents=True, exist_ok=True)
    cache_file = output_path / "historical_features.parquet"

    if cache_file.exists():
        cached = _safe_read_parquet(cache_file)
        if len(cached) >= min_rows:
            return cached

    df = _alpha_vantage_daily(settings.alpha_symbol)
    if df.empty:
        logger.warning("Could not load daily historical data from API, using synthetic bootstrap.")
        df = _synthetic_history(settings.alpha_symbol, periods=max(365, min_rows))

    df["hl_spread"] = df["high"] - df["low"]

    df = df.dropna().copy()
    df.to_parquet(cache_file, index=False)
    logger.info("bootstrap dataset saved rows=%s path=%s", len(df), cache_file)
    return df


def ensure_training_dataset(silver_path: Path, min_rows: int = 30) -> pd.DataFrame:
    frames = []

    if silver_path.exists():
        silver = _safe_read_parquet(silver_path)
        if not silver.empty:
            frames.append(silver)

    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if len(merged) < min_rows:
        bootstrap = get_bootstrap_dataset(min_rows=max(120, min_rows))
        merged = pd.concat([merged, bootstrap], ignore_index=True) if not merged.empty else bootstrap

    if merged.empty:
        return pd.DataFrame(columns=FEATURES + [TARGET])

    sort_cols = [col for col in ["symbol", "event_time"] if col in merged.columns]
    if sort_cols:
        merged = merged.sort_values(sort_cols).reset_index(drop=True)

    if "symbol" in merged.columns:
        merged[TARGET] = merged.groupby("symbol")["close"].shift(-1)
    else:
        merged[TARGET] = merged["close"].shift(-1)

    cols = FEATURES + [TARGET]
    return merged[cols].dropna().reset_index(drop=True)
