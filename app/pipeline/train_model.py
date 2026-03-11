from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error

from app.config.settings import settings
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

FEATURES = ["open", "high", "low", "volume", "hl_spread", "oc_change"]
TARGET = "close"


def run() -> None:
    silver_path = settings.output_dir / "silver"
    model_path = settings.output_dir / "models"
    model_path.mkdir(parents=True, exist_ok=True)

    if not silver_path.exists():
        raise FileNotFoundError(f"Silver data does not exist at {silver_path}")

    df = pd.read_parquet(silver_path)
    if df.empty or len(df) < 50:
        raise ValueError("Not enough silver records to train model. Collect more streaming data.")

    data = df[FEATURES + [TARGET]].dropna().copy()
    if len(data) < 50:
        raise ValueError("Not enough clean records after dropping NA values.")

    split_idx = int(len(data) * 0.8)
    train = data.iloc[:split_idx]
    test = data.iloc[split_idx:]

    model = LinearRegression()
    model.fit(train[FEATURES], train[TARGET])

    preds = model.predict(test[FEATURES])
    mae = mean_absolute_error(test[TARGET], preds)
    rmse = np.sqrt(mean_squared_error(test[TARGET], preds))

    artifact = {
        "model": model,
        "features": FEATURES,
        "target": TARGET,
        "metrics": {"mae": float(mae), "rmse": float(rmse), "n_train": len(train), "n_test": len(test)},
    }

    output_file = model_path / "linear_regression.joblib"
    joblib.dump(artifact, output_file)
    logger.info("saved model to %s", output_file)
    logger.info("metrics: MAE=%.5f RMSE=%.5f", mae, rmse)


if __name__ == "__main__":
    run()
