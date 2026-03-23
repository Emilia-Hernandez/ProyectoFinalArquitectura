from __future__ import annotations

import joblib
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error

from app.config.settings import settings
from app.data.bootstrap_data import ensure_training_dataset
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

FEATURES = ["open", "high", "low", "close", "volume", "hl_spread"]
TARGET = "next_close"


def run() -> None:
    silver_path = settings.output_dir / "silver"
    model_path = settings.output_dir / "models"
    model_path.mkdir(parents=True, exist_ok=True)

    data = ensure_training_dataset(silver_path=silver_path, min_rows=30)
    if len(data) < 10:
        raise ValueError("Not enough records even after bootstrap fallback.")

    split_idx = int(len(data) * 0.8)
    if split_idx == 0:
        split_idx = max(1, len(data) - 1)
    if split_idx >= len(data):
        split_idx = len(data) - 1

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
    logger.info("dataset rows used=%s train=%s test=%s", len(data), len(train), len(test))
    logger.info("metrics: MAE=%.5f RMSE=%.5f", mae, rmse)


if __name__ == "__main__":
    run()
