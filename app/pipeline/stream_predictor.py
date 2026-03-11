from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from app.config.settings import settings
from app.utils.logging_utils import get_logger
from app.utils.schema import MARKET_SCHEMA

logger = get_logger(__name__)


def _build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", settings.spark_kafka_package)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run() -> None:
    model_file = settings.output_dir / "models" / "linear_regression.joblib"
    if not model_file.exists():
        raise FileNotFoundError(f"Model artifact not found at {model_file}. Run train_model first.")

    artifact = joblib.load(model_file)
    model = artifact["model"]
    features = artifact["features"]

    pred_path = settings.output_dir / "predictions"
    ckpt_path = settings.checkpoint_dir / "predictor"
    pred_path.mkdir(parents=True, exist_ok=True)
    ckpt_path.mkdir(parents=True, exist_ok=True)

    spark = _build_spark("market-stream-predictor")

    kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        kafka_raw.select(F.from_json(F.col("value").cast("string"), MARKET_SCHEMA).alias("json"))
        .select("json.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("hl_spread", F.col("high") - F.col("low"))
        .withColumn("oc_change", F.col("close") - F.col("open"))
    )

    def score_microbatch(batch_df, batch_id: int) -> None:
        pdf = batch_df.toPandas()
        if pdf.empty:
            return

        for col in features:
            if col not in pdf.columns:
                pdf[col] = 0.0

        pdf["pred_close"] = model.predict(pdf[features])
        pdf["abs_error"] = (pdf["pred_close"] - pdf["close"]).abs()
        pdf["batch_id"] = batch_id

        pdf.to_parquet(pred_path / f"batch_{batch_id:08d}.parquet", index=False)
        logger.info("stored prediction batch=%s rows=%s", batch_id, len(pdf))

    query = (
        parsed.writeStream.foreachBatch(score_microbatch)
        .outputMode("append")
        .option("checkpointLocation", str(ckpt_path))
        .start()
    )

    logger.info("stream predictor running")
    query.awaitTermination()


if __name__ == "__main__":
    run()
