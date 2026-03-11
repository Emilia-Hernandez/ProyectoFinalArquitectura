from __future__ import annotations

from pathlib import Path

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
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run() -> None:
    output_root = settings.output_dir
    checkpoint_root = settings.checkpoint_dir

    for path in [output_root / "bronze", output_root / "silver", output_root / "stats", checkpoint_root / "processor"]:
        Path(path).mkdir(parents=True, exist_ok=True)

    spark = _build_spark("market-stream-processor")

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
        .withWatermark("event_time", "2 minutes")
    )

    stats = (
        parsed.groupBy(
            F.window("event_time", f"{settings.stream_window_seconds} seconds", f"{settings.stream_slide_seconds} seconds"),
            F.col("symbol"),
        )
        .agg(
            F.min("close").alias("min_close"),
            F.max("close").alias("max_close"),
            F.avg("close").alias("avg_close"),
            F.variance("close").alias("var_close"),
            F.avg("volume").alias("avg_volume"),
            F.count("*").alias("n_obs"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            "min_close",
            "max_close",
            "avg_close",
            "var_close",
            "avg_volume",
            "n_obs",
        )
    )

    bronze_query = (
        parsed.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(output_root / "bronze"))
        .option("checkpointLocation", str(checkpoint_root / "processor" / "bronze"))
        .start()
    )

    silver = (
        parsed.withColumn("range_bucket", F.when(F.col("close") < 80, "low").when(F.col("close") < 120, "mid").otherwise("high"))
        .withColumn("hl_spread", F.col("high") - F.col("low"))
        .withColumn("oc_change", F.col("close") - F.col("open"))
    )

    silver_query = (
        silver.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(output_root / "silver"))
        .option("checkpointLocation", str(checkpoint_root / "processor" / "silver"))
        .start()
    )

    stats_query = (
        stats.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(output_root / "stats"))
        .option("checkpointLocation", str(checkpoint_root / "processor" / "stats"))
        .start()
    )

    console_query = (
        stats.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .start()
    )

    logger.info("stream processor running; Spark UI expected at http://localhost:4040")

    spark.streams.awaitAnyTermination()

    for query in [bronze_query, silver_query, stats_query, console_query]:
        query.stop()


if __name__ == "__main__":
    run()
