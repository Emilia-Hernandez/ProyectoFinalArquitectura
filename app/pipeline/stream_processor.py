from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from app.config.settings import settings
from app.utils.logging_utils import get_logger
from app.utils.schema import MARKET_SCHEMA

logger = get_logger(__name__)


class _ProgressLogger(StreamingQueryListener):
    def __init__(self, output_dir: Path) -> None:
        super().__init__()
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def onQueryStarted(self, event) -> None:
        logger.info("stream query started id=%s runId=%s", event.id, event.runId)

    def onQueryProgress(self, event) -> None:
        progress = json.loads(event.progress.json)
        query_name = progress.get("name") or "unnamed_query"
        metrics_file = self.output_dir / f"{query_name}.json"
        metrics_file.write_text(json.dumps(progress, indent=2), encoding="utf-8")

    def onQueryTerminated(self, event) -> None:
        logger.info("stream query terminated id=%s runId=%s", event.id, event.runId)


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
    logger.info("using spark kafka package: %s", settings.spark_kafka_package)
    spark.streams.addListener(_ProgressLogger(output_root / "logs" / "stream_progress"))

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

    def write_stats_snapshot(batch_df, batch_id: int) -> None:
        try:
            rows = [row.asDict(recursive=True) for row in batch_df.collect()]
            stats_dir = output_root / "stats"
            stats_file = stats_dir / "latest.parquet"
            temp_file = stats_dir / f".latest.batch_{batch_id}.parquet"

            if not rows:
                logger.info("stats batch=%s empty; skipping snapshot write", batch_id)
                return

            pdf = pd.DataFrame(rows)
            pdf = pdf.sort_values(["window_start", "symbol"]).reset_index(drop=True)
            pdf.to_parquet(temp_file, index=False)
            temp_file.replace(stats_file)
            logger.info("stored stats snapshot batch=%s rows=%s file=%s", batch_id, len(pdf), stats_file)
        except Exception as exc:
            logger.exception("failed storing stats snapshot batch=%s error=%s", batch_id, exc)

    bronze_query = (
        parsed.writeStream.format("parquet")
        .queryName("bronze_sink")
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
        .queryName("silver_sink")
        .outputMode("append")
        .option("path", str(output_root / "silver"))
        .option("checkpointLocation", str(checkpoint_root / "processor" / "silver"))
        .start()
    )

    stats_checkpoint = checkpoint_root / "processor" / "stats_snapshot"
    if stats_checkpoint.exists():
        shutil.rmtree(stats_checkpoint, ignore_errors=True)
    stats_checkpoint.mkdir(parents=True, exist_ok=True)
    latest_stats_file = output_root / "stats" / "latest.parquet"
    if latest_stats_file.exists():
        latest_stats_file.unlink()

    # Remove legacy Spark parquet sink artifacts so the dashboard only sees the fresh snapshot file.
    for stale_path in (output_root / "stats").iterdir():
        if stale_path.name != latest_stats_file.name:
            if stale_path.is_dir():
                shutil.rmtree(stale_path, ignore_errors=True)
            else:
                stale_path.unlink(missing_ok=True)

    stats_query = (
        stats.writeStream.queryName("stats_snapshot").foreachBatch(write_stats_snapshot)
        .outputMode("complete")
        .option("checkpointLocation", str(stats_checkpoint))
        .start()
    )

    console_query = (
        stats.writeStream.queryName("stats_console").outputMode("update")
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
