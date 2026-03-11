from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

MARKET_SCHEMA = StructType(
    [
        StructField("event_time", TimestampType(), False),
        StructField("symbol", StringType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("source", StringType(), False),
    ]
)
