import os
import signal
import sys

from influxdb_client import InfluxDBClient, Point, WritePrecision
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr, from_json, max as spark_max, max_by, min as spark_min, min_by, sum as spark_sum, window
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "trades")
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://bd:8086")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "ej3")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "market")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "ej3-influxdb-token")
WINDOW_SIZE = "1 minute"
WATERMARK_DELAY = "2 minutes"


TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("timestamp", LongType(), True),
        StructField("source", StringType(), True),
    ]
)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ohlc-processor")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )


def build_influx_client() -> InfluxDBClient:
    return InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)


def write_batch(batch_df, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return

    client = build_influx_client()
    write_api = client.write_api()

    points = []
    for row in rows:
        points.append(
            Point("ohlc")
            .tag("symbol", row["symbol"])
            .field("open", float(row["open"]))
            .field("high", float(row["high"]))
            .field("low", float(row["low"]))
            .field("close", float(row["close"]))
            .field("volume", float(row["volume"]))
            .field("trades", int(row["trades"]))
            .time(row["window_end"], WritePrecision.NS)
        )

    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
    client.close()
    print(f"[procesamiento] batch={batch_id} escritos={len(points)}", flush=True)


def main() -> None:
    spark = build_spark_session()
    query = None

    def handle_stop(_signum, _frame):
        if query is not None:
            query.stop()

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    trades = (
        raw_stream.select(from_json(col("value").cast("string"), TRADE_SCHEMA).alias("trade"))
        .select("trade.*")
        .filter(col("symbol").isNotNull())
        .withColumn("event_time", expr("timestamp_millis(timestamp)"))
    )

    candles = (
        trades.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(window(col("event_time"), WINDOW_SIZE).alias("window"), col("symbol"))
        .agg(
            min_by(col("price"), col("event_time")).alias("open"),
            max_by(col("price"), col("event_time")).alias("close"),
            spark_max(col("price")).alias("high"),
            spark_min(col("price")).alias("low"),
            spark_sum(col("volume")).alias("volume"),
            count(col("price")).alias("trades"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("trades"),
        )
    )

    print("[procesamiento] Iniciando Spark Structured Streaming", flush=True)
    query = (
        candles.writeStream.outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", "/tmp/ej3ibd-spark-checkpoint")
        .trigger(processingTime="10 seconds")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()

    sys.exit(0)


if __name__ == "__main__":
    main()