import os
import logging
from datetime import datetime
from time import perf_counter
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("silver_transform")


def step_timer(label: str):
    start = perf_counter()
    logger.info("START: %s", label)
    return start


def finish_step(label: str, start_time: float) -> None:
    elapsed = perf_counter() - start_time
    logger.info("DONE: %s (%.2fs)", label, elapsed)


def get_spark_session() -> SparkSession:
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    t = step_timer("Create Spark session")
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("VN30 Stock Analysis")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.4.1")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .getOrCreate()
    )
    spark.conf.set(
        f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SharedKey"
    )
    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", account_key
    )
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    finish_step("Create Spark session", t)
    return spark


def check_critical_rule(metrics: dict) -> None:
    if metrics["invalid_price_count"] > 0:
        raise ValueError(
            f"Data quality check failed: {metrics['invalid_price_count']} rows with negative price values"
        )
    if metrics["invalid_ohlc_count"] > 0:
        raise ValueError(
            f"Data quality check failed: {metrics['invalid_ohlc_count']} rows with invalid OHLC values"
        )
    if metrics["invalid_volume_count"] > 0:
        raise ValueError(
            f"Data quality check failed: {metrics['invalid_volume_count']} rows with negative volume values"
        )
    if metrics["duplicate_key_count"] > 0:
        raise ValueError(
            f"Data quality check failed: {metrics['duplicate_key_count']} duplicate rows by (ticker, event_time)"
        )
    if metrics["ticker_null_count"] > 0 or metrics["event_time_null_count"] > 0:
        raise ValueError(
            "Data quality check failed: Found "
            f"{metrics['ticker_null_count']} rows with null ticker and "
            f"{metrics['event_time_null_count']} rows with null event_time"
        )
    if metrics["ohlc_null_count"] > 0:
        raise ValueError(
            f"Data quality check failed: Found {metrics['ohlc_null_count']} rows with null OHLC values after ffill"
        )
    if metrics["total_rows"] == 0:
        raise ValueError("Data quality check failed: No rows left after transformations")

def get_data_quality_metrics(df_ffill):
    metrics = {
        "total_rows": df_ffill.count(),
        "ticker_null_count": df_ffill.filter(
            col("ticker").isNull() | col("ticker").isNaN()
        ).count(),
        "event_time_null_count": df_ffill.filter(
            col("event_time").isNull() | col("event_time").isNaN()
        ).count(),
        "ohlc_null_count": df_ffill.filter(
            col("open").isNull()
            | col("open").isNaN()
            | col("high").isNull()
            | col("high").isNaN()
            | col("low").isNull()
            | col("low").isNaN()
            | col("close").isNull()
            | col("close").isNaN()
        ).count(),
        "duplicate_key_count": df_ffill.groupBy("ticker", "event_time")
        .count()
        .filter("count > 1")
        .count(),
        "invalid_price_count": df_ffill.filter(
            (col("open") < 0)
            | (col("high") < 0)
            | (col("low") < 0)
            | (col("close") < 0)
        ).count(),
        "invalid_ohlc_count": df_ffill.filter(
            (col("high") < col("low"))
            | (col("high") < col("open"))
            | (col("high") < col("close"))
            | (col("low") > col("open"))
            | (col("low") > col("close"))
        ).count(),
        "invalid_volume_count": df_ffill.filter(col("volume") < 0).count(),
    }
    
    return metrics

def main() -> None:
    logger.info("Load environment and initialize pipeline")
    spark = get_spark_session()

    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    base_path_raw = f"abfss://raw@{storage_account}.dfs.core.windows.net"
    base_path_processed = f"abfss://processed@{storage_account}.dfs.core.windows.net"
    silver_output_path = f"{base_path_processed}/silver"
    silver_quality_output_path = f"{base_path_processed}/silver_quality"

    logger.info("Spark version: %s", spark.version)
    logger.info("Read path: %s/", base_path_raw)
    logger.info("Write path: %s", silver_output_path)

    t = step_timer("Build raw read + select transform")
    df = (
        spark.read.option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.parquet")
        .parquet(f"{base_path_raw}/")
        .withColumn(
            "event_time",
            F.expr(
                "to_utc_timestamp(timestamp_micros(CAST(Time / 1000 AS BIGINT)), 'Asia/Ho_Chi_Minh')"
            ),
        )
        .select(
            col("Ticker").alias("ticker").cast("string"),
            col("event_time").alias("event_time").cast("timestamp"),
            col("Open").alias("open").cast("double"),
            col("High").alias("high").cast("double"),
            col("Low").alias("low").cast("double"),
            col("Close").alias("close").cast("double"),
            col("Volume").alias("volume").cast("long"),
        )
    )
    finish_step("Build raw read + select transform", t)

    t = step_timer("Validate required columns")
    required_cols = {"ticker", "event_time", "open", "high", "low", "close", "volume"}
    missing_cols = sorted(required_cols.difference(set(df.columns)))
    if missing_cols:
        raise ValueError(f"Missing required columns in raw input: {missing_cols}")
    finish_step("Validate required columns", t)

    t = step_timer("Clean nulls and normalize volume")
    df = (
        df.filter(F.col("ticker").isNotNull() & (F.length(F.trim(F.col("ticker"))) > 0))
        .dropna(subset=["ticker", "event_time"])
        .withColumn("volume", F.coalesce(F.col("volume"), F.lit(0)))
    )
    finish_step("Clean nulls and normalize volume", t)

    w_ffill = (
        Window.partitionBy("ticker")
        .orderBy("event_time")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    t = step_timer("Forward-fill OHLC by ticker")
    df_ffill = (
        df.withColumn("open", F.last("open", ignorenulls=True).over(w_ffill))
        .withColumn("high", F.last("high", ignorenulls=True).over(w_ffill))
        .withColumn("low", F.last("low", ignorenulls=True).over(w_ffill))
        .withColumn("close", F.last("close", ignorenulls=True).over(w_ffill))
    )
    finish_step("Forward-fill OHLC by ticker", t)

    t = step_timer("Compute moving averages")
    df_ffill = (
        df_ffill.withColumn(
            "ma10",
            F.avg("close").over(
                Window.partitionBy("ticker").orderBy("event_time").rowsBetween(-9, 0)
            ),
        )
        .withColumn(
            "ma20",
            F.avg("close").over(
                Window.partitionBy("ticker").orderBy("event_time").rowsBetween(-19, 0)
            ),
        )
        .withColumn(
            "ma50",
            F.avg("close").over(
                Window.partitionBy("ticker").orderBy("event_time").rowsBetween(-49, 0)
            ),
        )
        .withColumn(
            "ma200",
            F.avg("close").over(
                Window.partitionBy("ticker").orderBy("event_time").rowsBetween(-199, 0)
            ),
        )
    )
    finish_step("Compute moving averages", t)

    t = step_timer("Drop rows with null OHLC after ffill")
    df_ffill = df_ffill.dropna(subset=["open", "high", "low", "close"])
    finish_step("Drop rows with null OHLC after ffill", t)

    metrics = get_data_quality_metrics(df_ffill)
    logger.info("Data quality metrics: %s", metrics)
    check_critical_rule(metrics)

    run_ts_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    t = step_timer("Write silver quality report")
    quality_metrics = [{**metrics, "run_ts_utc": run_ts_utc, "run_date": run_date}]
    (
        spark.createDataFrame(quality_metrics)
        .write.mode("append")
        .partitionBy("run_date")
        .parquet(silver_quality_output_path)
    )
    finish_step("Write silver quality report", t)
    logger.info("Silver quality report path: %s", silver_quality_output_path)

    t = step_timer("Write silver parquet")
    (
        df_ffill.repartition(16, "ticker")
        .write.mode("overwrite")
        .partitionBy("ticker")
        .parquet(silver_output_path)
    )
    finish_step("Write silver parquet", t)

    t = step_timer("Show sample rows")
    df_ffill.show(truncate=False)
    finish_step("Show sample rows", t)

    t = step_timer("Print schema")
    df_ffill.printSchema()
    finish_step("Print schema", t)

    t = step_timer("Count rows")
    print(f"Total rows: {metrics['total_rows']}")
    finish_step("Count rows", t)
    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()
