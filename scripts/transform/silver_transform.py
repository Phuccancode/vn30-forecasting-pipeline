import argparse
import logging
import os
from datetime import datetime, timedelta
from time import perf_counter

from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col

load_dotenv()


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("silver_transform")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver transform for VN30 medallion pipeline")
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Run mode. incremental writes only target-date partitions; full rebuilds all partitions.",
    )
    parser.add_argument(
        "--target-date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Target date (YYYY-MM-DD). Used for incremental partition output.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=45,
        help="Lookback window in days used in incremental mode to compute MA/ffill context.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional lower bound date (YYYY-MM-DD). Rows older than this date are excluded.",
    )
    return parser.parse_args()


def step_timer(label: str) -> float:
    start = perf_counter()
    logger.info("START: %s", label)
    return start


def finish_step(label: str, start_time: float) -> None:
    elapsed = perf_counter() - start_time
    logger.info("DONE: %s (%.2fs)", label, elapsed)


def daterange(start_date, end_date):
    cursor = start_date
    while cursor <= end_date:
        yield cursor
        cursor += timedelta(days=1)


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hadoop_conf)
    return fs.exists(jpath)


def build_raw_input_paths(
    spark: SparkSession,
    base_path_raw: str,
    mode: str,
    target_date,
    lookback_days: int,
) -> list[str]:
    if mode == "full":
        return [f"{base_path_raw}/"]

    lower_bound = target_date - timedelta(days=lookback_days)
    paths = []
    for d in daterange(lower_bound, target_date):
        p = f"{base_path_raw}/trade_date={d}"
        if path_exists(spark, p):
            paths.append(p)
    return paths


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
        f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
        "SharedKey",
    )
    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        account_key,
    )
    # Ensure Hadoop FileSystem API (used by path existence checks) sees the same credentials.
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set(
        f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
        "SharedKey",
    )
    hadoop_conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        account_key,
    )
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    finish_step("Create Spark session", t)
    return spark


def check_critical_rule(metrics: dict, mode: str) -> None:
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
    if metrics["total_rows"] == 0 and mode == "full":
        raise ValueError("Data quality check failed: No rows left after transformations")


def get_data_quality_metrics(df_ffill):
    agg_row = (
        df_ffill.agg(
            F.count("*").alias("total_rows"),
            F.sum(F.when(col("ticker").isNull(), 1).otherwise(0)).alias("ticker_null_count"),
            F.sum(F.when(col("event_time").isNull(), 1).otherwise(0)).alias("event_time_null_count"),
            F.sum(
                F.when(
                    col("open").isNull()
                    | F.isnan(col("open"))
                    | col("high").isNull()
                    | F.isnan(col("high"))
                    | col("low").isNull()
                    | F.isnan(col("low"))
                    | col("close").isNull()
                    | F.isnan(col("close")),
                    1,
                ).otherwise(0)
            ).alias("ohlc_null_count"),
            F.sum(
                F.when(
                    (col("open") < 0)
                    | (col("high") < 0)
                    | (col("low") < 0)
                    | (col("close") < 0),
                    1,
                ).otherwise(0)
            ).alias("invalid_price_count"),
            F.sum(
                F.when(
                    (col("high") < col("low"))
                    | (col("high") < col("open"))
                    | (col("high") < col("close"))
                    | (col("low") > col("open"))
                    | (col("low") > col("close")),
                    1,
                ).otherwise(0)
            ).alias("invalid_ohlc_count"),
            F.sum(F.when(col("volume") < 0, 1).otherwise(0)).alias("invalid_volume_count"),
        )
        .collect()[0]
        .asDict()
    )
    duplicate_key_count = (
        df_ffill.groupBy("ticker", "event_time").count().filter("count > 1").count()
    )
    agg_row["duplicate_key_count"] = duplicate_key_count
    return agg_row


def main() -> None:
    args = parse_args()
    logger.info(
        "Load environment and initialize pipeline | mode=%s | target_date=%s | lookback_days=%s | start_date=%s",
        args.mode,
        args.target_date,
        args.lookback_days,
        args.start_date,
    )
    spark = get_spark_session()

    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    base_path_raw = f"abfss://raw@{storage_account}.dfs.core.windows.net"
    base_path_processed = f"abfss://processed@{storage_account}.dfs.core.windows.net"
    silver_output_path = f"{base_path_processed}/silver"
    silver_quality_output_path = f"{base_path_processed}/silver_quality"

    logger.info("Spark version: %s", spark.version)
    logger.info("Read path: %s/", base_path_raw)
    logger.info("Write path: %s", silver_output_path)

    target_date = datetime.strptime(args.target_date, "%Y-%m-%d").date()
    start_date = (
        datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else None
    )
    if start_date and start_date > target_date:
        raise ValueError("--start-date must be <= --target-date")

    t = step_timer("Resolve raw input paths")
    raw_input_paths = build_raw_input_paths(
        spark=spark,
        base_path_raw=base_path_raw,
        mode=args.mode,
        target_date=target_date,
        lookback_days=args.lookback_days,
    )
    finish_step("Resolve raw input paths", t)
    logger.info("Raw input path count: %s", len(raw_input_paths))

    if not raw_input_paths:
        logger.warning("No raw partitions found for selected mode/date window. Skip silver run.")
        return

    t = step_timer("Build raw read + select transform")
    df = (
        spark.read.option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.parquet")
        .parquet(*raw_input_paths)
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

    t = step_timer("Apply mode-specific input filter")
    if args.mode == "incremental":
        lower_bound = target_date - timedelta(days=args.lookback_days)
        df = df.filter(
            (col("event_time") >= F.to_timestamp(F.lit(str(lower_bound))))
            & (col("event_time") < F.to_timestamp(F.lit(str(target_date + timedelta(days=1)))))
        )
        logger.info(
            "Incremental raw filter applied: [%s, %s)",
            lower_bound,
            target_date + timedelta(days=1),
        )
    if start_date:
        df = df.filter(col("event_time") >= F.to_timestamp(F.lit(str(start_date))))
        logger.info("Applied start-date lower bound filter: event_time >= %s", start_date)
    finish_step("Apply mode-specific input filter", t)

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

    df_ffill = df_ffill.withColumn("trade_date", F.to_date("event_time"))

    if args.mode == "incremental":
        t = step_timer("Restrict output rows to target-date partitions")
        df_output = df_ffill.filter(col("trade_date") == F.to_date(F.lit(str(target_date))))
        finish_step("Restrict output rows to target-date partitions", t)
    else:
        df_output = df_ffill

    metrics = get_data_quality_metrics(df_output)
    logger.info("Data quality metrics: %s", metrics)

    run_ts_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    t = step_timer("Write silver quality report")
    quality_metrics = [
        {
            **metrics,
            "run_ts_utc": run_ts_utc,
            "run_date": run_date,
            "mode": args.mode,
            "target_date": str(target_date),
            "start_date": str(start_date) if start_date else None,
        }
    ]
    (
        spark.createDataFrame(quality_metrics)
        .write.mode("append")
        .partitionBy("run_date")
        .parquet(silver_quality_output_path)
    )
    finish_step("Write silver quality report", t)
    logger.info("Silver quality report path: %s", silver_quality_output_path)

    if metrics["total_rows"] == 0 and args.mode == "incremental":
        logger.warning(
            "No rows found for target_date=%s in incremental mode. Skip silver write.",
            target_date,
        )
        return

    check_critical_rule(metrics, args.mode)

    t = step_timer("Write silver parquet")
    writer = (
        df_output.repartition(4, "trade_date", "ticker")
        .write.mode("overwrite")
        .partitionBy("trade_date", "ticker")
    )
    if args.mode == "incremental":
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        writer.parquet(silver_output_path)
    else:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        # In full mode we overwrite the whole Silver dataset to avoid keeping stale old partitions.
        writer.parquet(silver_output_path)
    finish_step("Write silver parquet", t)

    t = step_timer("Count rows")
    print(f"Total rows: {metrics['total_rows']}")
    finish_step("Count rows", t)
    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()
