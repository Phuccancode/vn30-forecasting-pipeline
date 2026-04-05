import os
import logging
from time import perf_counter
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, input_file_name, regexp_extract, upper
from pyspark.sql import functions as F
# Load .env
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
    logger.info(f"START: {label}")
    return start


def finish_step(label: str, start_time: float) -> None:
    elapsed = perf_counter() - start_time
    logger.info(f"DONE: {label} ({elapsed:.2f}s)")


def get_spark_session():
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    t = step_timer("Create Spark session")
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("VN30 Stock Analysis")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.4.1")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    
    spark.conf.set(
        f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
        "SharedKey"
    )
    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        account_key
    )

    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    finish_step("Create Spark session", t)
    
    return spark

logger.info("Load environment and initialize pipeline")
spark = get_spark_session()
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
BASE_PATH_RAW = f"abfss://raw@{storage_account}.dfs.core.windows.net"
BASE_PATH_PROCESSED = f"abfss://processed@{storage_account}.dfs.core.windows.net/silver"
logger.info(f"Spark version: {spark.version}")
logger.info(f"Read path: {BASE_PATH_RAW}/")
logger.info(f"Write path: {BASE_PATH_PROCESSED}")

t = step_timer("Build raw read + select transform")
df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.parquet")
    # Keep trailing slash for ABFS root container paths.
    # This avoids a known hadoop-azure 3.3.x getFileStatus("/") issue on some runtimes (e.g. Colab).
    .parquet(
        f"{BASE_PATH_RAW}/"
    )
    .withColumn("event_time", F.expr("to_utc_timestamp(timestamp_micros(CAST(Time / 1000 AS BIGINT)), 'Asia/Ho_Chi_Minh')"))
    .withColumn("trade_date", F.to_date("event_time"))
    .select(
        col("Ticker").alias("ticker").cast("string"), 
        col("event_time").alias("event_time").cast("timestamp"),
        col("trade_date").alias("trade_date").cast("date"),
        col("Open").alias("open").cast("double"),
        col("High").alias("high").cast("double"),
        col("Low").alias("low").cast("double"),
        col("Close").alias("close").cast("double"),
        col("Volume").alias("volume").cast("long")
    )
)
finish_step("Build raw read + select transform", t)

# Fail fast if unexpected parquet schema is mixed into raw container.
t = step_timer("Validate required columns")
required_cols = {"ticker", "event_time", "trade_date", "open", "high", "low", "close", "volume"}
missing_cols = sorted(required_cols.difference(set(df.columns)))
if missing_cols:
    raise ValueError(f"Missing required columns in raw input: {missing_cols}")
finish_step("Validate required columns", t)

t = step_timer("Clean nulls and normalize volume")
df = (
    df
    .filter(F.col("ticker").isNotNull() & (F.length(F.trim(F.col("ticker"))) > 0))
    .dropna(subset=["ticker", "trade_date"]) 
    .withColumn("volume", F.coalesce(F.col("volume"), F.lit(0)))
)
finish_step("Clean nulls and normalize volume", t)
# 3B) FFill open/high/low/close theo ticker, theo thời gian tăng dần
w_ffill = (
    Window
    .partitionBy("ticker")
    .orderBy("trade_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)

t = step_timer("Forward-fill OHLC by ticker")
df_ffill = (
    df
    .withColumn("open",  F.last("open",  ignorenulls=True).over(w_ffill))
    .withColumn("high",  F.last("high",  ignorenulls=True).over(w_ffill))
    .withColumn("low",   F.last("low",   ignorenulls=True).over(w_ffill))
    .withColumn("close", F.last("close", ignorenulls=True).over(w_ffill))
)
finish_step("Forward-fill OHLC by ticker", t)

t = step_timer("Compute moving averages")
df_ffill = (
    df_ffill
    .withColumn("ma10", F.avg("close").over(Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-9, 0)))
    .withColumn("ma20", F.avg("close").over(Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-19, 0)))
    .withColumn("ma50", F.avg("close").over(Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-49, 0))) 
    .withColumn("ma200", F.avg("close").over(Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-199, 0)))
)
finish_step("Compute moving averages", t)
#xóa đầu tiên
t = step_timer("Drop rows with null OHLC after ffill")
df_ffill = df_ffill.dropna(subset=["open","high","low","close"])
finish_step("Drop rows with null OHLC after ffill", t)

output_path = f"{BASE_PATH_PROCESSED}"
t = step_timer("Write silver parquet")
(
    df_ffill
    .repartition(16, "ticker")
    .write
    .mode("overwrite")
    .partitionBy("ticker")
    .parquet(output_path)
)
finish_step("Write silver parquet", t)


t = step_timer("Show sample rows")
df_ffill.show(truncate=False)
finish_step("Show sample rows", t)

t = step_timer("Print schema")
df_ffill.printSchema()
finish_step("Print schema", t)

t = step_timer("Count rows")
print(f"Total rows: {df_ffill.count()}")
finish_step("Count rows", t)

logger.info("Pipeline completed successfully")
