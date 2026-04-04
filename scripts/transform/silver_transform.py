import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, input_file_name, regexp_extract, upper
from pyspark.sql import functions as F
# Load .env
from dotenv import load_dotenv
load_dotenv()


def get_spark_session():
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("VN30 Stock Analysis")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.4.1")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
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
    
    return spark

spark = get_spark_session()
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
BASE_PATH_RAW = f"abfss://raw@{storage_account}.dfs.core.windows.net"
BASE_PATH_PROCESSED = f"abfss://processed@{storage_account}.dfs.core.windows.net"

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.parquet")
    .parquet(
        f"{BASE_PATH_RAW}"
    )
    .withColumn("_source_file", input_file_name())
    .withColumn(
        "Ticker",
        upper(regexp_extract("_source_file", r"/([^/]+)\.parquet$", 1))
    )
    .withColumn("event_time", F.expr("to_utc_timestamp(timestamp_micros(CAST(Time / 1000 AS BIGINT)), 'Asia/Ho_Chi_Minh')"))
    .withColumn("trade_date", F.to_date("event_time"))
    .drop("_source_file")
    .drop("Time")
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

# Fail fast if unexpected parquet schema is mixed into raw container.
required_cols = {"ticker", "event_time", "trade_date", "open", "high", "low", "close", "volume"}
missing_cols = sorted(required_cols.difference(set(df.columns)))
if missing_cols:
    raise ValueError(f"Missing required columns in raw input: {missing_cols}")

df = (
    df
    .filter(F.col("ticker").isNotNull() & (F.length(F.trim(F.col("ticker"))) > 0))
    .dropna(subset=["ticker", "trade_date"]) 
    .withColumn("volume", F.coalesce(F.col("volume"), F.lit(0)))
)
# 3B) FFill open/high/low/close theo ticker, theo thời gian tăng dần
w_ffill = (
    Window
    .partitionBy("ticker")
    .orderBy("trade_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)

df_ffill = (
    df
    .withColumn("open",  F.last("open",  ignorenulls=True).over(w_ffill))
    .withColumn("high",  F.last("high",  ignorenulls=True).over(w_ffill))
    .withColumn("low",   F.last("low",   ignorenulls=True).over(w_ffill))
    .withColumn("close", F.last("close", ignorenulls=True).over(w_ffill))
)

df_ffill = (
    df_ffill
    .withColumn("ma10", F.avg("close").over(Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-9, 0)))
    .withColumn("ma20", F.avg("close").over(Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-19, 0)))
    .withColumn("ma50", F.avg("close").over(Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-49, 0))) 
    .withColumn("ma200", F.avg("close").over(Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-199, 0)))
)
#xóa đầu tiên
df_ffill = df_ffill.dropna(subset=["open","high","low","close"])

output_path = f"{BASE_PATH_PROCESSED}"
(
    df_ffill
    .write
    .repartition(16, "ticker")
    .mode("overwrite")
    .partitionBy("ticker")
    .parquet(output_path)
)


df_ffill.show(truncate=False)
df_ffill.printSchema()
print(f"Total rows: {df_ffill.count()}")
