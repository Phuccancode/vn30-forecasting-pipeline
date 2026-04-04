import os
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
# Load .env
from dotenv import load_dotenv
load_dotenv()

def get_blob_service_client():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("Cannot find azure connection string in env")
    print("Successfully get azure connection string from env:", connection_string)
    return BlobServiceClient.from_connection_string(connection_string)

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

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    
    return spark

spark = get_spark_session()
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

from pyspark.sql.functions import input_file_name, regexp_extract, upper
from pyspark.sql import functions as F
df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.parquet")
    .parquet(
        f"abfss://raw@{storage_account}.dfs.core.windows.net/2026/03"
    )
    .withColumn("_source_file", input_file_name())
    .withColumn(
        "Ticker",
        upper(regexp_extract("_source_file", r"/([^/]+)\.parquet$", 1))
    )
    .withColumn("event_time_utc", F.expr("to_utc_timestamp(timestamp_micros(CAST(Time / 1000 AS BIGINT)), 'Asia/Ho_Chi_Minh')"))
    .withColumn("trade_date_utc", F.to_date("event_time_utc"))
    .drop("_source_file")
    .drop("Time")
    .select("Ticker", "event_time_utc", "trade_date_utc", "Open", "High", "Low", "Close", "Volume")
)

df.show(truncate=False)
print(f"Total rows: {df.count()}")

