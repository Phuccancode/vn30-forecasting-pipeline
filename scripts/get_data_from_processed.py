import os
from pyspark.sql import SparkSession

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

storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
BASE_PATH_PROCESSED = f"abfss://processed@{storage_account}.dfs.core.windows.net"

spark = get_spark_session()
df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.parquet")
    .parquet(
        f"{BASE_PATH_PROCESSED}/silver"
    )
)

# df.show(truncate=False)
print(df.count())