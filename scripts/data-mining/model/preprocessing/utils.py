import os
import numpy as np
from pyspark.sql import SparkSession

try:
    from google.colab import userdata
    ACCOUNT_NAME = userdata.get('AZURE_STORAGE_ACCOUNT_NAME')
    ACCOUNT_KEY = userdata.get('AZURE_STORAGE_ACCOUNT_KEY')
    print("--- Đang dùng Secrets của Colab ---")

except ImportError:
    from dotenv import load_dotenv
    load_dotenv()
    ACCOUNT_NAME = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
    ACCOUNT_KEY = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
    print("--- Đang dùng file .env ở máy nhà ---")

# Kiểm tra xem có lấy được key không
if not ACCOUNT_NAME or not ACCOUNT_KEY:
    print("⚠️ Cảnh báo: Không tìm thấy API Key!")

storage_account = ACCOUNT_NAME
BASE_PATH_PROCESSED = f"abfss://processed@{storage_account}.dfs.core.windows.net"

def _get_spark_session():
    storage_account = ACCOUNT_NAME
    account_key = ACCOUNT_KEY
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


def get_data():
    spark = _get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.parquet")
        .parquet(
            f"{BASE_PATH_PROCESSED}/silver"
        )
    )
    
    return df.toPandas()

def create_sequences_multifeature(data, seq_len=30, target_idx=3):
    """
    data: (n_samples, n_features)
    target_idx: vị trí cột cần predict (Close = 3)
    """
    X, y = [], []
    
    for i in range(len(data) - seq_len):
        X.append(data[i:i+seq_len])          # (seq_len, n_features)
        y.append(data[i+seq_len, target_idx])  # lấy Close
    
    return np.array(X), np.array(y)

def time_series_split(df, train_ratio=0.8, sort_col="event_time"):
    df = df.sort_values(by=sort_col).reset_index(drop=True)
    
    split_idx = int(len(df) * train_ratio)
    
    train_df = df.iloc[:split_idx]
    test_df = df.iloc[split_idx:]
    
    return train_df, test_df