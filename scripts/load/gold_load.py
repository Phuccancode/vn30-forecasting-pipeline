from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from time import perf_counter
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("gold_load")


EMBEDDED_GOLD_DDL = """
IF OBJECT_ID('dbo.dim_ticker', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_ticker (
        ticker_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ticker NVARCHAR(16) NOT NULL,
        sector NVARCHAR(128) NULL,
        exchange NVARCHAR(32) NULL CONSTRAINT DF_dim_ticker_exchange DEFAULT ('HOSE'),
        is_active BIT NOT NULL CONSTRAINT DF_dim_ticker_is_active DEFAULT (1),
        created_at DATETIME2(0) NOT NULL CONSTRAINT DF_dim_ticker_created_at DEFAULT (SYSUTCDATETIME()),
        updated_at DATETIME2(0) NOT NULL CONSTRAINT DF_dim_ticker_updated_at DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT UQ_dim_ticker_ticker UNIQUE (ticker)
    );
END;
GO

IF OBJECT_ID('dbo.dim_date', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_date (
        date_key INT NOT NULL PRIMARY KEY,
        full_date DATE NOT NULL,
        [year] SMALLINT NOT NULL,
        [quarter] TINYINT NOT NULL,
        [month] TINYINT NOT NULL,
        [day] TINYINT NOT NULL,
        day_of_week TINYINT NOT NULL,
        is_weekend BIT NOT NULL,
        CONSTRAINT UQ_dim_date_full_date UNIQUE (full_date)
    );
END;
GO

IF OBJECT_ID('dbo.fact_prices', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_prices (
        fact_price_key BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ticker_key INT NOT NULL,
        date_key INT NOT NULL,
        event_time DATETIME2(0) NOT NULL,
        open_price DECIMAL(19,4) NOT NULL,
        high_price DECIMAL(19,4) NOT NULL,
        low_price DECIMAL(19,4) NOT NULL,
        close_price DECIMAL(19,4) NOT NULL,
        volume BIGINT NOT NULL,
        ma10 DECIMAL(19,4) NULL,
        ma20 DECIMAL(19,4) NULL,
        ma50 DECIMAL(19,4) NULL,
        ma200 DECIMAL(19,4) NULL,
        load_ts_utc DATETIME2(0) NOT NULL CONSTRAINT DF_fact_prices_load_ts_utc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT FK_fact_prices_dim_ticker FOREIGN KEY (ticker_key) REFERENCES dbo.dim_ticker(ticker_key),
        CONSTRAINT FK_fact_prices_dim_date FOREIGN KEY (date_key) REFERENCES dbo.dim_date(date_key),
        CONSTRAINT UQ_fact_prices_ticker_event_time UNIQUE (ticker_key, event_time)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_fact_prices_event_time'
      AND object_id = OBJECT_ID('dbo.fact_prices')
)
BEGIN
    CREATE INDEX IX_fact_prices_event_time
        ON dbo.fact_prices (event_time);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_fact_prices_ticker_date'
      AND object_id = OBJECT_ID('dbo.fact_prices')
)
BEGIN
    CREATE INDEX IX_fact_prices_ticker_date
        ON dbo.fact_prices (ticker_key, date_key);
END;
GO

CREATE OR ALTER VIEW dbo.vw_ml_features
AS
WITH base AS (
    SELECT
        dt.ticker,
        dd.full_date AS feature_date,
        fp.event_time,
        fp.open_price,
        fp.high_price,
        fp.low_price,
        fp.close_price,
        fp.volume,
        fp.ma10,
        fp.ma20,
        fp.ma50,
        fp.ma200
    FROM dbo.fact_prices fp
    INNER JOIN dbo.dim_ticker dt
        ON dt.ticker_key = fp.ticker_key
    INNER JOIN dbo.dim_date dd
        ON dd.date_key = fp.date_key
)
SELECT
    ticker,
    feature_date,
    event_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    ma10,
    ma20,
    ma50,
    ma200,
    LEAD(close_price) OVER (
        PARTITION BY ticker
        ORDER BY event_time
    ) AS target_next_close
FROM base;
""".strip()


def step_timer(label: str) -> float:
    logger.info("START: %s", label)
    return perf_counter()


def finish_step(label: str, started_at: float) -> None:
    elapsed = perf_counter() - started_at
    logger.info("DONE: %s (%.2fs)", label, elapsed)


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_spark_session(storage_account: str, account_key: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("VN30 Gold Loader")
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
    return spark


def read_silver_dataframe(spark: SparkSession, storage_account: str) -> DataFrame:
    silver_path = f"abfss://processed@{storage_account}.dfs.core.windows.net/silver"
    logger.info("Read Silver path: %s", silver_path)
    # Silver currently has mixed partition layouts from historical runs:
    # - trade_date=.../ticker=...
    # - ticker=...
    # Use recursive read to avoid partition schema conflicts, then recover ticker from file path.
    df = spark.read.option("recursiveFileLookup", "true").parquet(silver_path)
    source_path_col = F.input_file_name()
    ticker_from_path = F.regexp_extract(source_path_col, r"/ticker=([^/]+)", 1)

    if "ticker" in df.columns:
        df = df.withColumn("ticker", F.coalesce(F.col("ticker").cast("string"), ticker_from_path))
    else:
        df = df.withColumn("ticker", ticker_from_path)

    return df


def build_gold_dataframes(silver_df: DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    base = (
        silver_df.select(
            "ticker",
            "event_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ma10",
            "ma20",
            "ma50",
            "ma200",
        )
        .withColumn("full_date", F.to_date("event_time"))
        .dropna(subset=["ticker", "event_time", "full_date"])
    )

    dim_ticker = (
        base.select(F.col("ticker").cast("string"))
        .dropna(subset=["ticker"])
        .dropDuplicates(["ticker"])
        .orderBy("ticker")
    )

    dim_date = (
        base.select("full_date")
        .dropDuplicates(["full_date"])
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date").cast("smallint"))
        .withColumn("quarter", F.quarter("full_date").cast("tinyint"))
        .withColumn("month", F.month("full_date").cast("tinyint"))
        .withColumn("day", F.dayofmonth("full_date").cast("tinyint"))
        .withColumn("day_of_week", F.dayofweek("full_date").cast("tinyint"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("full_date").isin(1, 7), F.lit(1)).otherwise(F.lit(0)),
        )
        .orderBy("full_date")
    )

    fact = (
        base.select(
            F.col("ticker").cast("string").alias("ticker"),
            F.col("full_date").alias("full_date"),
            F.col("event_time").alias("event_time"),
            F.col("open").cast("double").alias("open_price"),
            F.col("high").cast("double").alias("high_price"),
            F.col("low").cast("double").alias("low_price"),
            F.col("close").cast("double").alias("close_price"),
            F.col("volume").cast("long").alias("volume"),
            F.col("ma10").cast("double").alias("ma10"),
            F.col("ma20").cast("double").alias("ma20"),
            F.col("ma50").cast("double").alias("ma50"),
            F.col("ma200").cast("double").alias("ma200"),
            F.current_timestamp().alias("load_ts_utc"),
        )
        .dropDuplicates(["ticker", "event_time"])
    )

    return dim_ticker.toPandas(), dim_date.toPandas(), fact.toPandas()


def create_engine_from_odbc(odbc_connection_string: str) -> Engine:
    sqlalchemy_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_connection_string)}"
    return create_engine(
        sqlalchemy_url,
        fast_executemany=True,
        pool_pre_ping=True,
    )


def find_ddl_file() -> Path | None:
    candidates = [
        Path("/opt/airflow/sql/gold_schema.sql"),
        Path(__file__).resolve().parents[2] / "sql" / "gold_schema.sql",
        Path.cwd() / "sql" / "gold_schema.sql",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def run_sql_batches(engine: Engine, sql_text: str) -> None:
    statements = [s.strip() for s in re.split(r"(?im)^\s*GO\s*$", sql_text) if s.strip()]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))


def apply_gold_ddl(engine: Engine) -> None:
    ddl_file = find_ddl_file()
    if ddl_file:
        logger.info("Apply DDL from file: %s", ddl_file)
        sql_text = ddl_file.read_text(encoding="utf-8")
    else:
        logger.warning("DDL file not found; using embedded DDL fallback")
        sql_text = EMBEDDED_GOLD_DDL
    run_sql_batches(engine, sql_text)


def load_staging_tables(
    engine: Engine,
    dim_ticker: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact: pd.DataFrame,
) -> None:
    dim_ticker.to_sql("stg_dim_ticker", engine, schema="dbo", if_exists="replace", index=False)
    dim_date.to_sql("stg_dim_date", engine, schema="dbo", if_exists="replace", index=False)
    fact.to_sql("stg_fact_prices", engine, schema="dbo", if_exists="replace", index=False)


def merge_to_gold(engine: Engine) -> None:
    merge_sql = """
    MERGE dbo.dim_ticker AS tgt
    USING (
        SELECT DISTINCT ticker
        FROM dbo.stg_dim_ticker
        WHERE ticker IS NOT NULL
    ) AS src
        ON tgt.ticker = src.ticker
    WHEN MATCHED THEN
        UPDATE SET
            tgt.is_active = 1,
            tgt.updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (ticker, sector, exchange, is_active, created_at, updated_at)
        VALUES (src.ticker, NULL, 'HOSE', 1, SYSUTCDATETIME(), SYSUTCDATETIME());

    MERGE dbo.dim_date AS tgt
    USING (
        SELECT DISTINCT
            CAST(date_key AS INT) AS date_key,
            CAST(full_date AS DATE) AS full_date,
            CAST([year] AS SMALLINT) AS [year],
            CAST([quarter] AS TINYINT) AS [quarter],
            CAST([month] AS TINYINT) AS [month],
            CAST([day] AS TINYINT) AS [day],
            CAST(day_of_week AS TINYINT) AS day_of_week,
            CAST(is_weekend AS BIT) AS is_weekend
        FROM dbo.stg_dim_date
    ) AS src
        ON tgt.date_key = src.date_key
    WHEN MATCHED THEN
        UPDATE SET
            tgt.full_date = src.full_date,
            tgt.[year] = src.[year],
            tgt.[quarter] = src.[quarter],
            tgt.[month] = src.[month],
            tgt.[day] = src.[day],
            tgt.day_of_week = src.day_of_week,
            tgt.is_weekend = src.is_weekend
    WHEN NOT MATCHED THEN
        INSERT (date_key, full_date, [year], [quarter], [month], [day], day_of_week, is_weekend)
        VALUES (src.date_key, src.full_date, src.[year], src.[quarter], src.[month], src.[day], src.day_of_week, src.is_weekend);

    MERGE dbo.fact_prices AS tgt
    USING (
        SELECT
            t.ticker_key,
            d.date_key,
            CAST(s.event_time AS DATETIME2(0)) AS event_time,
            CAST(s.open_price AS DECIMAL(19,4)) AS open_price,
            CAST(s.high_price AS DECIMAL(19,4)) AS high_price,
            CAST(s.low_price AS DECIMAL(19,4)) AS low_price,
            CAST(s.close_price AS DECIMAL(19,4)) AS close_price,
            CAST(s.volume AS BIGINT) AS volume,
            CAST(s.ma10 AS DECIMAL(19,4)) AS ma10,
            CAST(s.ma20 AS DECIMAL(19,4)) AS ma20,
            CAST(s.ma50 AS DECIMAL(19,4)) AS ma50,
            CAST(s.ma200 AS DECIMAL(19,4)) AS ma200,
            CAST(s.load_ts_utc AS DATETIME2(0)) AS load_ts_utc
        FROM dbo.stg_fact_prices AS s
        INNER JOIN dbo.dim_ticker AS t
            ON t.ticker = s.ticker
        INNER JOIN dbo.dim_date AS d
            ON d.full_date = CAST(s.full_date AS DATE)
    ) AS src
        ON tgt.ticker_key = src.ticker_key
       AND tgt.event_time = src.event_time
    WHEN MATCHED THEN
        UPDATE SET
            tgt.date_key = src.date_key,
            tgt.open_price = src.open_price,
            tgt.high_price = src.high_price,
            tgt.low_price = src.low_price,
            tgt.close_price = src.close_price,
            tgt.volume = src.volume,
            tgt.ma10 = src.ma10,
            tgt.ma20 = src.ma20,
            tgt.ma50 = src.ma50,
            tgt.ma200 = src.ma200,
            tgt.load_ts_utc = src.load_ts_utc
    WHEN NOT MATCHED THEN
        INSERT (
            ticker_key,
            date_key,
            event_time,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            ma10,
            ma20,
            ma50,
            ma200,
            load_ts_utc
        )
        VALUES (
            src.ticker_key,
            src.date_key,
            src.event_time,
            src.open_price,
            src.high_price,
            src.low_price,
            src.close_price,
            src.volume,
            src.ma10,
            src.ma20,
            src.ma50,
            src.ma200,
            src.load_ts_utc
        );

    DROP TABLE IF EXISTS dbo.stg_dim_ticker;
    DROP TABLE IF EXISTS dbo.stg_dim_date;
    DROP TABLE IF EXISTS dbo.stg_fact_prices;
    """

    view_sql = """
    CREATE OR ALTER VIEW dbo.vw_ml_features
    AS
    WITH base AS (
        SELECT
            dt.ticker,
            dd.full_date AS feature_date,
            fp.event_time,
            fp.open_price,
            fp.high_price,
            fp.low_price,
            fp.close_price,
            fp.volume,
            fp.ma10,
            fp.ma20,
            fp.ma50,
            fp.ma200
        FROM dbo.fact_prices fp
        INNER JOIN dbo.dim_ticker dt
            ON dt.ticker_key = fp.ticker_key
        INNER JOIN dbo.dim_date dd
            ON dd.date_key = fp.date_key
    )
    SELECT
        ticker,
        feature_date,
        event_time,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        ma10,
        ma20,
        ma50,
        ma200,
        LEAD(close_price) OVER (
            PARTITION BY ticker
            ORDER BY event_time
        ) AS target_next_close
    FROM base;
    """
    with engine.begin() as conn:
        conn.execute(text(merge_sql))
        conn.execute(text(view_sql))


def log_validation(engine: Engine) -> None:
    validation_sql = """
    SELECT
        (SELECT COUNT(*) FROM dbo.dim_ticker) AS dim_ticker_count,
        (SELECT COUNT(*) FROM dbo.dim_date) AS dim_date_count,
        (SELECT COUNT(*) FROM dbo.fact_prices) AS fact_prices_count,
        (SELECT COUNT(*) FROM dbo.vw_ml_features) AS ml_feature_rows;
    """
    with engine.begin() as conn:
        row = conn.execute(text(validation_sql)).mappings().first()
    logger.info("Gold validation snapshot: %s", dict(row) if row else {})


def main() -> None:
    storage_account = get_required_env("AZURE_STORAGE_ACCOUNT_NAME")
    storage_key = get_required_env("AZURE_STORAGE_ACCOUNT_KEY")
    sql_conn_str = get_required_env("AZURE_SQL_CONNECTION_STRING")

    t = step_timer("Create Spark session")
    spark = get_spark_session(storage_account, storage_key)
    finish_step("Create Spark session", t)

    try:
        t = step_timer("Read Silver dataset")
        silver_df = read_silver_dataframe(spark, storage_account)
        silver_count = silver_df.count()
        finish_step("Read Silver dataset", t)
        logger.info("Silver rows: %s", silver_count)

        if silver_count == 0:
            raise RuntimeError("Silver dataset is empty; abort Gold load")

        t = step_timer("Build Gold source dataframes")
        dim_ticker, dim_date, fact = build_gold_dataframes(silver_df)
        finish_step("Build Gold source dataframes", t)
        logger.info(
            "Prepared rows - dim_ticker: %s | dim_date: %s | fact: %s",
            len(dim_ticker),
            len(dim_date),
            len(fact),
        )

        t = step_timer("Open SQL engine")
        engine = create_engine_from_odbc(sql_conn_str)
        finish_step("Open SQL engine", t)

        t = step_timer("Apply Gold DDL")
        apply_gold_ddl(engine)
        finish_step("Apply Gold DDL", t)

        t = step_timer("Load staging tables")
        load_staging_tables(engine, dim_ticker, dim_date, fact)
        finish_step("Load staging tables", t)

        t = step_timer("Merge staging to Gold schema")
        merge_to_gold(engine)
        finish_step("Merge staging to Gold schema", t)

        t = step_timer("Validate Gold outputs")
        log_validation(engine)
        finish_step("Validate Gold outputs", t)
        logger.info("Gold modeling completed successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
