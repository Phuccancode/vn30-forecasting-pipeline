from __future__ import annotations

import argparse
import logging
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from time import perf_counter, sleep
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Gold model to Azure SQL")
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional lower bound date (YYYY-MM-DD) for Gold load window.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional upper bound date (YYYY-MM-DD) for Gold load window.",
    )
    parser.add_argument(
        "--batch-days",
        type=int,
        default=93,
        help="Number of days per Gold batch to limit memory usage.",
    )
    return parser.parse_args()


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
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"),
    )
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


def parse_optional_date(value: str | None, field_name: str) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}: {value}. Expected YYYY-MM-DD") from exc


def prepare_gold_base_dataframe(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.select(
            F.col("ticker").cast("string").alias("ticker"),
            F.col("event_time").cast("timestamp").alias("event_time"),
            F.col("open").cast("double").alias("open_price"),
            F.col("high").cast("double").alias("high_price"),
            F.col("low").cast("double").alias("low_price"),
            F.col("close").cast("double").alias("close_price"),
            F.col("volume").cast("long").alias("volume"),
            F.col("ma10").cast("double").alias("ma10"),
            F.col("ma20").cast("double").alias("ma20"),
            F.col("ma50").cast("double").alias("ma50"),
            F.col("ma200").cast("double").alias("ma200"),
        )
        .withColumn("full_date", F.to_date("event_time"))
        .dropna(subset=["ticker", "event_time", "full_date"])
        .dropDuplicates(["ticker", "event_time"])
    )


def resolve_load_window(
    gold_base_df: DataFrame,
    start_date_arg: date | None,
    end_date_arg: date | None,
) -> tuple[date, date]:
    stats = (
        gold_base_df.select(
            F.min("full_date").alias("min_date"),
            F.max("full_date").alias("max_date"),
        )
        .collect()[0]
    )
    data_min = stats["min_date"]
    data_max = stats["max_date"]
    if data_min is None or data_max is None:
        raise RuntimeError("Silver dataset has no valid event_time values for Gold window")

    start_date = start_date_arg or data_min
    end_date = end_date_arg or data_max
    if start_date > end_date:
        raise ValueError("--start-date must be <= --end-date")
    return start_date, end_date


def build_gold_dataframes(silver_df: DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fact = silver_df.select(
        "ticker",
        "full_date",
        "event_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "ma10",
        "ma20",
        "ma50",
        "ma200",
    ).toPandas()

    if fact.empty:
        empty_ticker = pd.DataFrame(columns=["ticker"])
        empty_date = pd.DataFrame(
            columns=[
                "date_key",
                "full_date",
                "year",
                "quarter",
                "month",
                "day",
                "day_of_week",
                "is_weekend",
            ]
        )
        return empty_ticker, empty_date, fact

    fact = fact.drop_duplicates(subset=["ticker", "event_time"], keep="last")
    fact["ticker"] = fact["ticker"].astype(str)
    fact["event_time"] = pd.to_datetime(fact["event_time"])
    fact["full_date"] = pd.to_datetime(fact["full_date"]).dt.date
    fact["load_ts_utc"] = pd.Timestamp.now(tz="UTC").floor("s").tz_localize(None)

    dim_ticker = (
        fact[["ticker"]]
        .dropna(subset=["ticker"])
        .drop_duplicates(subset=["ticker"])
        .sort_values("ticker")
        .reset_index(drop=True)
    )

    dim_date = (
        fact[["full_date"]]
        .dropna(subset=["full_date"])
        .drop_duplicates(subset=["full_date"])
        .sort_values("full_date")
        .reset_index(drop=True)
    )
    dt = pd.to_datetime(dim_date["full_date"])
    day_of_week = ((dt.dt.dayofweek + 1) % 7) + 1
    dim_date["date_key"] = dt.dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dt.dt.year.astype("int16")
    dim_date["quarter"] = dt.dt.quarter.astype("int8")
    dim_date["month"] = dt.dt.month.astype("int8")
    dim_date["day"] = dt.dt.day.astype("int8")
    dim_date["day_of_week"] = day_of_week.astype("int8")
    dim_date["is_weekend"] = day_of_week.isin([1, 7]).astype("int8")
    dim_date = dim_date[
        ["date_key", "full_date", "year", "quarter", "month", "day", "day_of_week", "is_weekend"]
    ]

    return dim_ticker, dim_date, fact


def create_engine_from_odbc(odbc_connection_string: str) -> Engine:
    if "Connection Timeout=" not in odbc_connection_string:
        odbc_connection_string = f"{odbc_connection_string.rstrip(';')};Connection Timeout=60;"

    sqlalchemy_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_connection_string)}"
    return create_engine(
        sqlalchemy_url,
        fast_executemany=True,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=120,
    )


def run_with_sql_retry(step_name: str, operation, max_attempts: int = 3) -> None:
    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            operation()
            return
        except OperationalError as err:
            last_err = err
            is_last_attempt = attempt == max_attempts
            err_text = str(err)
            is_transient_timeout = "HYT00" in err_text or "Login timeout expired" in err_text
            if not is_transient_timeout or is_last_attempt:
                raise

            delay_seconds = 5 * attempt
            logger.warning(
                "%s failed with transient SQL timeout (attempt %s/%s). Retrying in %ss",
                step_name,
                attempt,
                max_attempts,
                delay_seconds,
            )
            sleep(delay_seconds)

    if last_err:
        raise last_err


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
    run_with_sql_retry("Apply Gold DDL", lambda: run_sql_batches(engine, sql_text))


def load_staging_tables(
    engine: Engine,
    dim_ticker: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact: pd.DataFrame,
) -> None:
    staging_ddl_sql = """
    IF OBJECT_ID('dbo.stg_dim_ticker', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.stg_dim_ticker (
            ticker NVARCHAR(16) NOT NULL
        );
    END;

    IF OBJECT_ID('dbo.stg_dim_date', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.stg_dim_date (
            date_key INT NOT NULL,
            full_date DATE NOT NULL,
            [year] SMALLINT NOT NULL,
            [quarter] TINYINT NOT NULL,
            [month] TINYINT NOT NULL,
            [day] TINYINT NOT NULL,
            day_of_week TINYINT NOT NULL,
            is_weekend BIT NOT NULL
        );
    END;

    IF OBJECT_ID('dbo.stg_fact_prices', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.stg_fact_prices (
            ticker NVARCHAR(16) NOT NULL,
            full_date DATE NOT NULL,
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
            load_ts_utc DATETIME2(0) NOT NULL
        );
    END;
    """

    def _run() -> None:
        with engine.begin() as conn:
            conn.execute(text(staging_ddl_sql))
            conn.execute(text("TRUNCATE TABLE dbo.stg_dim_ticker;"))
            conn.execute(text("TRUNCATE TABLE dbo.stg_dim_date;"))
            conn.execute(text("TRUNCATE TABLE dbo.stg_fact_prices;"))

            dim_ticker.to_sql(
                "stg_dim_ticker",
                conn,
                schema="dbo",
                if_exists="append",
                index=False,
                chunksize=5000,
            )
            dim_date.to_sql(
                "stg_dim_date",
                conn,
                schema="dbo",
                if_exists="append",
                index=False,
                chunksize=5000,
            )
            fact[
                [
                    "ticker",
                    "full_date",
                    "event_time",
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                    "volume",
                    "ma10",
                    "ma20",
                    "ma50",
                    "ma200",
                    "load_ts_utc",
                ]
            ].to_sql(
                "stg_fact_prices",
                conn,
                schema="dbo",
                if_exists="append",
                index=False,
                chunksize=1000,
            )

    run_with_sql_retry(
        "Load staging tables",
        _run,
    )


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
    def _run() -> None:
        with engine.begin() as conn:
            conn.execute(text(merge_sql))
            conn.execute(text(view_sql))

    run_with_sql_retry(
        "Merge to Gold",
        _run,
    )


def log_validation(engine: Engine) -> None:
    validation_sql = """
    SELECT
        (SELECT COUNT(*) FROM dbo.dim_ticker) AS dim_ticker_count,
        (SELECT COUNT(*) FROM dbo.dim_date) AS dim_date_count,
        (SELECT COUNT(*) FROM dbo.fact_prices) AS fact_prices_count,
        (SELECT COUNT(*) FROM dbo.vw_ml_features) AS ml_feature_rows;
    """
    holder: dict[str, object] = {}

    def _run() -> None:
        with engine.begin() as conn:
            holder["row"] = conn.execute(text(validation_sql)).mappings().first()

    run_with_sql_retry("Validate Gold outputs", _run)
    row = holder.get("row")
    logger.info("Gold validation snapshot: %s", dict(row) if row else {})


def main() -> None:
    args = parse_args()
    storage_account = get_required_env("AZURE_STORAGE_ACCOUNT_NAME")
    storage_key = get_required_env("AZURE_STORAGE_ACCOUNT_KEY")
    sql_conn_str = get_required_env("AZURE_SQL_CONNECTION_STRING")
    start_date_arg = parse_optional_date(args.start_date, "--start-date")
    end_date_arg = parse_optional_date(args.end_date, "--end-date")
    if args.batch_days <= 0:
        raise ValueError("--batch-days must be > 0")

    t = step_timer("Create Spark session")
    spark = get_spark_session(storage_account, storage_key)
    finish_step("Create Spark session", t)

    try:
        t = step_timer("Read Silver dataset")
        silver_df = read_silver_dataframe(spark, storage_account)
        finish_step("Read Silver dataset", t)

        t = step_timer("Prepare and cache Gold base dataframe")
        gold_base_df = prepare_gold_base_dataframe(silver_df).persist(StorageLevel.MEMORY_AND_DISK)
        gold_base_rows = gold_base_df.count()
        finish_step("Prepare and cache Gold base dataframe", t)
        logger.info("Gold base rows: %s", gold_base_rows)
        if gold_base_rows == 0:
            raise RuntimeError("Silver dataset yielded no rows for Gold base dataframe")

        t = step_timer("Open SQL engine")
        engine = create_engine_from_odbc(sql_conn_str)
        finish_step("Open SQL engine", t)

        t = step_timer("Apply Gold DDL")
        apply_gold_ddl(engine)
        finish_step("Apply Gold DDL", t)

        window_start, window_end = resolve_load_window(
            gold_base_df=gold_base_df,
            start_date_arg=start_date_arg,
            end_date_arg=end_date_arg,
        )
        logger.info(
            "Gold load window resolved: %s -> %s | batch_days=%s",
            window_start,
            window_end,
            args.batch_days,
        )

        total_batches = 0
        loaded_batches = 0
        cursor = window_start
        while cursor <= window_end:
            total_batches += 1
            batch_start = cursor
            batch_end = min(window_end, batch_start + timedelta(days=args.batch_days - 1))
            logger.info("START: Gold batch %s | [%s, %s]", total_batches, batch_start, batch_end)

            batch_df = gold_base_df.filter(
                (F.col("full_date") >= F.lit(batch_start)) & (F.col("full_date") <= F.lit(batch_end))
            ).persist(StorageLevel.MEMORY_AND_DISK)
            try:
                batch_row_count = batch_df.count()
                if batch_row_count == 0:
                    logger.info("SKIP: Gold batch %s has no rows", total_batches)
                    cursor = batch_end + timedelta(days=1)
                    continue

                t = step_timer(f"Build Gold source dataframes for batch {total_batches}")
                dim_ticker, dim_date, fact = build_gold_dataframes(batch_df)
                finish_step(f"Build Gold source dataframes for batch {total_batches}", t)
                logger.info(
                    "Batch %s prepared rows - spark_batch_rows: %s | dim_ticker: %s | dim_date: %s | fact: %s",
                    total_batches,
                    batch_row_count,
                    len(dim_ticker),
                    len(dim_date),
                    len(fact),
                )
                if fact.empty:
                    logger.info("SKIP: Gold batch %s has empty fact dataframe", total_batches)
                    cursor = batch_end + timedelta(days=1)
                    continue

                t = step_timer(f"Load staging tables for batch {total_batches}")
                load_staging_tables(engine, dim_ticker, dim_date, fact)
                finish_step(f"Load staging tables for batch {total_batches}", t)

                t = step_timer(f"Merge staging to Gold schema for batch {total_batches}")
                merge_to_gold(engine)
                finish_step(f"Merge staging to Gold schema for batch {total_batches}", t)
                loaded_batches += 1
            finally:
                batch_df.unpersist()

            cursor = batch_end + timedelta(days=1)

        logger.info(
            "Gold batch summary: loaded_batches=%s total_batches=%s",
            loaded_batches,
            total_batches,
        )
        if loaded_batches == 0:
            raise RuntimeError("No Gold batches were loaded; check selected date window and Silver data")

        t = step_timer("Validate Gold outputs")
        log_validation(engine)
        finish_step("Validate Gold outputs", t)
        logger.info("Gold modeling completed successfully")
    finally:
        if "gold_base_df" in locals():
            gold_base_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
