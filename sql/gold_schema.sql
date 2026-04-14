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
