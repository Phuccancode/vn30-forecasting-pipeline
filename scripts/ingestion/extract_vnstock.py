import argparse
import logging
import time
import shutil
from pathlib import Path
from datetime import datetime
import pandas as pd
from vnstock import Listing, Quote

from upload_to_blob import upload_directory_to_blob

# Config logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("vnstock_ingestion")
LOCAL_RAW_DIR = Path("/tmp/vn30_raw")

def load_tickers() -> list[str]:
    """
    Load stock tickers from HOSE only.
    """
    listing = Listing(source="KBS")
    df = listing.symbols_by_exchange()

    df_hose = df[(df["exchange"] == "HOSE") & (df["type"] == "stock")]
    tickers = df_hose["symbol"].dropna().unique().tolist()

    logger.info("--------------------------------------------")
    logger.info(f"Total symbols fetched: {len(df)}")
    logger.info(f"Total HOSE stock symbols: {len(tickers)}")

    return tickers

def fetch_data_with_retry(ticker: str, start_date: str, end_date: str, max_retries: int = 3) -> pd.DataFrame:
    """Fetch historical data using vnstock with retry logic"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching {ticker} from {start_date} to {end_date} (Attempt {attempt+1}/{max_retries})...")
            df = (
                Quote(symbol=ticker, source="KBS")
                .history(start=start_date, end=end_date, interval="1H")
            )
            time.sleep(3)  # Short sleep to avoid hitting API rate limits

            if df is not None and not df.empty:
                logger.info(f"Successfully fetched {len(df)} records for {ticker}")
                return df
            else:
                logger.warning(f"No data returned for {ticker}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.info(f"Waiting {wait_time}s before retrying...")
                time.sleep(wait_time)
            
    logger.error(f"Failed to fetch {ticker} after {max_retries} attempts.")
    return pd.DataFrame()

def chunked(items: list[str], chunk_size: int):
    """Yield fixed-size chunks from a list."""
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def reset_local_raw_dir():
    """Reset temporary local directory for one ingestion batch."""
    if LOCAL_RAW_DIR.exists():
        shutil.rmtree(LOCAL_RAW_DIR)
    LOCAL_RAW_DIR.mkdir(parents=True, exist_ok=True)


def process_and_store_locally(df: pd.DataFrame, ticker: str) -> int:
    """Save dataframe to local parquet partitioned by trade_date and ticker."""
    if df.empty:
        return 0

    df.columns = [col.capitalize() for col in df.columns]
    df["Ticker"] = ticker.upper()
    df["Trade_date"] = pd.to_datetime(df["Time"], errors="coerce").dt.date
    df = df.dropna(subset=["Trade_date"])

    files_written = 0
    for trade_date, part in df.groupby("Trade_date"):
        part = (
            part.sort_values(by=["Time"])
            .drop_duplicates(subset=["Time"], keep="last")
        )
        output_dir = LOCAL_RAW_DIR / f"trade_date={trade_date}" / f"ticker={ticker.upper()}"
        output_dir.mkdir(parents=True, exist_ok=True)
        local_file = output_dir / "part-000.parquet"
        part.to_parquet(local_file, index=False)
        files_written += 1

    return files_written


def main():
    parser = argparse.ArgumentParser(description="Extract VN30 data from vnstock API")
    parser.add_argument("--mode", type=str, choices=["daily", "backfill"], required=True,
                        help="Execution mode: 'daily' (yesterday/today data) or 'backfill' (5 years historical)")
    parser.add_argument("--date", type=str, default=datetime.today().strftime('%Y-%m-%d'),
                        help="Target date for data fetching (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Number of tickers per ingestion/upload batch.")
    parser.add_argument("--upload-workers", type=int, default=8,
                        help="Parallel workers for uploading parquet files to Azure Blob.")
    parser.add_argument("--sleep-between-tickers", type=float, default=3.0,
                        help="Sleep seconds after each ticker fetch for rate-limit protection.")
    parser.add_argument("--sleep-between-batches", type=float, default=2.0,
                        help="Sleep seconds after each batch upload.")

    args = parser.parse_args()

    tickers = load_tickers()
    target_date = args.date

    if args.mode == "backfill":
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        start_date = f"{target_dt.year - 5}-{target_dt.month:02d}-{target_dt.day:02d}"
        logger.info(f"Running BACKFILL mode: fetching data from {start_date} to {target_date}")
    else:
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        ts = pd.Timestamp(target_dt)
        start_date = (ts - pd.Timedelta(days=3)).strftime("%Y-%m-%d")
        logger.info(f"Running DAILY mode for target date {target_date}")

    total_written = 0
    total_uploaded_batches = 0
    total_failed_batches = 0

    for batch_index, ticker_batch in enumerate(chunked(tickers, args.batch_size), start=1):
        logger.info("Starting batch %s with %s tickers", batch_index, len(ticker_batch))
        reset_local_raw_dir()

        batch_written = 0
        for ticker in ticker_batch:
            df = fetch_data_with_retry(ticker, start_date, target_date)
            logger.info(f"Fetched {len(df)} records for {ticker}")

            if not df.empty:
                batch_written += process_and_store_locally(df, ticker)

            time.sleep(args.sleep_between_tickers)

        if batch_written == 0:
            logger.warning("Batch %s has no parquet files to upload", batch_index)
            continue

        ok = upload_directory_to_blob(
            local_dir_path=str(LOCAL_RAW_DIR),
            container_name="raw",
            prefix="",
            overwrite=True,
            max_workers=args.upload_workers,
        )

        total_written += batch_written
        if ok:
            total_uploaded_batches += 1
            logger.info("Batch %s uploaded successfully", batch_index)
            reset_local_raw_dir()
        else:
            total_failed_batches += 1
            logger.error("Batch %s upload failed. Local files kept at %s", batch_index, LOCAL_RAW_DIR)

        time.sleep(args.sleep_between_batches)

    logger.info(
        "Ingestion completed. total_files_written=%s successful_batches=%s failed_batches=%s",
        total_written,
        total_uploaded_batches,
        total_failed_batches,
    )

if __name__ == "__main__":
    main()
