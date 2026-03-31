import os
import argparse
import logging
import yaml
import time
from datetime import datetime
import pandas as pd
from vnstock import Vnstock

from upload_to_blob import upload_parquet_to_blob

# Config logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("vnstock_ingestion")

def load_tickers(config_path: str = "../../config/tickers.yaml") -> list:
    """Load VN30 tickers from yaml config"""
    # Adjust path if running locally vs from Airflow
    if not os.path.exists(config_path):
        # Fallback to absolute path relative to script location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "../../config/tickers.yaml")
        
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def fetch_data_with_retry(ticker: str, start_date: str, end_date: str, max_retries: int = 3) -> pd.DataFrame:
    """Fetch historical data using vnstock with retry logic"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching {ticker} from {start_date} to {end_date} (Attempt {attempt+1}/{max_retries})...")
            df = (
                Vnstock()
                .stock(symbol=ticker, source="KBS")
                .quote.history(start=start_date, end=end_date, interval="1D")
            )
            
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

def process_and_upload(df: pd.DataFrame, ticker: str, start_date: str, target_date: str):
    """Save dataframe to local parquet and upload to Azure Blob"""
    if df.empty:
        return

    # Create local temp directory
    temp_dir = "/tmp/vn30_raw"
    os.makedirs(temp_dir, exist_ok=True)
    
    local_file = os.path.join(temp_dir, f"{ticker}_{start_date}_{target_date}.parquet")
    
    # Standardize column names
    df.columns = [col.capitalize() for col in df.columns]
    
    # Save to parquet
    df.to_parquet(local_file, index=False)
    
    # Upload to Azure Blob Storage
    # Target path: raw/YYYY/MM/DD/TICKER.parquet based on execution target date
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    blob_name = f"{dt.year}/{dt.month:02d}/{dt.day:02d}/{ticker}.parquet"
    
    success = upload_parquet_to_blob(
        local_file_path=local_file,
        container_name="raw",
        blob_name=blob_name,
        overwrite=True
    )
    
    if success:
        # Clean up local file 
        os.remove(local_file)
    else:
        logger.error(f"Failed to upload {ticker}. Local file kept at {local_file}")

def main():
    parser = argparse.ArgumentParser(description="Extract VN30 data from vnstock API")
    parser.add_argument("--mode", type=str, choices=["daily", "backfill"], required=True, 
                        help="Execution mode: 'daily' (yesterday/today data) or 'backfill' (5 years historical)")
    parser.add_argument("--date", type=str, default=datetime.today().strftime('%Y-%m-%d'),
                        help="Target date for data fetching (YYYY-MM-DD). Defaults to today.")
    
    args = parser.parse_args()
    
    tickers = load_tickers()
    target_date = args.date
    
    if args.mode == "backfill":
        # 5 years back from target date
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        start_date = f"{target_dt.year - 5}-{target_dt.month:02d}-{target_dt.day:02d}"
        logger.info(f"Running BACKFILL mode: fetching data from {start_date} to {target_date}")
    else:
        # Daily mode: just get last 3 days to account for weekends/holidays
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        ts = pd.Timestamp(target_dt)
        start_date = (ts - pd.Timedelta(days=3)).strftime("%Y-%m-%d")
        logger.info(f"Running DAILY mode for target date {target_date}")


    for ticker in tickers:
        df = fetch_data_with_retry(ticker, start_date, target_date)
        
        if not df.empty:
            process_and_upload(df, ticker, start_date, target_date)
            
        # Rate limit protection
        time.sleep(1.5)

if __name__ == "__main__":
    main()
