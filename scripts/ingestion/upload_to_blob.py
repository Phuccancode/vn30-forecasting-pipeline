import os
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

def get_blob_service_client():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set")
    return BlobServiceClient.from_connection_string(connection_string)

def upload_parquet_to_blob(local_file_path: str, container_name: str, blob_name: str, overwrite: bool = True) -> bool:
    """
    Uploads a local parquet file to Azure Blob Storage.
    
    Args:
        local_file_path: Path to the local file
        container_name: Name of the Azure container (e.g., 'raw' or 'processed')
        blob_name: Path within the container (e.g., '2023/10/01/ACB.parquet')
        overwrite: Whether to overwrite if file exists
        
    Returns:
        bool: True if upload was successful or file already exists (and overwrite=False), False on failure
    """
    try:
        blob_service_client = get_blob_service_client()
        container_client = blob_service_client.get_container_client(container_name)
        
        # Create container if it doesn't exist
        try:
            container_client.create_container()
            logger.info(f"Created container '{container_name}'")
        except ResourceExistsError:
            pass # Container already exists
            
        blob_client = container_client.get_blob_client(blob_name)
        
        logger.info(f"Uploading '{local_file_path}' to container '{container_name}' as '{blob_name}'...")
        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=overwrite)
            
        logger.info(f"Successfully uploaded '{blob_name}'")
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload '{blob_name}': {e}")
        return False


def upload_directory_to_blob(
    local_dir_path: str,
    container_name: str,
    prefix: str = "",
    overwrite: bool = True,
    max_workers: int = 8,
) -> bool:
    """
    Upload parquet files from a local directory recursively to Azure Blob Storage.

    Args:
        local_dir_path: Directory containing parquet files
        container_name: Azure container name
        prefix: Optional blob prefix inside container
        overwrite: Whether to overwrite existing blobs
        max_workers: Number of parallel upload workers
    """
    try:
        root = Path(local_dir_path)
        if not root.exists() or not root.is_dir():
            logger.warning(f"Directory not found or not a directory: {local_dir_path}")
            return False

        files = [p for p in root.rglob("*.parquet") if p.is_file()]
        if not files:
            logger.warning(f"No parquet files found in {local_dir_path}")
            return False

        blob_service_client = get_blob_service_client()
        container_client = blob_service_client.get_container_client(container_name)

        try:
            container_client.create_container()
            logger.info(f"Created container '{container_name}'")
        except ResourceExistsError:
            pass

        def _upload_file(file_path: Path):
            relative_path = file_path.relative_to(root).as_posix()
            blob_name = f"{prefix.strip('/')}/{relative_path}" if prefix else relative_path
            with file_path.open("rb") as data:
                container_client.get_blob_client(blob_name).upload_blob(data, overwrite=overwrite)
            return blob_name

        uploaded = 0
        failed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_upload_file, file_path): file_path for file_path in files}
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    blob_name = future.result()
                    uploaded += 1
                    logger.info(f"Uploaded '{file_path}' -> '{blob_name}'")
                except Exception as exc:
                    failed += 1
                    logger.error(f"Failed to upload '{file_path}': {exc}")

        logger.info(
            "Directory upload completed. uploaded=%s failed=%s total=%s",
            uploaded,
            failed,
            len(files),
        )
        return failed == 0
    except Exception as e:
        logger.error(f"Failed directory upload from '{local_dir_path}': {e}")
        return False
        
def check_blob_exists(container_name: str, blob_name: str) -> bool:
    """Checks if a blob exists in the specified container."""
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        return blob_client.exists()
    except Exception as e:
        logger.error(f"Error checking blob existence for '{blob_name}': {e}")
        return False
