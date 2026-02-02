"""
PHASE 1: Automated Data Ingestion
==================================

This module handles downloading TLC taxi data from the NYC TLC website.

Key Concepts:
- Web scraping: Parsing HTML to find download links
- HTTP downloads: Downloading large files with progress bars
- Retry logic: Handling network failures gracefully
- File validation: Ensuring downloaded files aren't corrupted

Functions:
- scrape_tlc_data_urls(): Finds all parquet file URLs on TLC website
- download_file_with_retry(): Downloads a file with exponential backoff
- validate_parquet_file(): Checks if downloaded file is valid
- download_all_data(): Main orchestrator function
"""

import requests
from bs4 import BeautifulSoup
from pathlib import Path
import time
from tqdm import tqdm
from loguru import logger
import sys

# Import our configuration
from .config import (
    TLC_BASE_URL, YELLOW_PATTERN, GREEN_PATTERN,
    YELLOW_RAW_DIR, GREEN_RAW_DIR, TAXI_ZONES_DIR, TAXI_ZONES_URL,
    YEARS_TO_DOWNLOAD, MONTHS_TO_DOWNLOAD, LOGS_DIR
)

# Setup logging
logger.remove()  # Remove default handler
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(LOGS_DIR / "ingestion.log", rotation="10 MB")


def download_file_with_retry(url: str, destination: Path, max_retries: int = 3) -> bool:
    """
    Download a file from URL to destination with retry logic.
    
    Args:
        url: URL to download from
        destination: Local path to save file
        max_retries: Maximum number of retry attempts
    
    Returns:
        True if successful, False otherwise
    
    How it works:
    1. Try to download file
    2. If fails, wait (exponential backoff: 2s, 4s, 8s)
    3. Retry up to max_retries times
    4. Show progress bar during download
    """
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading {url} (attempt {attempt + 1}/{max_retries})")
            
            # Send HTTP GET request with streaming (don't load entire file into memory)
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()  # Raise exception for 4xx/5xx status codes
            
            # Get file size from headers
            total_size = int(response.headers.get('content-length', 0))
            
            # Create parent directory if doesn't exist
            destination.parent.mkdir(parents=True, exist_ok=True)
            
            # Download with progress bar
            with open(destination, 'wb') as f:
                with tqdm(total=total_size, unit='B', unit_scale=True, 
                         desc=destination.name) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:  # Filter out keep-alive chunks
                            f.write(chunk)
                            pbar.update(len(chunk))
            
            logger.success(f"Downloaded {destination.name}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Download failed: {e}")
            
            if attempt < max_retries - 1:
                # Exponential backoff: 2^attempt seconds
                wait_time = 2 ** attempt
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to download {url} after {max_retries} attempts")
                return False
    
    return False


def validate_parquet_file(file_path: Path) -> bool:
    """
    Validate that a parquet file is readable and not corrupted.
    
    Args:
        file_path: Path to parquet file
    
    Returns:
        True if valid, False otherwise
    
    How it works:
    1. Try to read file metadata using pyarrow
    2. Check if file has rows
    3. If any error, file is corrupted
    """
    try:
        import pyarrow.parquet as pq
        
        # Read parquet file metadata (doesn't load data)
        parquet_file = pq.ParquetFile(file_path)
        
        # Check if file has data
        num_rows = parquet_file.metadata.num_rows
        
        if num_rows > 0:
            logger.success(f"Validated {file_path.name}: {num_rows:,} rows")
            return True
        else:
            logger.error(f"File {file_path.name} has 0 rows")
            return False
            
    except Exception as e:
        logger.error(f"Validation failed for {file_path.name}: {e}")
        return False


def download_taxi_zones() -> bool:
    """
    Download and extract taxi zone shapefiles.
    
    Returns:
        True if successful, False otherwise
    
    What are taxi zones?
    - NYC is divided into 264 taxi zones
    - Each zone has a unique ID (1-264)
    - Shapefiles contain geographic boundaries
    - We need this to identify "South of 60th Street"
    """
    try:
        import zipfile
        
        logger.info("Downloading taxi zone shapefiles...")
        
        # Download zip file
        zip_path = TAXI_ZONES_DIR / "taxi_zones.zip"
        if not download_file_with_retry(TAXI_ZONES_URL, zip_path):
            return False
        
        # Extract zip file
        logger.info("Extracting shapefiles...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(TAXI_ZONES_DIR)
        
        # Remove zip file
        zip_path.unlink()
        
        logger.success("Taxi zones downloaded and extracted")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download taxi zones: {e}")
        return False


def download_all_data(skip_existing: bool = True) -> dict:
    """
    Download all required TLC data files.
    
    Args:
        skip_existing: If True, skip files that already exist
    
    Returns:
        Dictionary with download statistics
    
    What this downloads:
    - Yellow taxi data: 2023-2025 (36 files)
    - Green taxi data: 2023-2025 (36 files)
    - Taxi zone shapefiles (1 zip)
    
    Total: ~50 GB of data
    """
    
    stats = {
        'yellow_downloaded': 0,
        'yellow_skipped': 0,
        'yellow_failed': 0,
        'green_downloaded': 0,
        'green_skipped': 0,
        'green_failed': 0,
    }
    
    logger.info("=" * 60)
    logger.info("Starting TLC Data Download")
    logger.info("=" * 60)
    
    # Download taxi zones first (needed for Phase 5)
    if not (TAXI_ZONES_DIR / "taxi_zones.shp").exists():
        download_taxi_zones()
    else:
        logger.info("Taxi zones already exist, skipping")
    
    # Download Yellow taxi data
    logger.info("\n" + "=" * 60)
    logger.info("Downloading Yellow Taxi Data")
    logger.info("=" * 60)
    
    for year in YEARS_TO_DOWNLOAD:
        for month in MONTHS_TO_DOWNLOAD:
            # Build filename and URL
            filename = YELLOW_PATTERN.format(year=year, month=month)
            url = f"{TLC_BASE_URL}/{filename}"
            destination = YELLOW_RAW_DIR / filename
            
            # Skip if already exists
            if skip_existing and destination.exists():
                logger.info(f"Skipping {filename} (already exists)")
                stats['yellow_skipped'] += 1
                continue
            
            # Download file
            if download_file_with_retry(url, destination):
                # Validate file
                if validate_parquet_file(destination):
                    stats['yellow_downloaded'] += 1
                else:
                    # Delete corrupted file
                    destination.unlink()
                    stats['yellow_failed'] += 1
            else:
                stats['yellow_failed'] += 1
    
    # Download Green taxi data
    logger.info("\n" + "=" * 60)
    logger.info("Downloading Green Taxi Data")
    logger.info("=" * 60)
    
    for year in YEARS_TO_DOWNLOAD:
        for month in MONTHS_TO_DOWNLOAD:
            # Build filename and URL
            filename = GREEN_PATTERN.format(year=year, month=month)
            url = f"{TLC_BASE_URL}/{filename}"
            destination = GREEN_RAW_DIR / filename
            
            # Skip if already exists
            if skip_existing and destination.exists():
                logger.info(f"Skipping {filename} (already exists)")
                stats['green_skipped'] += 1
                continue
            
            # Download file
            if download_file_with_retry(url, destination):
                # Validate file
                if validate_parquet_file(destination):
                    stats['green_downloaded'] += 1
                else:
                    # Delete corrupted file
                    destination.unlink()
                    stats['green_failed'] += 1
            else:
                stats['green_failed'] += 1
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("Download Summary")
    logger.info("=" * 60)
    logger.info(f"Yellow Taxi:")
    logger.info(f"  Downloaded: {stats['yellow_downloaded']}")
    logger.info(f"  Skipped: {stats['yellow_skipped']}")
    logger.info(f"  Failed: {stats['yellow_failed']}")
    logger.info(f"Green Taxi:")
    logger.info(f"  Downloaded: {stats['green_downloaded']}")
    logger.info(f"  Skipped: {stats['green_skipped']}")
    logger.info(f"  Failed: {stats['green_failed']}")
    
    return stats


if __name__ == "__main__":
    """
    Run this module directly to download all data:
    
    python -m src.ingestion
    
    This will download ~50 GB of data, so make sure you have:
    - Stable internet connection
    - Enough disk space
    - Time (30-60 minutes depending on connection)
    """
    
    # Create directories
    from .config import create_directories
    create_directories()
    
    # Download all data
    stats = download_all_data(skip_existing=True)
    
    # Exit with error code if any downloads failed
    total_failed = stats['yellow_failed'] + stats['green_failed']
    if total_failed > 0:
        logger.error(f"Some downloads failed ({total_failed} files)")
        sys.exit(1)
    else:
        logger.success("All downloads completed successfully!")
        sys.exit(0)
