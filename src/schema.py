"""
PHASE 2: Schema Unification
============================

This module unifies Yellow and Green taxi schemas into a common format.

Why do we need this?
- Yellow and Green taxis have different column names
- We need consistent schema for analysis
- Makes downstream processing easier

Target Schema:
[pickup_time, dropoff_time, pickup_loc, dropoff_loc,
 trip_distance, fare, total_amount, congestion_surcharge]

How it works:
1. Read parquet file with DuckDB (out-of-core)
2. Map columns to unified schema
3. Cast to correct data types
4. Save as new parquet file
"""

import duckdb
from pathlib import Path
from loguru import logger
import sys

from .config import (
    YELLOW_RAW_DIR, GREEN_RAW_DIR,
    UNIFIED_DIR, YELLOW_COLUMN_MAP, GREEN_COLUMN_MAP,
    UNIFIED_SCHEMA, LOGS_DIR
)

# Setup logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(LOGS_DIR / "schema.log", rotation="10 MB")


def build_select_query(column_map: dict, taxi_type: str) -> str:
    """
    Build SQL SELECT query to map columns to unified schema.
    
    Args:
        column_map: Dictionary mapping original columns to unified columns
        taxi_type: 'yellow' or 'green'
    
    Returns:
        SQL SELECT statement
    
    Example:
        Original Yellow schema: tpep_pickup_datetime, tpep_dropoff_datetime, ...
        Unified schema: pickup_time, dropoff_time, ...
        
        Query: SELECT tpep_pickup_datetime AS pickup_time,
                      tpep_dropoff_datetime AS dropoff_time, ...
    """
    
    select_parts = []
    
    for unified_col in UNIFIED_SCHEMA:
        if unified_col in column_map.values():
            # Find original column name
            original_col = [k for k, v in column_map.items() if v == unified_col][0]
            
            # Add type casting for datetime columns
            if 'time' in unified_col:
                select_parts.append(f"CAST({original_col} AS TIMESTAMP) AS {unified_col}")
            # Add type casting for numeric columns
            elif unified_col in ['trip_distance', 'fare', 'total_amount', 'congestion_surcharge']:
                select_parts.append(f"CAST({original_col} AS DOUBLE) AS {unified_col}")
            # Add type casting for location IDs
            elif 'loc' in unified_col:
                select_parts.append(f"CAST({original_col} AS INTEGER) AS {unified_col}")
            else:
                select_parts.append(f"{original_col} AS {unified_col}")
        else:
            # Column doesn't exist in this taxi type, use NULL
            select_parts.append(f"NULL AS {unified_col}")
    
    return "SELECT " + ",\n       ".join(select_parts)


def unify_file(input_path: Path, output_path: Path, column_map: dict, taxi_type: str) -> bool:
    """
    Unify a single parquet file to the common schema.
    
    Args:
        input_path: Path to original parquet file
        output_path: Path to save unified parquet file
        column_map: Column mapping dictionary
        taxi_type: 'yellow' or 'green'
    
    Returns:
        True if successful, False otherwise
    
    How DuckDB makes this efficient:
    1. Reads only needed columns (columnar storage)
    2. Doesn't load entire file into memory (streaming)
    3. Writes directly to parquet (no intermediate format)
    """
    
    try:
        logger.info(f"Unifying {input_path.name}...")
        
        # Create DuckDB connection (in-memory database)
        con = duckdb.connect()
        
        # Build SELECT query
        select_query = build_select_query(column_map, taxi_type)
        
        # Full query: read parquet, transform, write parquet
        query = f"""
        COPY (
            {select_query}
            FROM read_parquet('{input_path}')
            WHERE pickup_time IS NOT NULL 
              AND dropoff_time IS NOT NULL
              AND pickup_loc IS NOT NULL
              AND dropoff_loc IS NOT NULL
        )
        TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        
        # Execute query
        con.execute(query)
        
        # Get row count
        result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()
        row_count = result[0]
        
        logger.success(f"Unified {input_path.name}: {row_count:,} rows")
        
        con.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to unify {input_path.name}: {e}")
        return False


def unify_all_files() -> dict:
    """
    Unify all Yellow and Green taxi files.
    
    Returns:
        Dictionary with unification statistics
    
    What this does:
    1. Find all parquet files in raw directories
    2. For each file, map columns to unified schema
    3. Save to processed/unified directory
    4. Track success/failure statistics
    """
    
    stats = {
        'yellow_unified': 0,
        'yellow_failed': 0,
        'green_unified': 0,
        'green_failed': 0,
    }
    
    logger.info("=" * 60)
    logger.info("Starting Schema Unification")
    logger.info("=" * 60)
    
    # Create output directory
    UNIFIED_DIR.mkdir(parents=True, exist_ok=True)
    
    # Unify Yellow taxi files
    logger.info("\nUnifying Yellow Taxi Files...")
    yellow_files = sorted(YELLOW_RAW_DIR.glob("*.parquet"))
    
    for input_path in yellow_files:
        output_path = UNIFIED_DIR / f"yellow_unified_{input_path.name}"
        
        # Skip if already exists
        if output_path.exists():
            logger.info(f"Skipping {input_path.name} (already unified)")
            stats['yellow_unified'] += 1
            continue
        
        if unify_file(input_path, output_path, YELLOW_COLUMN_MAP, 'yellow'):
            stats['yellow_unified'] += 1
        else:
            stats['yellow_failed'] += 1
    
    # Unify Green taxi files
    logger.info("\nUnifying Green Taxi Files...")
    green_files = sorted(GREEN_RAW_DIR.glob("*.parquet"))
    
    for input_path in green_files:
        output_path = UNIFIED_DIR / f"green_unified_{input_path.name}"
        
        # Skip if already exists
        if output_path.exists():
            logger.info(f"Skipping {input_path.name} (already unified)")
            stats['green_unified'] += 1
            continue
        
        if unify_file(input_path, output_path, GREEN_COLUMN_MAP, 'green'):
            stats['green_unified'] += 1
        else:
            stats['green_failed'] += 1
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("Unification Summary")
    logger.info("=" * 60)
    logger.info(f"Yellow Taxi:")
    logger.info(f"  Unified: {stats['yellow_unified']}")
    logger.info(f"  Failed: {stats['yellow_failed']}")
    logger.info(f"Green Taxi:")
    logger.info(f"  Unified: {stats['green_unified']}")
    logger.info(f"  Failed: {stats['green_failed']}")
    
    return stats


def verify_unified_schema(file_path: Path) -> bool:
    """
    Verify that a unified file has the correct schema.
    
    Args:
        file_path: Path to unified parquet file
    
    Returns:
        True if schema is correct, False otherwise
    """
    
    try:
        con = duckdb.connect()
        
        # Get column names
        result = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").fetchall()
        column_names = [row[0] for row in result]
        
        # Check if all expected columns exist
        for expected_col in UNIFIED_SCHEMA:
            if expected_col not in column_names:
                logger.error(f"Missing column {expected_col} in {file_path.name}")
                return False
        
        logger.success(f"Schema verified for {file_path.name}")
        con.close()
        return True
        
    except Exception as e:
        logger.error(f"Schema verification failed for {file_path.name}: {e}")
        return False


if __name__ == "__main__":
    """
    Run this module directly to unify all schemas:
    
    python -m src.schema
    
    This will:
    1. Read all files from data/raw/yellow/ and data/raw/green/
    2. Map columns to unified schema
    3. Save to data/processed/unified/
    
    Time: ~10-20 minutes depending on data size
    """
    
    # Unify all files
    stats = unify_all_files()
    
    # Verify a sample file
    sample_file = UNIFIED_DIR / "yellow_unified_yellow_tripdata_2025-01.parquet"
    if sample_file.exists():
        verify_unified_schema(sample_file)
    
    # Exit with error code if any unification failed
    total_failed = stats['yellow_failed'] + stats['green_failed']
    if total_failed > 0:
        logger.error(f"Some unifications failed ({total_failed} files)")
        sys.exit(1)
    else:
        logger.success("All files unified successfully!")
        sys.exit(0)
