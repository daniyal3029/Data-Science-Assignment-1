"""
PHASE 3: Ghost Trip Detection
==============================

This module detects and flags suspicious/fraudulent trip records.

What are "ghost trips"?
- Trips with impossible speeds (> 65 mph in NYC)
- Very short trips (< 1 min) with high fares (> $20)
- Zero distance trips with positive fares
- Data entry errors or fraudulent records

Why detect them?
- Skew analysis results
- Inflate revenue numbers
- Need to be logged and removed

Detection Rules:
1. Speed > 65 mph (NYC max is ~50 mph)
2. Duration < 1 min AND fare > $20 (suspicious)
3. Distance = 0 AND fare > 0 (impossible)

How we handle them:
- Flag and log to separate file
- Don't silently delete (need audit trail)
- Report statistics
"""

import duckdb
from pathlib import Path
from loguru import logger
import sys

from .config import (
    UNIFIED_DIR, GHOST_TRIPS_DIR,
    MAX_SPEED_MPH, MIN_TRIP_DURATION_SECONDS, HIGH_FARE_THRESHOLD,
    MIN_DISTANCE_MILES, LOGS_DIR
)

# Setup logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(LOGS_DIR / "cleaning.log", rotation="10 MB")


def detect_ghost_trips(input_path: Path, output_clean_path: Path, output_ghost_path: Path) -> dict:
    """
    Detect and separate ghost trips from clean trips.
    
    Args:
        input_path: Path to unified parquet file
        output_clean_path: Path to save clean trips
        output_ghost_path: Path to save ghost trips
    
    Returns:
        Dictionary with detection statistics
    
    Detection Logic:
    
    1. Speed Check:
       Speed = Distance / Time
       If Speed > 65 mph, it's a ghost trip
       
       Formula: speed_mph = (trip_distance / ((dropoff_time - pickup_time) / 3600))
       
    2. Short Trip High Fare:
       If trip < 1 minute AND fare > $20, suspicious
       
    3. Zero Distance Positive Fare:
       If distance = 0 but fare > 0, data error
    """
    
    try:
        logger.info(f"Detecting ghost trips in {input_path.name}...")
        
        con = duckdb.connect()
        
        # Build ghost trip detection query
        ghost_query = f"""
        CREATE TEMP TABLE all_trips AS
        SELECT *,
            -- Calculate trip duration in seconds
            EPOCH(dropoff_time - pickup_time) AS duration_seconds,
            
            -- Calculate speed in mph
            CASE 
                WHEN EPOCH(dropoff_time - pickup_time) > 0 THEN
                    (trip_distance / (EPOCH(dropoff_time - pickup_time) / 3600.0))
                ELSE 
                    0
            END AS speed_mph,
            
            -- Flag ghost trips
            CASE
                -- Rule 1: Speed > 65 mph
                WHEN (trip_distance / (EPOCH(dropoff_time - pickup_time) / 3600.0)) > {MAX_SPEED_MPH}
                    THEN 'excessive_speed'
                
                -- Rule 2: Short trip with high fare
                WHEN EPOCH(dropoff_time - pickup_time) < {MIN_TRIP_DURATION_SECONDS}
                     AND fare > {HIGH_FARE_THRESHOLD}
                    THEN 'short_trip_high_fare'
                
                -- Rule 3: Zero distance with positive fare
                WHEN trip_distance <= {MIN_DISTANCE_MILES}
                     AND fare > 0
                    THEN 'zero_distance_positive_fare'
                
                -- Rule 4: Negative duration (dropoff before pickup)
                WHEN EPOCH(dropoff_time - pickup_time) <= 0
                    THEN 'negative_duration'
                
                -- Rule 5: Negative fare
                WHEN fare < 0 OR total_amount < 0
                    THEN 'negative_fare'
                
                ELSE 'clean'
            END AS ghost_flag
        FROM read_parquet('{input_path}')
        """
        
        con.execute(ghost_query)
        
        # Save clean trips
        clean_query = f"""
        COPY (
            SELECT pickup_time, dropoff_time, pickup_loc, dropoff_loc,
                   trip_distance, fare, total_amount, congestion_surcharge
            FROM all_trips
            WHERE ghost_flag = 'clean'
        )
        TO '{output_clean_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        con.execute(clean_query)
        
        # Save ghost trips (for audit trail)
        ghost_save_query = f"""
        COPY (
            SELECT *, 
                   '{input_path.name}' AS source_file
            FROM all_trips
            WHERE ghost_flag != 'clean'
        )
        TO '{output_ghost_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        con.execute(ghost_save_query)
        
        # Get statistics
        stats_query = """
        SELECT 
            ghost_flag,
            COUNT(*) as count,
            ROUND(AVG(speed_mph), 2) as avg_speed_mph,
            ROUND(AVG(fare), 2) as avg_fare,
            ROUND(AVG(trip_distance), 2) as avg_distance
        FROM all_trips
        GROUP BY ghost_flag
        ORDER BY count DESC
        """
        
        stats_result = con.execute(stats_query).fetchall()
        
        # Convert to dictionary
        stats = {}
        for row in stats_result:
            flag, count, avg_speed, avg_fare, avg_distance = row
            stats[flag] = {
                'count': count,
                'avg_speed_mph': avg_speed,
                'avg_fare': avg_fare,
                'avg_distance': avg_distance
            }
        
        # Log statistics
        total_trips = sum(s['count'] for s in stats.values())
        clean_trips = stats.get('clean', {}).get('count', 0)
        ghost_trips = total_trips - clean_trips
        ghost_pct = (ghost_trips / total_trips * 100) if total_trips > 0 else 0
        
        logger.info(f"  Total trips: {total_trips:,}")
        logger.info(f"  Clean trips: {clean_trips:,}")
        logger.info(f"  Ghost trips: {ghost_trips:,} ({ghost_pct:.2f}%)")
        
        for flag, data in stats.items():
            if flag != 'clean':
                logger.warning(f"    {flag}: {data['count']:,} trips")
        
        con.close()
        return stats
        
    except Exception as e:
        logger.error(f"Failed to detect ghost trips in {input_path.name}: {e}")
        return {}


def clean_all_files() -> dict:
    """
    Detect ghost trips in all unified files.
    
    Returns:
        Dictionary with cleaning statistics
    """
    
    total_stats = {
        'files_processed': 0,
        'total_trips': 0,
        'clean_trips': 0,
        'ghost_trips': 0,
        'ghost_types': {}
    }
    
    logger.info("=" * 60)
    logger.info("Starting Ghost Trip Detection")
    logger.info("=" * 60)
    
    # Create output directory
    GHOST_TRIPS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Process all unified files
    unified_files = sorted(UNIFIED_DIR.glob("*.parquet"))
    
    for input_path in unified_files:
        # Define output paths
        output_clean_path = UNIFIED_DIR / input_path.name.replace('unified', 'clean')
        output_ghost_path = GHOST_TRIPS_DIR / input_path.name.replace('unified', 'ghost')
        
        # Skip if already processed
        if output_clean_path.exists():
            logger.info(f"Skipping {input_path.name} (already cleaned)")
            continue
        
        # Detect ghost trips
        stats = detect_ghost_trips(input_path, output_clean_path, output_ghost_path)
        
        if stats:
            total_stats['files_processed'] += 1
            
            # Aggregate statistics
            for flag, data in stats.items():
                total_stats['total_trips'] += data['count']
                
                if flag == 'clean':
                    total_stats['clean_trips'] += data['count']
                else:
                    total_stats['ghost_trips'] += data['count']
                    
                    if flag not in total_stats['ghost_types']:
                        total_stats['ghost_types'][flag] = 0
                    total_stats['ghost_types'][flag] += data['count']
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("Ghost Trip Detection Summary")
    logger.info("=" * 60)
    logger.info(f"Files processed: {total_stats['files_processed']}")
    logger.info(f"Total trips: {total_stats['total_trips']:,}")
    logger.info(f"Clean trips: {total_stats['clean_trips']:,}")
    logger.info(f"Ghost trips: {total_stats['ghost_trips']:,}")
    
    if total_stats['total_trips'] > 0:
        ghost_pct = (total_stats['ghost_trips'] / total_stats['total_trips'] * 100)
        logger.info(f"Ghost trip rate: {ghost_pct:.2f}%")
    
    logger.info("\nGhost trip breakdown:")
    for ghost_type, count in total_stats['ghost_types'].items():
        logger.info(f"  {ghost_type}: {count:,}")
    
    return total_stats


def analyze_ghost_patterns() -> None:
    """
    Analyze patterns in ghost trips for reporting.
    
    This creates a summary report of ghost trip patterns:
    - Most common ghost trip types
    - Time patterns (hour of day, day of week)
    - Location patterns (pickup/dropoff zones)
    """
    
    try:
        logger.info("\nAnalyzing ghost trip patterns...")
        
        con = duckdb.connect()
        
        # Combine all ghost trip files
        ghost_files = list(GHOST_TRIPS_DIR.glob("*.parquet"))
        
        if not ghost_files:
            logger.warning("No ghost trip files found")
            return
        
        # Create pattern analysis
        pattern_query = f"""
        SELECT 
            ghost_flag,
            COUNT(*) as count,
            ROUND(AVG(speed_mph), 2) as avg_speed,
            ROUND(AVG(fare), 2) as avg_fare,
            ROUND(AVG(trip_distance), 2) as avg_distance,
            ROUND(AVG(duration_seconds), 2) as avg_duration_sec
        FROM read_parquet('{GHOST_TRIPS_DIR}/*.parquet')
        GROUP BY ghost_flag
        ORDER BY count DESC
        """
        
        results = con.execute(pattern_query).fetchall()
        
        logger.info("\nGhost Trip Patterns:")
        logger.info("-" * 80)
        logger.info(f"{'Type':<30} {'Count':>10} {'Avg Speed':>12} {'Avg Fare':>10} {'Avg Dist':>10}")
        logger.info("-" * 80)
        
        for row in results:
            ghost_type, count, avg_speed, avg_fare, avg_dist, avg_dur = row
            logger.info(f"{ghost_type:<30} {count:>10,} {avg_speed:>12.1f} {avg_fare:>10.2f} {avg_dist:>10.2f}")
        
        con.close()
        
    except Exception as e:
        logger.error(f"Failed to analyze ghost patterns: {e}")


if __name__ == "__main__":
    """
    Run this module directly to detect ghost trips:
    
    python -m src.cleaning
    
    This will:
    1. Read all unified files
    2. Detect ghost trips using 5 rules
    3. Save clean trips to data/processed/unified/
    4. Save ghost trips to data/processed/ghost_trips/
    5. Generate statistics report
    
    Time: ~15-30 minutes depending on data size
    """
    
    # Clean all files
    stats = clean_all_files()
    
    # Analyze patterns
    analyze_ghost_patterns()
    
    # Exit
    if stats['files_processed'] > 0:
        logger.success("Ghost trip detection completed!")
        sys.exit(0)
    else:
        logger.error("No files were processed")
        sys.exit(1)
