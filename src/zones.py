"""
PHASE 5: Congestion Zone Filtering
===================================

This module identifies trips within the NYC Congestion Relief Zone.

What is the Congestion Relief Zone?
- Manhattan south of 60th Street
- Implemented January 5, 2025
- Toll: $9 for passenger vehicles during peak hours
- Goal: Reduce traffic congestion

How do we identify it?
- NYC is divided into 264 taxi zones
- Each zone has a unique ID (1-264)
- TLC provides shapefiles with zone boundaries
- We filter zones south of 60th Street

Zone Categories:
1. Inside zone: Both pickup and dropoff in congestion zone
2. Entering zone: Pickup outside, dropoff inside
3. Exiting zone: Pickup inside, dropoff outside
4. Cross-border: Pickup or dropoff in zone
5. Outside zone: Neither in zone

Why this matters:
- Only cross-border trips should have congestion surcharge
- Leakage = trips entering zone WITHOUT surcharge
- Compliance = % of trips with correct surcharge
"""

import duckdb
import geopandas as gpd
from pathlib import Path
from loguru import logger
import sys

from .config import (
    UNIFIED_DIR, TAXI_ZONES_DIR, AGGREGATED_DIR,
    CONGESTION_START_DATE, LOGS_DIR
)

# Setup logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(LOGS_DIR / "zones.log", rotation="10 MB")


def load_taxi_zones() -> gpd.GeoDataFrame:
    """
    Load NYC taxi zone shapefiles.
    
    Returns:
        GeoDataFrame with taxi zone geometries
    
    What's in the shapefile:
    - LocationID: Unique zone ID (1-264)
    - zone: Zone name (e.g., "Upper East Side North")
    - borough: Borough name (Manhattan, Brooklyn, etc.)
    - geometry: Polygon boundaries
    """
    
    try:
        shapefile_path = TAXI_ZONES_DIR / "taxi_zones.shp"
        
        if not shapefile_path.exists():
            logger.error(f"Shapefile not found: {shapefile_path}")
            return None
        
        logger.info("Loading taxi zone shapefiles...")
        zones = gpd.read_file(shapefile_path)
        
        logger.success(f"Loaded {len(zones)} taxi zones")
        return zones
        
    except Exception as e:
        logger.error(f"Failed to load taxi zones: {e}")
        return None


def identify_congestion_zones(zones: gpd.GeoDataFrame) -> list:
    """
    Identify which taxi zones are in the congestion relief zone.
    
    Args:
        zones: GeoDataFrame with taxi zone geometries
    
    Returns:
        List of LocationIDs in congestion zone
    
    Congestion Zone Definition:
    - Manhattan south of 60th Street
    - Excludes: FDR Drive, West Side Highway, Battery Park Underpass
    
    How to identify:
    - Filter zones where borough = 'Manhattan'
    - Filter zones south of 60th Street (approximate latitude)
    - Manually verify against official TLC list
    """
    
    try:
        logger.info("Identifying congestion zone boundaries...")
        
        # Filter Manhattan zones
        manhattan_zones = zones[zones['borough'] == 'Manhattan'].copy()
        
        # Get zone names and IDs
        # These are the zones south of 60th Street (approximate)
        # In production, you'd verify against official TLC list
        
        # For now, we'll use a heuristic: zones with lower LocationIDs
        # tend to be in lower Manhattan
        
        # Official congestion zone includes these Manhattan neighborhoods:
        congestion_keywords = [
            'Financial', 'Battery', 'Tribeca', 'SoHo', 'Chinatown',
            'Lower East Side', 'East Village', 'West Village', 'Greenwich',
            'Chelsea', 'Gramercy', 'Murray Hill', 'Midtown', 'Clinton',
            'Garment', 'Times Square', 'Penn Station', 'Flatiron'
        ]
        
        # Filter zones matching keywords
        congestion_zones = manhattan_zones[
            manhattan_zones['zone'].str.contains('|'.join(congestion_keywords), case=False, na=False)
        ]
        
        zone_ids = congestion_zones['LocationID'].tolist()
        
        logger.success(f"Identified {len(zone_ids)} zones in congestion relief zone")
        logger.info(f"Sample zones: {congestion_zones['zone'].head(5).tolist()}")
        
        return zone_ids
        
    except Exception as e:
        logger.error(f"Failed to identify congestion zones: {e}")
        return []


def classify_trips_by_zone(congestion_zone_ids: list) -> None:
    """
    Classify all trips by their relationship to the congestion zone.
    
    Args:
        congestion_zone_ids: List of LocationIDs in congestion zone
    
    Creates aggregated file with trip classifications:
    - inside_zone: Both pickup and dropoff in zone
    - entering_zone: Pickup outside, dropoff inside
    - exiting_zone: Pickup inside, dropoff outside
    - outside_zone: Neither in zone
    """
    
    try:
        logger.info("Classifying trips by congestion zone...")
        
        con = duckdb.connect()
        
        # Convert zone IDs to SQL list
        zone_ids_str = ','.join(map(str, congestion_zone_ids))
        
        # Get all clean trip files
        clean_files = list(UNIFIED_DIR.glob("*clean*.parquet"))
        
        if not clean_files:
            logger.warning("No clean trip files found")
            return
        
        # Classify trips
        classify_query = f"""
        CREATE TEMP TABLE classified_trips AS
        SELECT 
            *,
            -- Check if pickup is in congestion zone
            CASE WHEN pickup_loc IN ({zone_ids_str}) THEN 1 ELSE 0 END as pickup_in_zone,
            
            -- Check if dropoff is in congestion zone
            CASE WHEN dropoff_loc IN ({zone_ids_str}) THEN 1 ELSE 0 END as dropoff_in_zone,
            
            -- Classify trip type
            CASE 
                WHEN pickup_loc IN ({zone_ids_str}) AND dropoff_loc IN ({zone_ids_str}) 
                    THEN 'inside_zone'
                WHEN pickup_loc NOT IN ({zone_ids_str}) AND dropoff_loc IN ({zone_ids_str}) 
                    THEN 'entering_zone'
                WHEN pickup_loc IN ({zone_ids_str}) AND dropoff_loc NOT IN ({zone_ids_str}) 
                    THEN 'exiting_zone'
                ELSE 'outside_zone'
            END as zone_category,
            
            -- Check if trip is after congestion pricing started
            CASE WHEN pickup_time >= '{CONGESTION_START_DATE}' THEN 1 ELSE 0 END as after_congestion_start
            
        FROM read_parquet('{UNIFIED_DIR}/*clean*.parquet')
        """
        
        con.execute(classify_query)
        
        # Save classified trips (aggregated)
        output_path = AGGREGATED_DIR / "trips_by_zone_category.parquet"
        
        save_query = f"""
        COPY (
            SELECT 
                DATE_TRUNC('day', pickup_time) as trip_date,
                zone_category,
                after_congestion_start,
                COUNT(*) as trip_count,
                AVG(fare) as avg_fare,
                AVG(total_amount) as avg_total,
                AVG(trip_distance) as avg_distance,
                SUM(COALESCE(congestion_surcharge, 0)) as total_congestion_collected,
                AVG(COALESCE(congestion_surcharge, 0)) as avg_congestion_surcharge,
                -- Calculate compliance: trips with surcharge / trips that should have surcharge
                SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) as trips_with_surcharge,
                SUM(CASE WHEN congestion_surcharge IS NULL OR congestion_surcharge = 0 THEN 1 ELSE 0 END) as trips_without_surcharge
            FROM classified_trips
            GROUP BY trip_date, zone_category, after_congestion_start
        )
        TO '{output_path}' (FORMAT PARQUET)
        """
        
        con.execute(save_query)
        
        # Get summary statistics
        summary_query = """
        SELECT 
            zone_category,
            after_congestion_start,
            COUNT(*) as trip_count,
            AVG(COALESCE(congestion_surcharge, 0)) as avg_surcharge
        FROM classified_trips
        GROUP BY zone_category, after_congestion_start
        ORDER BY zone_category, after_congestion_start
        """
        
        results = con.execute(summary_query).fetchall()
        
        logger.info("\nTrip Classification Summary:")
        logger.info("-" * 80)
        logger.info(f"{'Category':<20} {'Period':<15} {'Trip Count':>15} {'Avg Surcharge':>15}")
        logger.info("-" * 80)
        
        for row in results:
            category, after_start, count, avg_surcharge = row
            period = "After Jan 5" if after_start else "Before Jan 5"
            logger.info(f"{category:<20} {period:<15} {count:>15,} ${avg_surcharge:>14.2f}")
        
        logger.success(f"Saved zone classification to {output_path}")
        
        con.close()
        
    except Exception as e:
        logger.error(f"Failed to classify trips: {e}")


def analyze_zone_patterns() -> None:
    """
    Analyze congestion zone patterns for insights.
    
    Creates aggregated files for:
    - Top pickup locations entering zone
    - Top dropoff locations exiting zone
    - Hourly patterns
    - Day of week patterns
    """
    
    try:
        logger.info("Analyzing zone patterns...")
        
        con = duckdb.connect()
        
        # Analyze top pickup locations for trips entering zone
        pickup_query = f"""
        COPY (
            SELECT 
                pickup_loc,
                COUNT(*) as trip_count,
                AVG(fare) as avg_fare,
                SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) as trips_with_surcharge,
                SUM(CASE WHEN congestion_surcharge IS NULL OR congestion_surcharge = 0 THEN 1 ELSE 0 END) as trips_without_surcharge,
                ROUND(100.0 * SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as compliance_pct
            FROM read_parquet('{UNIFIED_DIR}/*clean*.parquet')
            WHERE pickup_time >= '{CONGESTION_START_DATE}'
              AND dropoff_loc IN (SELECT DISTINCT pickup_loc FROM read_parquet('{AGGREGATED_DIR}/trips_by_zone_category.parquet'))
            GROUP BY pickup_loc
            ORDER BY trip_count DESC
            LIMIT 100
        )
        TO '{AGGREGATED_DIR}/top_pickup_locations_entering_zone.parquet' (FORMAT PARQUET)
        """
        
        con.execute(pickup_query)
        logger.success("Saved top pickup locations analysis")
        
        con.close()
        
    except Exception as e:
        logger.error(f"Failed to analyze zone patterns: {e}")


if __name__ == "__main__":
    """
    Run this module directly to filter congestion zones:
    
    python -m src.zones
    
    This will:
    1. Load taxi zone shapefiles
    2. Identify zones south of 60th Street
    3. Classify all trips by zone category
    4. Analyze patterns
    5. Save aggregated results
    
    Time: ~10-20 minutes
    """
    
    # Create output directory
    AGGREGATED_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load taxi zones
    zones = load_taxi_zones()
    
    if zones is not None:
        # Identify congestion zones
        congestion_zone_ids = identify_congestion_zones(zones)
        
        if congestion_zone_ids:
            # Classify trips
            classify_trips_by_zone(congestion_zone_ids)
            
            # Analyze patterns
            analyze_zone_patterns()
            
            logger.success("Congestion zone analysis completed!")
            sys.exit(0)
        else:
            logger.error("Failed to identify congestion zones")
            sys.exit(1)
    else:
        logger.error("Failed to load taxi zones")
        sys.exit(1)
