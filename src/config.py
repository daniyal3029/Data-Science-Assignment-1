"""
Configuration file for NYC Congestion Pricing Audit
====================================================

This file centralizes all constants, paths, and configuration settings.
Think of it as the "control panel" for the entire pipeline.

Why separate config?
- Easy to modify settings without touching logic
- Single source of truth
- Prevents hardcoded values scattered across files
"""

from pathlib import Path
from datetime import datetime

# ============================================
# PROJECT PATHS
# ============================================

# Root directory (parent of src/)
PROJECT_ROOT = Path(__file__).parent.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
AGGREGATED_DIR = DATA_DIR / "aggregated"

# Specific raw data folders
YELLOW_RAW_DIR = RAW_DIR / "yellow"
GREEN_RAW_DIR = RAW_DIR / "green"
TAXI_ZONES_DIR = RAW_DIR / "taxi_zones"

# Processed data folders
UNIFIED_DIR = PROCESSED_DIR / "unified"
GHOST_TRIPS_DIR = PROCESSED_DIR / "ghost_trips"

# Output directories
OUTPUT_DIR = PROJECT_ROOT / "outputs"
FIGURES_DIR = OUTPUT_DIR / "figures"
LOGS_DIR = OUTPUT_DIR / "logs"

# ============================================
# TLC DATA SOURCES
# ============================================

# NYC TLC Trip Record Data
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# File naming patterns
YELLOW_PATTERN = "yellow_tripdata_{year}-{month:02d}.parquet"
GREEN_PATTERN = "green_tripdata_{year}-{month:02d}.parquet"

# Taxi zone shapefile
TAXI_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

# ============================================
# TIME PERIODS
# ============================================

# Analysis periods
YEARS_TO_DOWNLOAD = [2023, 2024, 2025]  # For Dec imputation + analysis
MONTHS_TO_DOWNLOAD = list(range(1, 13))  # Jan-Dec

# Congestion pricing start date
CONGESTION_START_DATE = datetime(2025, 1, 5)

# Comparison periods
Q1_2024_MONTHS = [1, 2, 3]  # Jan-Mar 2024 (before)
Q1_2025_MONTHS = [1, 2, 3]  # Jan-Mar 2025 (after)

# ============================================
# GHOST TRIP DETECTION THRESHOLDS
# ============================================

# Speed threshold (mph)
MAX_SPEED_MPH = 65  # NYC speed limit is 25-50 mph; 65+ is suspicious

# Fare anomalies
MIN_TRIP_DURATION_SECONDS = 60  # 1 minute
HIGH_FARE_THRESHOLD = 20  # $20 for very short trips is suspicious

# Distance anomalies
MIN_DISTANCE_MILES = 0.01  # Trips with 0 distance but fare > 0 are errors

# ============================================
# CONGESTION ZONE DEFINITION
# ============================================

# Manhattan south of 60th Street
# These are TLC LocationIDs (we'll validate against shapefile)
CONGESTION_ZONE_BOUNDARY = "60th Street"

# We'll identify zones programmatically, but for reference:
# Manhattan zones below 60th St include: 
# - Financial District, Midtown, Lower East Side, etc.
# - Excludes: Upper East Side, Upper West Side, Harlem, etc.

# ============================================
# SCHEMA MAPPING
# ============================================

# Target unified schema
UNIFIED_SCHEMA = [
    "pickup_time",
    "dropoff_time",
    "pickup_loc",
    "dropoff_loc",
    "trip_distance",
    "fare",
    "total_amount",
    "congestion_surcharge"
]

# Yellow taxi column mapping (TLC schema -> our schema)
YELLOW_COLUMN_MAP = {
    "tpep_pickup_datetime": "pickup_time",
    "tpep_dropoff_datetime": "dropoff_time",
    "PULocationID": "pickup_loc",
    "DOLocationID": "dropoff_loc",
    "trip_distance": "trip_distance",
    "fare_amount": "fare",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge"
}

# Green taxi column mapping (TLC schema -> our schema)
GREEN_COLUMN_MAP = {
    "lpep_pickup_datetime": "pickup_time",
    "lpep_dropoff_datetime": "dropoff_time",
    "PULocationID": "pickup_loc",
    "DOLocationID": "dropoff_loc",
    "trip_distance": "trip_distance",
    "fare_amount": "fare",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge"
}

# ============================================
# MISSING DATA IMPUTATION
# ============================================

# Weighted average for missing December 2025
# Formula: Dec2025 = 0.3 × Dec2023 + 0.7 × Dec2024
DEC_2023_WEIGHT = 0.3
DEC_2024_WEIGHT = 0.7

# Why these weights?
# - Dec 2024 is more recent (higher weight)
# - Dec 2023 captures year-over-year trends
# - 70/30 split balances recency vs. historical pattern

# ============================================
# LOGGING
# ============================================

LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"
LOG_FILE = LOGS_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# ============================================
# DUCKDB SETTINGS
# ============================================

# Memory limit (prevents crashes on large datasets)
DUCKDB_MEMORY_LIMIT = "4GB"  # Adjust based on your machine

# Number of threads (use all available cores)
DUCKDB_THREADS = -1  # -1 = auto-detect

# ============================================
# WEATHER DATA
# ============================================

# NYC weather station (Central Park)
WEATHER_STATION = "72505"  # NOAA station ID
WEATHER_START_DATE = "2023-01-01"
WEATHER_END_DATE = "2025-12-31"

# ============================================
# VISUALIZATION SETTINGS
# ============================================

# Chart dimensions
FIGURE_WIDTH = 1200
FIGURE_HEIGHT = 600

# Color scheme (professional, colorblind-friendly)
COLOR_YELLOW = "#FFD700"  # Yellow taxi
COLOR_GREEN = "#2ECC71"   # Green taxi
COLOR_CONGESTION = "#E74C3C"  # Congestion zone
COLOR_NEUTRAL = "#34495E"  # Neutral/background

# ============================================
# HELPER FUNCTION
# ============================================

def create_directories():
    """
    Create all necessary directories if they don't exist.
    Call this at the start of pipeline.py
    """
    dirs = [
        RAW_DIR, YELLOW_RAW_DIR, GREEN_RAW_DIR, TAXI_ZONES_DIR,
        PROCESSED_DIR, UNIFIED_DIR, GHOST_TRIPS_DIR,
        AGGREGATED_DIR, OUTPUT_DIR, FIGURES_DIR, LOGS_DIR
    ]
    for dir_path in dirs:
        dir_path.mkdir(parents=True, exist_ok=True)
    print("✓ All directories created/verified")


if __name__ == "__main__":
    # Test configuration
    print("=" * 50)
    print("NYC Congestion Pricing Audit - Configuration")
    print("=" * 50)
    print(f"\nProject Root: {PROJECT_ROOT}")
    print(f"Data Directory: {DATA_DIR}")
    print(f"DuckDB Memory Limit: {DUCKDB_MEMORY_LIMIT}")
    print(f"\nTarget Schema: {UNIFIED_SCHEMA}")
    print(f"\nGhost Trip Thresholds:")
    print(f"  - Max Speed: {MAX_SPEED_MPH} mph")
    print(f"  - Min Duration: {MIN_TRIP_DURATION_SECONDS} seconds")
    print(f"  - High Fare: ${HIGH_FARE_THRESHOLD}")
    print("\n✓ Configuration loaded successfully")
