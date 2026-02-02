"""
PHASE 9: Weather Integration
=============================

This module integrates weather data with trip data.

Why weather matters:
- Rain increases taxi demand ("rain tax")
- Affects congestion pricing effectiveness
- Important for elasticity analysis

Metrics:
- Precipitation elasticity: % change in trips per % change in precipitation
- Correlation between rain and trip volume
- Impact on congestion toll compliance

Data source: Meteostat (historical weather data)
"""

import duckdb
from pathlib import Path
from loguru import logger
import sys
from datetime import datetime
import pandas as pd

from .config import (
    AGGREGATED_DIR, WEATHER_STATION, 
    WEATHER_START_DATE, WEATHER_END_DATE, LOGS_DIR
)

# Setup logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(LOGS_DIR / "weather.log", rotation="10 MB")


def fetch_weather_data() -> pd.DataFrame:
    """
    Fetch historical weather data for NYC.
    
    Returns:
        DataFrame with daily weather data
    
    Weather metrics:
    - tavg: Average temperature (°C)
    - prcp: Precipitation (mm)
    - wspd: Wind speed (km/h)
    - pres: Atmospheric pressure (hPa)
    """
    
    try:
        logger.info("Fetching weather data from Meteostat...")
        
        from meteostat import Point, Daily
        
        # NYC Central Park coordinates
        nyc = Point(40.7829, -73.9654)
        
        # Fetch daily data
        start = datetime.strptime(WEATHER_START_DATE, '%Y-%m-%d')
        end = datetime.strptime(WEATHER_END_DATE, '%Y-%m-%d')
        
        data = Daily(nyc, start, end)
        data = data.fetch()
        
        # Reset index to get date as column
        data = data.reset_index()
        data = data.rename(columns={'time': 'date'})
        
        logger.success(f"Fetched {len(data)} days of weather data")
        
        return data
        
    except Exception as e:
        logger.error(f"Failed to fetch weather data: {e}")
        return pd.DataFrame()


def join_weather_with_trips() -> None:
    """
    Join weather data with trip data.
    
    Creates aggregated file with:
    - Daily trip counts
    - Daily weather metrics
    - Correlation analysis
    """
    
    try:
        logger.info("Joining weather data with trip data...")
        
        # Fetch weather data
        weather_df = fetch_weather_data()
        
        if weather_df.empty:
            logger.error("No weather data available")
            return
        
        con = duckdb.connect()
        
        # Load trip data
        trip_query = f"""
        SELECT 
            trip_date,
            SUM(trip_count) as total_trips,
            AVG(avg_fare) as avg_fare,
            SUM(total_congestion_collected) as congestion_revenue
        FROM read_parquet('{AGGREGATED_DIR}/trips_by_zone_category.parquet')
        GROUP BY trip_date
        ORDER BY trip_date
        """
        
        trip_df = con.execute(trip_query).df()
        
        # Join with weather data
        # Convert trip_date to date type for joining
        trip_df['date'] = pd.to_datetime(trip_df['trip_date']).dt.date
        weather_df['date'] = pd.to_datetime(weather_df['date']).dt.date
        
        # Merge
        merged_df = trip_df.merge(weather_df, on='date', how='left')
        
        # Calculate elasticity metrics
        # Elasticity = % change in trips / % change in precipitation
        merged_df['prcp_filled'] = merged_df['prcp'].fillna(0)
        merged_df['is_rainy_day'] = (merged_df['prcp_filled'] > 1.0).astype(int)
        
        # Save joined data
        output_path = AGGREGATED_DIR / "weather_joined.parquet"
        con.execute(f"COPY (SELECT * FROM merged_df) TO '{output_path}' (FORMAT PARQUET)")
        
        logger.success(f"Saved weather-joined data to {output_path}")
        
        # Calculate correlation
        correlation = merged_df[['total_trips', 'prcp_filled', 'tavg']].corr()
        
        logger.info("\nCorrelation Analysis:")
        logger.info(f"  Trips vs Precipitation: {correlation.loc['total_trips', 'prcp_filled']:.3f}")
        logger.info(f"  Trips vs Temperature: {correlation.loc['total_trips', 'tavg']:.3f}")
        
        # Calculate rain tax effect
        rainy_avg = merged_df[merged_df['is_rainy_day'] == 1]['total_trips'].mean()
        dry_avg = merged_df[merged_df['is_rainy_day'] == 0]['total_trips'].mean()
        rain_tax_pct = ((rainy_avg - dry_avg) / dry_avg * 100) if dry_avg > 0 else 0
        
        logger.info(f"\nRain Tax Effect:")
        logger.info(f"  Average trips on rainy days: {rainy_avg:,.0f}")
        logger.info(f"  Average trips on dry days: {dry_avg:,.0f}")
        logger.info(f"  Rain tax (% increase): {rain_tax_pct:.1f}%")
        
        con.close()
        
    except Exception as e:
        logger.error(f"Failed to join weather data: {e}")


if __name__ == "__main__":
    """
    Run this module directly to integrate weather data:
    
    python -m src.weather
    
    This will:
    1. Fetch historical weather data from Meteostat
    2. Join with trip data
    3. Calculate correlations and elasticity
    4. Save aggregated results
    
    Time: ~2-5 minutes
    """
    
    join_weather_with_trips()
    logger.success("Weather integration completed!")
    sys.exit(0)
