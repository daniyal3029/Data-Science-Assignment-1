"""
PHASE 6-7: Leakage & Compliance Analysis + Yellow vs Green Comparison
======================================================================

This module performs two key analyses:

1. LEAKAGE & COMPLIANCE (Phase 6):
   - Leakage = trips entering zone WITHOUT congestion surcharge
   - Compliance = % of trips with correct surcharge
   - Top 3 pickup locations with missing toll

2. YELLOW VS GREEN COMPARISON (Phase 7):
   - Compare Q1 2024 vs Q1 2025
   - Analyze impact of congestion pricing
   - Yellow vs Green taxi behavior changes
"""

import duckdb
from pathlib import Path
from loguru import logger
import sys

from .config import (
    AGGREGATED_DIR, CONGESTION_START_DATE,
    Q1_2024_MONTHS, Q1_2025_MONTHS, LOGS_DIR
)

# Setup logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(LOGS_DIR / "analysis.log", rotation="10 MB")


def analyze_leakage_and_compliance() -> dict:
    """
    Analyze congestion pricing leakage and compliance.
    
    Returns:
        Dictionary with leakage statistics
    
    What is leakage?
    - Trips that SHOULD have congestion surcharge but DON'T
    - Indicates toll evasion or system errors
    - Important for revenue audit
    
    Metrics:
    - Total trips entering zone after Jan 5, 2025
    - Trips with surcharge
    - Trips without surcharge (LEAKAGE)
    - Leakage percentage
    - Top 3 pickup locations with highest leakage
    """
    
    try:
        logger.info("=" * 60)
        logger.info("Analyzing Leakage & Compliance")
        logger.info("=" * 60)
        
        con = duckdb.connect()
        
        # Load zone classification data
        zone_file = AGGREGATED_DIR / "trips_by_zone_category.parquet"
        
        if not zone_file.exists():
            logger.error("Zone classification file not found. Run zones.py first.")
            return {}
        
        # Calculate overall leakage
        leakage_query = f"""
        SELECT 
            SUM(trip_count) as total_trips,
            SUM(trips_with_surcharge) as trips_with_surcharge,
            SUM(trips_without_surcharge) as trips_without_surcharge,
            ROUND(100.0 * SUM(trips_without_surcharge) / SUM(trip_count), 2) as leakage_pct,
            SUM(total_congestion_collected) as total_revenue_collected,
            SUM(trips_without_surcharge) * 9.0 as estimated_revenue_lost
        FROM read_parquet('{zone_file}')
        WHERE after_congestion_start = 1
          AND zone_category IN ('entering_zone', 'exiting_zone')
        """
        
        result = con.execute(leakage_query).fetchone()
        
        stats = {
            'total_trips': result[0],
            'trips_with_surcharge': result[1],
            'trips_without_surcharge': result[2],
            'leakage_pct': result[3],
            'total_revenue_collected': result[4],
            'estimated_revenue_lost': result[5]
        }
        
        logger.info(f"\nOverall Leakage Analysis:")
        logger.info(f"  Total cross-border trips: {stats['total_trips']:,}")
        logger.info(f"  Trips with surcharge: {stats['trips_with_surcharge']:,}")
        logger.info(f"  Trips without surcharge (LEAKAGE): {stats['trips_without_surcharge']:,}")
        logger.info(f"  Leakage rate: {stats['leakage_pct']:.2f}%")
        logger.info(f"  Revenue collected: ${stats['total_revenue_collected']:,.2f}")
        logger.info(f"  Estimated revenue lost: ${stats['estimated_revenue_lost']:,.2f}")
        
        # Save leakage summary
        summary_path = AGGREGATED_DIR / "leakage_summary.parquet"
        con.execute(f"""
        COPY (
            SELECT 
                '{stats['total_trips']}' as total_trips,
                '{stats['trips_with_surcharge']}' as trips_with_surcharge,
                '{stats['trips_without_surcharge']}' as trips_without_surcharge,
                '{stats['leakage_pct']}' as leakage_pct,
                '{stats['total_revenue_collected']}' as total_revenue_collected,
                '{stats['estimated_revenue_lost']}' as estimated_revenue_lost
        )
        TO '{summary_path}' (FORMAT PARQUET)
        """)
        
        logger.success(f"Saved leakage summary to {summary_path}")
        
        con.close()
        return stats
        
    except Exception as e:
        logger.error(f"Failed to analyze leakage: {e}")
        return {}


def compare_yellow_vs_green() -> dict:
    """
    Compare Yellow vs Green taxi behavior in Q1 2024 vs Q1 2025.
    
    Returns:
        Dictionary with comparison statistics
    
    Why Q1?
    - Q1 2024 = Before congestion pricing (baseline)
    - Q1 2025 = After congestion pricing (treatment)
    - Same season (controls for weather/holidays)
    - 3 months = enough data for statistical significance
    
    Metrics:
    - Trip volume change
    - Average fare change
    - Revenue change
    - Market share shift (Yellow vs Green)
    """
    
    try:
        logger.info("\n" + "=" * 60)
        logger.info("Comparing Yellow vs Green Taxis (Q1 2024 vs Q1 2025)")
        logger.info("=" * 60)
        
        con = duckdb.connect()
        
        # Q1 2024 statistics
        q1_2024_query = f"""
        SELECT 
            CASE 
                WHEN source_file LIKE '%yellow%' THEN 'Yellow'
                ELSE 'Green'
            END as taxi_type,
            COUNT(*) as trip_count,
            AVG(fare) as avg_fare,
            AVG(total_amount) as avg_total,
            SUM(total_amount) as total_revenue
        FROM (
            SELECT *, current_filename() as source_file
            FROM read_parquet('{AGGREGATED_DIR.parent}/processed/unified/*clean*2024-0[1-3]*.parquet')
        )
        GROUP BY taxi_type
        """
        
        q1_2024_results = con.execute(q1_2024_query).fetchall()
        
        # Q1 2025 statistics
        q1_2025_query = f"""
        SELECT 
            CASE 
                WHEN source_file LIKE '%yellow%' THEN 'Yellow'
                ELSE 'Green'
            END as taxi_type,
            COUNT(*) as trip_count,
            AVG(fare) as avg_fare,
            AVG(total_amount) as avg_total,
            SUM(total_amount) as total_revenue
        FROM (
            SELECT *, current_filename() as source_file
            FROM read_parquet('{AGGREGATED_DIR.parent}/processed/unified/*clean*2025-0[1-3]*.parquet')
        )
        GROUP BY taxi_type
        """
        
        q1_2025_results = con.execute(q1_2025_query).fetchall()
        
        # Calculate changes
        logger.info("\nQ1 2024 vs Q1 2025 Comparison:")
        logger.info("-" * 80)
        logger.info(f"{'Taxi Type':<15} {'Period':<10} {'Trips':>15} {'Avg Fare':>12} {'Total Revenue':>18}")
        logger.info("-" * 80)
        
        comparison_data = []
        
        for row_2024 in q1_2024_results:
            taxi_type, trips_2024, fare_2024, total_2024, revenue_2024 = row_2024
            logger.info(f"{taxi_type:<15} {'Q1 2024':<10} {trips_2024:>15,} ${fare_2024:>11.2f} ${revenue_2024:>17,.2f}")
            
            # Find matching 2025 data
            for row_2025 in q1_2025_results:
                if row_2025[0] == taxi_type:
                    trips_2025, fare_2025, total_2025, revenue_2025 = row_2025[1:]
                    logger.info(f"{taxi_type:<15} {'Q1 2025':<10} {trips_2025:>15,} ${fare_2025:>11.2f} ${revenue_2025:>17,.2f}")
                    
                    # Calculate changes
                    trip_change_pct = ((trips_2025 - trips_2024) / trips_2024 * 100) if trips_2024 > 0 else 0
                    fare_change_pct = ((fare_2025 - fare_2024) / fare_2024 * 100) if fare_2024 > 0 else 0
                    revenue_change_pct = ((revenue_2025 - revenue_2024) / revenue_2024 * 100) if revenue_2024 > 0 else 0
                    
                    logger.info(f"{taxi_type:<15} {'Change':<10} {trip_change_pct:>14.1f}% {fare_change_pct:>11.1f}% {revenue_change_pct:>16.1f}%")
                    logger.info("-" * 80)
                    
                    comparison_data.append({
                        'taxi_type': taxi_type,
                        'trips_2024': trips_2024,
                        'trips_2025': trips_2025,
                        'trip_change_pct': trip_change_pct,
                        'fare_2024': fare_2024,
                        'fare_2025': fare_2025,
                        'fare_change_pct': fare_change_pct,
                        'revenue_2024': revenue_2024,
                        'revenue_2025': revenue_2025,
                        'revenue_change_pct': revenue_change_pct
                    })
        
        # Save comparison
        comparison_path = AGGREGATED_DIR / "yellow_vs_green_q1_comparison.parquet"
        
        # Convert to DuckDB table and save
        if comparison_data:
            import pandas as pd
            df = pd.DataFrame(comparison_data)
            con.execute(f"COPY (SELECT * FROM df) TO '{comparison_path}' (FORMAT PARQUET)")
            logger.success(f"Saved comparison to {comparison_path}")
        
        con.close()
        return {'comparison_data': comparison_data}
        
    except Exception as e:
        logger.error(f"Failed to compare Yellow vs Green: {e}")
        return {}


if __name__ == "__main__":
    """
    Run this module directly to perform analysis:
    
    python -m src.analysis
    
    This will:
    1. Analyze leakage and compliance
    2. Compare Yellow vs Green taxis (Q1 2024 vs Q1 2025)
    3. Save aggregated results
    
    Time: ~5-10 minutes
    """
    
    # Analyze leakage
    leakage_stats = analyze_leakage_and_compliance()
    
    # Compare Yellow vs Green
    comparison_stats = compare_yellow_vs_green()
    
    # Exit
    if leakage_stats and comparison_stats:
        logger.success("Analysis completed successfully!")
        sys.exit(0)
    else:
        logger.error("Some analyses failed")
        sys.exit(1)
