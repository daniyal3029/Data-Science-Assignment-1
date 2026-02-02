"""
PHASE 8: Visual Audit (Matplotlib Version)
===========================================

This module generates static charts using matplotlib and seaborn.

Visualizations:
1. Time Series - Trip volume over time
2. Revenue Analysis - Congestion toll revenue trends
3. Zone Category Distribution - Trips by zone
4. Leakage Analysis - Compliance tracking

Output: PNG images (high resolution, 300 DPI)
"""

import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from loguru import logger
import sys
import pandas as pd
import numpy as np

from src.config import (
    AGGREGATED_DIR, FIGURES_DIR, LOGS_DIR,
    COLOR_YELLOW, COLOR_GREEN, COLOR_CONGESTION,
    FIGURE_WIDTH, FIGURE_HEIGHT
)

# Setup logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(LOGS_DIR / "visualization.log", rotation="10 MB")

# Set matplotlib style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# DPI for high-quality images
DPI = 300
FIGSIZE = (12, 6)


def create_time_series_chart() -> None:
    """
    Create time series chart showing trip volume over time.
    """
    
    try:
        logger.info("Creating time series chart...")
        
        con = duckdb.connect()
        
        # Aggregate daily trips
        query = f"""
        SELECT 
            trip_date,
            SUM(trip_count) as total_trips
        FROM read_parquet('{AGGREGATED_DIR}/trips_by_zone_category.parquet')
        GROUP BY trip_date
        ORDER BY trip_date
        """
        
        df = con.execute(query).df()
        
        # Create figure
        fig, ax = plt.subplots(figsize=FIGSIZE, dpi=DPI)
        
        # Plot line chart
        ax.plot(df['trip_date'], df['total_trips'], 
                color='#1f77b4', linewidth=2, marker='o', markersize=4)
        
        # Formatting
        ax.set_title('NYC Taxi Trip Volume Over Time', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel('Daily Trip Count', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3)
        
        # Format y-axis with thousands separator
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        
        # Rotate x-axis labels
        plt.xticks(rotation=45, ha='right')
        
        # Tight layout
        plt.tight_layout()
        
        # Save
        output_path = FIGURES_DIR / "time_series_trips.png"
        plt.savefig(output_path, dpi=DPI, bbox_inches='tight')
        plt.close()
        
        logger.success(f"Saved time series chart to {output_path}")
        
        con.close()
        
    except Exception as e:
        logger.error(f"Failed to create time series chart: {e}")


def create_revenue_chart() -> None:
    """
    Create revenue analysis chart.
    """
    
    try:
        logger.info("Creating revenue chart...")
        
        con = duckdb.connect()
        
        # Aggregate daily revenue
        query = f"""
        SELECT 
            trip_date,
            SUM(total_congestion_collected) as daily_revenue
        FROM read_parquet('{AGGREGATED_DIR}/trips_by_zone_category.parquet')
        WHERE after_congestion_start = 1
        GROUP BY trip_date
        ORDER BY trip_date
        """
        
        df = con.execute(query).df()
        df['cumulative_revenue'] = df['daily_revenue'].cumsum()
        
        # Create figure with dual y-axis
        fig, ax1 = plt.subplots(figsize=FIGSIZE, dpi=DPI)
        
        # Bar chart for daily revenue
        ax1.bar(df['trip_date'], df['daily_revenue'], 
                color='#2ECC71', alpha=0.7, label='Daily Revenue')
        ax1.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Daily Revenue ($)', fontsize=12, fontweight='bold', color='#2ECC71')
        ax1.tick_params(axis='y', labelcolor='#2ECC71')
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${int(x):,}'))
        
        # Line chart for cumulative revenue
        ax2 = ax1.twinx()
        ax2.plot(df['trip_date'], df['cumulative_revenue'], 
                color='#E74C3C', linewidth=3, marker='o', markersize=5, label='Cumulative Revenue')
        ax2.set_ylabel('Cumulative Revenue ($)', fontsize=12, fontweight='bold', color='#E74C3C')
        ax2.tick_params(axis='y', labelcolor='#E74C3C')
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${int(x):,}'))
        
        # Title
        ax1.set_title('Congestion Toll Revenue Analysis', fontsize=16, fontweight='bold', pad=20)
        
        # Grid
        ax1.grid(True, alpha=0.3)
        
        # Rotate x-axis labels
        plt.xticks(rotation=45, ha='right')
        
        # Legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
        
        # Tight layout
        plt.tight_layout()
        
        # Save
        output_path = FIGURES_DIR / "revenue_analysis.png"
        plt.savefig(output_path, dpi=DPI, bbox_inches='tight')
        plt.close()
        
        logger.success(f"Saved revenue chart to {output_path}")
        
        con.close()
        
    except Exception as e:
        logger.error(f"Failed to create revenue chart: {e}")


def create_zone_category_chart() -> None:
    """
    Create chart showing trip distribution by zone category.
    """
    
    try:
        logger.info("Creating zone category chart...")
        
        con = duckdb.connect()
        
        # Aggregate by zone category
        query = f"""
        SELECT 
            zone_category,
            CASE WHEN after_congestion_start = 1 THEN 'After Jan 5' ELSE 'Before Jan 5' END as period,
            SUM(trip_count) as total_trips
        FROM read_parquet('{AGGREGATED_DIR}/trips_by_zone_category.parquet')
        GROUP BY zone_category, period
        ORDER BY zone_category, period
        """
        
        df = con.execute(query).df()
        
        # Create grouped bar chart
        fig, ax = plt.subplots(figsize=FIGSIZE, dpi=DPI)
        
        # Prepare data for grouped bars
        categories = df['zone_category'].unique()
        x = np.arange(len(categories))
        width = 0.35
        
        before_data = df[df['period'] == 'Before Jan 5'].set_index('zone_category')['total_trips']
        after_data = df[df['period'] == 'After Jan 5'].set_index('zone_category')['total_trips']
        
        # Create bars
        ax.bar(x - width/2, [before_data.get(cat, 0) for cat in categories], 
               width, label='Before Jan 5', color='#FFD700', alpha=0.8)
        ax.bar(x + width/2, [after_data.get(cat, 0) for cat in categories], 
               width, label='After Jan 5', color='#E74C3C', alpha=0.8)
        
        # Formatting
        ax.set_title('Trip Distribution by Congestion Zone Category', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Zone Category', fontsize=12, fontweight='bold')
        ax.set_ylabel('Total Trips', fontsize=12, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(categories, rotation=45, ha='right')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        # Tight layout
        plt.tight_layout()
        
        # Save
        output_path = FIGURES_DIR / "zone_category_distribution.png"
        plt.savefig(output_path, dpi=DPI, bbox_inches='tight')
        plt.close()
        
        logger.success(f"Saved zone category chart to {output_path}")
        
        con.close()
        
    except Exception as e:
        logger.error(f"Failed to create zone category chart: {e}")


def create_leakage_chart() -> None:
    """
    Create chart showing leakage analysis.
    """
    
    try:
        logger.info("Creating leakage chart...")
        
        con = duckdb.connect()
        
        # Daily leakage rate
        query = f"""
        SELECT 
            trip_date,
            SUM(trips_with_surcharge) as with_surcharge,
            SUM(trips_without_surcharge) as without_surcharge
        FROM read_parquet('{AGGREGATED_DIR}/trips_by_zone_category.parquet')
        WHERE after_congestion_start = 1
        GROUP BY trip_date
        ORDER BY trip_date
        """
        
        df = con.execute(query).df()
        
        # Create stacked area chart
        fig, ax = plt.subplots(figsize=FIGSIZE, dpi=DPI)
        
        # Plot stacked areas
        ax.fill_between(df['trip_date'], 0, df['with_surcharge'], 
                        color='#2ECC71', alpha=0.7, label='With Surcharge')
        ax.fill_between(df['trip_date'], df['with_surcharge'], 
                        df['with_surcharge'] + df['without_surcharge'], 
                        color='#E74C3C', alpha=0.7, label='Without Surcharge (Leakage)')
        
        # Formatting
        ax.set_title('Congestion Toll Leakage Analysis', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel('Number of Trips', fontsize=12, fontweight='bold')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3)
        
        # Rotate x-axis labels
        plt.xticks(rotation=45, ha='right')
        
        # Tight layout
        plt.tight_layout()
        
        # Save
        output_path = FIGURES_DIR / "leakage_analysis.png"
        plt.savefig(output_path, dpi=DPI, bbox_inches='tight')
        plt.close()
        
        logger.success(f"Saved leakage chart to {output_path}")
        
        con.close()
        
    except Exception as e:
        logger.error(f"Failed to create leakage chart: {e}")


def create_all_visualizations() -> None:
    """
    Create all visualizations for the audit report.
    """
    
    logger.info("=" * 60)
    logger.info("Creating All Visualizations (Matplotlib)")
    logger.info("=" * 60)
    
    # Create output directory
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create charts
    create_time_series_chart()
    create_revenue_chart()
    create_zone_category_chart()
    create_leakage_chart()
    
    logger.success("All visualizations created!")


if __name__ == "__main__":
    """
    Run this module directly to create all visualizations:
    
    python -m src.visualization
    
    This will:
    1. Load aggregated data
    2. Create 4 matplotlib charts (PNG, 300 DPI)
    3. Save to outputs/figures/
    
    Time: ~2-5 minutes
    """
    
    create_all_visualizations()
    sys.exit(0)
