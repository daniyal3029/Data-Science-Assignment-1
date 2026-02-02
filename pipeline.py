"""
NYC Congestion Pricing Audit - Main Pipeline
=============================================

This is the main orchestrator that runs all phases in sequence.

Usage:
    python pipeline.py              # Run all phases
    python pipeline.py --phase 1    # Run specific phase
    python pipeline.py --skip-download  # Skip data download

Phases:
1. Data Ingestion (download TLC data)
2. Schema Unification (map columns)
3. Ghost Trip Detection (clean data)
4. Missing Data Imputation (handle Dec 2025)
5. Congestion Zone Filtering (identify zones)
6-7. Analysis (leakage + Yellow vs Green)
8. Visualization (create charts)
9. Weather Integration (join weather data)

Output:
- Aggregated data files in data/aggregated/
- Visualizations in outputs/figures/
- Logs in outputs/logs/
"""

import sys
import argparse
from pathlib import Path
from loguru import logger
from datetime import datetime

# Import all phase modules
from src import config
from src import ingestion
from src import schema
from src import cleaning
from src import imputation
from src import zones
from src import analysis
from src import visualization
from src import weather

# Setup logging
LOG_FILE = config.LOGS_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logger.remove()
logger.add(sys.stderr, format="{time:HH:mm:ss} | {level: <8} | {message}", level="INFO")
logger.add(LOG_FILE, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")


def run_phase_1():
    """Phase 1: Data Ingestion"""
    logger.info("=" * 80)
    logger.info("PHASE 1: Data Ingestion")
    logger.info("=" * 80)
    
    stats = ingestion.download_all_data(skip_existing=True)
    
    total_failed = stats['yellow_failed'] + stats['green_failed']
    if total_failed > 0:
        logger.warning(f"Phase 1 completed with {total_failed} failures")
        return False
    
    logger.success("Phase 1 completed successfully!")
    return True


def run_phase_2():
    """Phase 2: Schema Unification"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 2: Schema Unification")
    logger.info("=" * 80)
    
    stats = schema.unify_all_files()
    
    total_failed = stats['yellow_failed'] + stats['green_failed']
    if total_failed > 0:
        logger.warning(f"Phase 2 completed with {total_failed} failures")
        return False
    
    logger.success("Phase 2 completed successfully!")
    return True


def run_phase_3():
    """Phase 3: Ghost Trip Detection"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 3: Ghost Trip Detection")
    logger.info("=" * 80)
    
    stats = cleaning.clean_all_files()
    
    if stats['files_processed'] == 0:
        logger.error("Phase 3 failed - no files processed")
        return False
    
    cleaning.analyze_ghost_patterns()
    
    logger.success("Phase 3 completed successfully!")
    return True


def run_phase_4():
    """Phase 4: Missing Data Imputation"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 4: Missing Data Imputation")
    logger.info("=" * 80)
    
    stats = imputation.impute_all_missing_data()
    
    logger.success("Phase 4 completed successfully!")
    return True


def run_phase_5():
    """Phase 5: Congestion Zone Filtering"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 5: Congestion Zone Filtering")
    logger.info("=" * 80)
    
    # Load taxi zones
    taxi_zones = zones.load_taxi_zones()
    
    if taxi_zones is None:
        logger.error("Phase 5 failed - could not load taxi zones")
        return False
    
    # Identify congestion zones
    congestion_zone_ids = zones.identify_congestion_zones(taxi_zones)
    
    if not congestion_zone_ids:
        logger.error("Phase 5 failed - could not identify congestion zones")
        return False
    
    # Classify trips
    zones.classify_trips_by_zone(congestion_zone_ids)
    zones.analyze_zone_patterns()
    
    logger.success("Phase 5 completed successfully!")
    return True


def run_phase_6_7():
    """Phase 6-7: Analysis (Leakage + Yellow vs Green)"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 6-7: Analysis")
    logger.info("=" * 80)
    
    # Leakage analysis
    leakage_stats = analysis.analyze_leakage_and_compliance()
    
    # Yellow vs Green comparison
    comparison_stats = analysis.compare_yellow_vs_green()
    
    if not leakage_stats or not comparison_stats:
        logger.warning("Phase 6-7 completed with some issues")
        return False
    
    logger.success("Phase 6-7 completed successfully!")
    return True


def run_phase_8():
    """Phase 8: Visualization"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 8: Visualization")
    logger.info("=" * 80)
    
    visualization.create_all_visualizations()
    
    logger.success("Phase 8 completed successfully!")
    return True


def run_phase_9():
    """Phase 9: Weather Integration"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 9: Weather Integration")
    logger.info("=" * 80)
    
    weather.join_weather_with_trips()
    
    logger.success("Phase 9 completed successfully!")
    return True


def run_all_phases(skip_download=False):
    """
    Run all phases in sequence.
    
    Args:
        skip_download: If True, skip Phase 1 (data download)
    """
    
    logger.info("=" * 80)
    logger.info("NYC CONGESTION PRICING AUDIT - FULL PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info("=" * 80)
    
    # Create all directories
    config.create_directories()
    
    # Track phase results
    results = {}
    
    # Phase 1: Data Ingestion
    if not skip_download:
        results['phase_1'] = run_phase_1()
    else:
        logger.info("Skipping Phase 1 (data download)")
        results['phase_1'] = True
    
    # Phase 2: Schema Unification
    if results['phase_1']:
        results['phase_2'] = run_phase_2()
    else:
        logger.error("Skipping Phase 2 due to Phase 1 failure")
        results['phase_2'] = False
    
    # Phase 3: Ghost Trip Detection
    if results['phase_2']:
        results['phase_3'] = run_phase_3()
    else:
        logger.error("Skipping Phase 3 due to Phase 2 failure")
        results['phase_3'] = False
    
    # Phase 4: Missing Data Imputation
    if results['phase_3']:
        results['phase_4'] = run_phase_4()
    else:
        logger.error("Skipping Phase 4 due to Phase 3 failure")
        results['phase_4'] = False
    
    # Phase 5: Congestion Zone Filtering
    if results['phase_4']:
        results['phase_5'] = run_phase_5()
    else:
        logger.error("Skipping Phase 5 due to Phase 4 failure")
        results['phase_5'] = False
    
    # Phase 6-7: Analysis
    if results['phase_5']:
        results['phase_6_7'] = run_phase_6_7()
    else:
        logger.error("Skipping Phase 6-7 due to Phase 5 failure")
        results['phase_6_7'] = False
    
    # Phase 8: Visualization
    if results['phase_6_7']:
        results['phase_8'] = run_phase_8()
    else:
        logger.error("Skipping Phase 8 due to Phase 6-7 failure")
        results['phase_8'] = False
    
    # Phase 9: Weather Integration
    if results['phase_8']:
        results['phase_9'] = run_phase_9()
    else:
        logger.error("Skipping Phase 9 due to Phase 8 failure")
        results['phase_9'] = False
    
    # Print summary
    logger.info("\n" + "=" * 80)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 80)
    
    for phase, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{phase.upper()}: {status}")
    
    logger.info("=" * 80)
    logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info("=" * 80)
    
    # Return overall success
    return all(results.values())


def main():
    """
    Main entry point with command-line argument parsing.
    """
    
    parser = argparse.ArgumentParser(
        description='NYC Congestion Pricing Audit Pipeline'
    )
    
    parser.add_argument(
        '--phase',
        type=int,
        choices=range(1, 10),
        help='Run specific phase only (1-9)'
    )
    
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip Phase 1 (data download)'
    )
    
    args = parser.parse_args()
    
    # Run specific phase or all phases
    if args.phase:
        phase_functions = {
            1: run_phase_1,
            2: run_phase_2,
            3: run_phase_3,
            4: run_phase_4,
            5: run_phase_5,
            6: run_phase_6_7,
            7: run_phase_6_7,
            8: run_phase_8,
            9: run_phase_9,
        }
        
        success = phase_functions[args.phase]()
        sys.exit(0 if success else 1)
    else:
        success = run_all_phases(skip_download=args.skip_download)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
