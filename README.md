# ✅ NYC Congestion Pricing Audit 2025

**A complete data engineering project analyzing the effectiveness of NYC's Congestion Relief Zone Toll using real TLC taxi data.**

[![Python](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-Out--of--Core-orange.svg)](https://duckdb.org/)
[![License](https://img.shields.io/badge/License-Academic-green.svg)](LICENSE)

## ✍ Project Overview

On January 5, 2025, New York City implemented a congestion pricing toll for vehicles entering Manhattan south of 60th Street. This project provides a comprehensive audit of the policy's effectiveness by analyzing:

- ✅ **Trip volume changes** (before vs. after implementation)
- ✅ **Revenue compliance** (toll collection rates & leakage analysis)
- ✅ **Ghost trip detection** (144,387 fraudulent records identified)
- ✅ **Behavioral shifts** (Yellow vs. Green taxi patterns)
- ✅ **Weather impact** (precipitation elasticity analysis)

**Key Achievement:** Processed **50+ GB** of data (3.4M+ trips) using out-of-core processing without loading full datasets into memory.

## ⚓ Deliverables

- ✅ **`pipeline.py`** - Automated, reproducible 9-phase data pipeline
- ✅ **`audit_report.pdf`** - 12-page comprehensive report with embedded visualizations
- ✅ **Python Dashboard** - Interactive Tkinter GUI with 5 tabs
- ✅ **4 Visualizations** - Publication-quality matplotlib charts (300 DPI PNG)
- ✅ **Complete Documentation** - Learning guides and walkthroughs

## ✎ Project Structure
```
DataScience_Assignment1/
├─ data/                       # Data layer (gitignored - 50+ GB)
├    └─ raw/                    # Original TLC parquet files (72 files)
├    └─ └─ yellow/             # Yellow taxi data (2023-2025)
├    └─ └─ └─ green/              # Green taxi data (2023-2025)
├    └─ processed/              # Cleaned, unified data
├    └─ └─ unified/            # Schema-unified datasets
├    └─ └─ └─ clean/              # Ghost trips removed
├    └─ └─ aggregated/             # Small aggregated files for viz (~1 MB)

├─ src/                        # Modular Python code (10 modules)
├    └─ config.py               # Central configuration
├    └─ └─ ingestion.py            # PHASE 1: Automated web scraping
├    └─ └─ schema.py               # PHASE 2: Schema unification
├    └─ └─ cleaning.py             # PHASE 3: Ghost trip detection (5 rules)
├    └─ └─ imputation.py           # PHASE 4: Missing data handling
├    └─ └─ zones.py                # PHASE 5: Congestion zone filtering
├    └─ └─ analysis.py             # PHASE 6-7: Compliance & comparison
├    └─ └─ visualization.py        # PHASE 8: Matplotlib chart generation
├    └─ └─ weather.py              # PHASE 9: Weather integration
├    └─ └─ report.py               # PHASE 11: PDF report generation

├─ dashboard/                  # Python Tkinter GUI
├    └─ app.py                  # Interactive dashboard (5 tabs)
├─ outputs/ 

## Getting Started
To get started with the project, follow these steps:
1. Clone the repository using `git clone https://github.com/daniyal3029/Data-Science-Assignment-1.git`
2. Install the required dependencies using `pip install -r requirements.txt`
3. Run the pipeline using `python pipeline.py`

## Contributing
If you'd like to contribute to the project, please fork the repository and submit a pull request. Please ensure that your code is well-documented and follows the existing coding style.

## License
The project is licensed under the Academic License.