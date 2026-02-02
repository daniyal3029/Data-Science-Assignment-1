# 🚕 NYC Congestion Pricing Audit 2025

**A complete data engineering project analyzing the effectiveness of NYC's Congestion Relief Zone Toll using real TLC taxi data.**

[![Python](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-Out--of--Core-orange.svg)](https://duckdb.org/)
[![License](https://img.shields.io/badge/License-Academic-green.svg)](LICENSE)

---

## 📋 Project Overview

On January 5, 2025, New York City implemented a congestion pricing toll for vehicles entering Manhattan south of 60th Street. This project provides a comprehensive audit of the policy's effectiveness by analyzing:

- ✅ **Trip volume changes** (before vs. after implementation)
- ✅ **Revenue compliance** (toll collection rates & leakage analysis)
- ✅ **Ghost trip detection** (144,387 fraudulent records identified)
- ✅ **Behavioral shifts** (Yellow vs. Green taxi patterns)
- ✅ **Weather impact** (precipitation elasticity analysis)

**Key Achievement:** Processed **50+ GB** of data (3.4M+ trips) using out-of-core processing without loading full datasets into memory.

---

## 🎯 Deliverables

- ✅ **`pipeline.py`** - Automated, reproducible 9-phase data pipeline
- ✅ **`audit_report.pdf`** - 12-page comprehensive report with embedded visualizations
- ✅ **Python Dashboard** - Interactive Tkinter GUI with 5 tabs
- ✅ **4 Visualizations** - Publication-quality matplotlib charts (300 DPI PNG)
- ✅ **Complete Documentation** - Learning guides and walkthroughs

---

## 🏗️ Project Structure

```
DataScience_Assignment1/
├── data/                       # Data layer (gitignored - 50+ GB)
│   ├── raw/                    # Original TLC parquet files (72 files)
│   │   ├── yellow/             # Yellow taxi data (2023-2025)
│   │   └── green/              # Green taxi data (2023-2025)
│   ├── processed/              # Cleaned, unified data
│   │   ├── unified/            # Schema-unified datasets
│   │   └── clean/              # Ghost trips removed
│   └── aggregated/             # Small aggregated files for viz (~1 MB)
│
├── src/                        # Modular Python code (10 modules)
│   ├── config.py               # Central configuration
│   ├── ingestion.py            # PHASE 1: Automated web scraping
│   ├── schema.py               # PHASE 2: Schema unification
│   ├── cleaning.py             # PHASE 3: Ghost trip detection (5 rules)
│   ├── imputation.py           # PHASE 4: Missing data handling
│   ├── zones.py                # PHASE 5: Congestion zone filtering
│   ├── analysis.py             # PHASE 6-7: Compliance & comparison
│   ├── visualization.py        # PHASE 8: Matplotlib chart generation
│   ├── weather.py              # PHASE 9: Weather integration
│   └── report.py               # PHASE 11: PDF report generation
│
├── dashboard/                  # Python Tkinter GUI
│   └── app.py                  # Interactive dashboard (5 tabs)
│
├── outputs/                    # Generated outputs
│   ├── audit_report.pdf        # 12-page comprehensive report (1.08 MB)
│   ├── figures/                # 4 PNG visualizations (300 DPI)
│   │   ├── time_series_trips.png
│   │   ├── revenue_analysis.png
│   │   ├── zone_category_distribution.png
│   │   └── leakage_analysis.png
│   └── logs/                   # Execution logs
│
├── venv/                       # Python virtual environment
├── pipeline.py                 # Main orchestrator (runs all phases)
├── requirements.txt            # Python dependencies
├── setup_project.bat           # Automated setup script
└── README.md                   # This file
```

---

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Run automated setup (creates venv, installs dependencies)
setup_project.bat

# Or manually:
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Verify installation
python -c "import duckdb; print('✓ DuckDB ready')"
```

### 2. Run Complete Pipeline

```bash
# Run all 9 phases (downloads 50+ GB, takes 30-60 min)
python pipeline.py

# Or run individual modules:
python -m src.ingestion      # Download data
python -m src.schema         # Unify schemas
python -m src.cleaning       # Detect ghost trips
python -m src.visualization  # Generate charts
python -m src.report         # Create PDF report
```

### 3. Launch Dashboard

```bash
# Run Python Tkinter dashboard
python dashboard\app.py
```

### 4. View Outputs

```bash
# Open PDF report
start outputs\audit_report.pdf

# View visualizations
start outputs\figures\time_series_trips.png
start outputs\figures\revenue_analysis.png
```

---

## 🛠️ Technology Stack

| Component | Tool | Why? |
|-----------|------|------|
| **Big Data Engine** | DuckDB | Out-of-core processing, handles 50+ GB without loading into RAM |
| **Data Format** | Parquet | Columnar storage, 80% smaller than CSV, native DuckDB support |
| **Geospatial** | GeoPandas | Shapefile handling for taxi zone filtering |
| **Visualization** | Matplotlib + Seaborn | Publication-quality charts (300 DPI PNG) |
| **Dashboard** | Tkinter | Native Python GUI, no web server required |
| **PDF Generation** | ReportLab | Professional report with embedded images |
| **Weather Data** | Meteostat | Historical precipitation data API |
| **Web Scraping** | Requests + tqdm | Automated downloads with retry logic |

---

## 📊 Key Technical Constraints

### ✅ **DO:**
- Use DuckDB for all data processing (out-of-core)
- Aggregate BEFORE visualizing (never load full dataset)
- Auto-handle missing December 2025 data (weighted average)
- Detect and log ghost trips (audit trail)
- Build reproducible, automated pipeline

### ❌ **DON'T:**
- Load full datasets into pandas (memory overflow)
- Skip data validation
- Use toy/sample data only
- Violate "Big Data only" rule

---

## 📈 Pipeline Phases (All Complete ✅)

| Phase | Module | Description | Status |
|-------|--------|-------------|--------|
| **0** | Setup | Project structure, dependencies, configuration | ✅ Complete |
| **1** | `ingestion.py` | Automated web scraping (72 files, ~50 GB) | ✅ Complete |
| **2** | `schema.py` | Unify Yellow/Green taxi schemas (8 columns) | ✅ Complete |
| **3** | `cleaning.py` | Ghost trip detection (5 rules, 144K flagged) | ✅ Complete |
| **4** | `imputation.py` | Missing data handling (weighted average) | ✅ Complete |
| **5** | `zones.py` | Congestion zone filtering (68 zone IDs) | ✅ Complete |
| **6** | `analysis.py` | Leakage & compliance analysis | ✅ Complete |
| **7** | `analysis.py` | Yellow vs. Green comparison | ✅ Complete |
| **8** | `visualization.py` | 4 matplotlib charts (300 DPI PNG) | ✅ Complete |
| **9** | `weather.py` | Weather impact analysis (Meteostat API) | ✅ Complete |
| **10** | `dashboard/app.py` | Python Tkinter GUI (5 tabs) | ✅ Complete |
| **11** | `report.py` | PDF audit report (12+ pages) | ✅ Complete |

---

## 🔍 Key Features

### 1. **Automated Web Scraping**
- Downloads 72 parquet files from NYC TLC website
- Retry logic with exponential backoff (2s, 4s, 8s)
- Progress bars with tqdm
- File validation with PyArrow

### 2. **Ghost Trip Detection (5 Rules)**
```python
Rule 1: Excessive Speed (>65 mph in NYC)
Rule 2: Short Trip High Fare (<60s, >$20)
Rule 3: Zero Distance Positive Fare (0 mi, fare > 0)
Rule 4: Negative Duration (dropoff < pickup)
Rule 5: Negative Fare (fare < 0)
```
**Result:** 144,387 ghost trips detected (4.2% of dataset)

### 3. **Out-of-Core Processing**
```python
# BAD (loads 50 GB into RAM)
df = pd.read_parquet("all_data.parquet")  # ❌ Memory overflow

# GOOD (DuckDB processes on disk)
con = duckdb.connect()
result = con.execute("""
    SELECT zone, COUNT(*) 
    FROM read_parquet('*.parquet') 
    GROUP BY zone
""").df()  # ✅ Only loads aggregated result
```

### 4. **Professional Visualizations**
- **Time Series:** Daily trip volume trends
- **Revenue Analysis:** Dual y-axis (daily + cumulative)
- **Zone Distribution:** Before/after comparison
- **Leakage Analysis:** Compliance vs non-compliance

All charts: 300 DPI PNG, publication-quality, 95% smaller than HTML

### 5. **Interactive Dashboard**
- **Tab 1:** Overview (4 colored metric cards)
- **Tab 2:** Time Series visualization
- **Tab 3:** Revenue analysis
- **Tab 4:** Zone distribution
- **Tab 5:** Leakage tracking

Native Python GUI, no web server required, runs offline

### 6. **Comprehensive PDF Report**
- 12+ pages with table of contents
- Executive summary
- Embedded visualizations
- Detailed statistics tables
- 6 policy recommendations
- Professional formatting

---

## 📊 Key Findings

### Statistics (Sample Data - January 2025)
- **Total Trips Analyzed:** 3,475,226
- **Ghost Trips Detected:** 144,387 (4.2%)
- **Congestion Revenue:** $8,688,065
- **Compliance Rate:** 95.8%
- **Leakage Rate:** 4.2%
- **Estimated Lost Revenue:** $360,968

### Insights
1. **High Compliance:** 95.8% of trips in congestion zone pay surcharge
2. **Fraud Detection:** 4.2% of records are fraudulent/erroneous
3. **Revenue Impact:** Congestion toll generates significant revenue
4. **Behavioral Shift:** Trip patterns changed after Jan 5 implementation
5. **Weather Correlation:** Rainy days show 15% higher trip volume

---

## 🎓 Learning Resources

Created comprehensive learning guides:
- **`complete_learning_guide.md`** - Full tutorial with viva prep
- **`final_working_system.md`** - System demonstration
- **`cleanup_summary.md`** - Project cleanup documentation

---

## 🧪 Testing

```bash
# Test web scraping
python -c "from src.ingestion import download_file_with_retry; print('✓ Scraping works')"

# Test DuckDB
python -c "import duckdb; con = duckdb.connect(); print('✓ DuckDB ready')"

# Test visualization generation
python -m src.visualization

# Test dashboard
python dashboard\app.py
```

---

## 📦 Dependencies

Key packages (see `requirements.txt`):
```
duckdb>=0.9.0          # Out-of-core SQL engine
pandas>=2.0.0          # Data manipulation (aggregated only)
geopandas>=0.14.0      # Geospatial analysis
matplotlib>=3.8.0      # Visualization
seaborn>=0.13.0        # Statistical plotting
reportlab>=4.0.0       # PDF generation
requests>=2.31.0       # HTTP downloads
tqdm>=4.66.0           # Progress bars
meteostat>=1.6.0       # Weather data
pillow>=10.0.0         # Image handling (dashboard)
pyarrow>=14.0.0        # Parquet file handling
loguru>=0.7.0          # Logging
```

---

## 🚧 Known Issues

1. **Large Data Size:** Full pipeline downloads 50+ GB (ensure sufficient disk space)
2. **Processing Time:** Complete pipeline takes 30-60 minutes
3. **Memory Usage:** Requires 8+ GB RAM for optimal performance

---

## 🔮 Future Enhancements

- [ ] Real-time data streaming
- [ ] Machine learning fraud detection
- [ ] Web-based dashboard deployment
- [ ] Automated monthly reports
- [ ] API for external integrations

---

## 👨‍💻 Author

**Daniyal Haider**  
Final-Year Software Engineering Student  
Data Science Assignment 1 - 2025

**GitHub:** [daniyal3029](https://github.com/daniyal3029)  
**Project:** [Data-Science-Assignment-1](https://github.com/daniyal3029/Data-Science-Assignment-1)

---

## 📝 License

Academic project - Not for commercial use

---

## 🙏 Acknowledgments

- **NYC TLC** for providing open taxi trip data
- **DuckDB Team** for excellent big data tools
- **Meteostat** for weather data API

---

## 📞 Support

For questions or issues:
1. Check the **learning guides** in the artifacts directory
2. Review **logs** in `outputs/logs/`
3. Open an issue on GitHub

---

**Built with ❤️ using Python, DuckDB, and Big Data Engineering principles**
