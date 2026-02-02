# 🚕 NYC Congestion Pricing Audit 2025

**A data engineering project analyzing the impact of NYC's Congestion Relief Zone Toll using TLC taxi data.**

---

## 📋 Project Overview

On January 5, 2025, New York City implemented a congestion pricing toll for vehicles entering Manhattan south of 60th Street. This project audits the policy's effectiveness by analyzing:

- **Trip volume changes** (before vs. after implementation)
- **Revenue compliance** (toll collection rates)
- **Ghost trip detection** (fraudulent/erroneous records)
- **Behavioral shifts** (Yellow vs. Green taxi patterns)
- **Weather impact** (precipitation elasticity)

---

## 🎯 Assignment Deliverables

- [x] `pipeline.py` - Automated, reproducible data pipeline
- [ ] `audit_report.pdf` - Executive summary with policy recommendations
- [ ] Streamlit Dashboard (4 tabs: Map | Flow | Economics | Weather)
- [ ] Medium blog post
- [ ] LinkedIn professional post

---

## 🏗️ Project Structure

```
DataScience_Assignment1/
├── data/                   # Data layer (gitignored)
│   ├── raw/                # Original TLC parquet files
│   ├── processed/          # Cleaned, unified data
│   └── aggregated/         # Small aggregated files for viz
├── src/                    # Modular Python code
│   ├── ingestion.py        # PHASE 1: Download automation
│   ├── schema.py           # PHASE 2: Schema unification
│   ├── cleaning.py         # PHASE 3: Ghost trip detection
│   ├── imputation.py       # PHASE 4: Missing data handling
│   ├── zones.py            # PHASE 5: Congestion zone logic
│   ├── analysis.py         # PHASE 6-7: Compliance & comparison
│   ├── visualization.py    # PHASE 8: Chart generation
│   └── weather.py          # PHASE 9: Weather integration
├── dashboard/              # Streamlit app
├── tests/                  # Unit tests
├── outputs/                # Reports, figures, logs
└── pipeline.py             # Main orchestrator
```

---

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Run automated setup
setup_project.bat

# Activate virtual environment
venv\Scripts\activate

# Verify installation
python -c "import duckdb; print('✓ DuckDB ready')"
```

### 2. Run Pipeline

```bash
# Full pipeline (all phases)
python pipeline.py

# Individual phases
python pipeline.py --phase ingestion
python pipeline.py --phase cleaning
```

### 3. Launch Dashboard

```bash
streamlit run dashboard/app.py
```

---

## 🛠️ Technology Stack

| Component | Tool | Why? |
|-----------|------|------|
| **Big Data Engine** | DuckDB | Out-of-core processing, SQL-based, parquet-native |
| **Geospatial** | GeoPandas | Shapefile handling, zone filtering |
| **Visualization** | Plotly + Folium | Interactive charts and maps |
| **Dashboard** | Streamlit | Rapid prototyping, easy deployment |
| **Weather Data** | Meteostat | Historical precipitation data |

---

## 📊 Key Constraints

✅ **DO:**
- Use DuckDB/Spark/Polars for all data processing
- Aggregate before plotting
- Auto-handle missing December 2025 data
- Detect and log ghost trips
- Build real, actionable insights

❌ **DON'T:**
- Load full datasets into pandas
- Skip data validation
- Use toy examples
- Violate "Big Data only" rule

---

## 📈 Development Phases

- [x] **PHASE 0**: Project setup
- [ ] **PHASE 1**: Automated data ingestion
- [ ] **PHASE 2**: Schema unification
- [ ] **PHASE 3**: Ghost trip detection
- [ ] **PHASE 4**: Missing data imputation
- [ ] **PHASE 5**: Congestion zone filtering
- [ ] **PHASE 6**: Leakage & compliance analysis
- [ ] **PHASE 7**: Yellow vs. Green comparison
- [ ] **PHASE 8**: Visual audit
- [ ] **PHASE 9**: Weather elasticity
- [ ] **PHASE 10**: Streamlit dashboard
- [ ] **PHASE 11**: Audit report
- [ ] **PHASE 12**: Medium + LinkedIn posts

---

## 👨‍💻 Author

**Daniyal Haider**  
Final-Year Software Engineering Student  
Data Science Assignment 1 - 2025

---

## 📝 License

Academic project - Not for commercial use
