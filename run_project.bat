@echo off
REM ============================================================================
REM NYC Congestion Pricing Audit 2025 - Master Control Script
REM ============================================================================
REM This script provides a menu-driven interface to run all project components
REM ============================================================================

setlocal enabledelayedexpansion

:MENU
cls
echo.
echo ============================================================================
echo           NYC CONGESTION PRICING AUDIT 2025 - MASTER CONTROL
echo ============================================================================
echo.
echo  [1] Run Complete Pipeline (All Phases)
echo  [2] Generate Visualizations Only
echo  [3] Generate PDF Report Only
echo  [4] Launch Streamlit Dashboard (Web)
echo  [5] Launch Tkinter Dashboard (Desktop)
echo  [6] Run Quick Demo (Viz + Report + Dashboard)
echo  [7] View Project Statistics
echo  [8] Clean and Regenerate Everything
echo  [9] Open Output Files
echo  [0] Exit
echo.
echo ============================================================================
echo.

set /p choice="Enter your choice (0-9): "

if "%choice%"=="1" goto RUN_PIPELINE
if "%choice%"=="2" goto RUN_VIZ
if "%choice%"=="3" goto RUN_REPORT
if "%choice%"=="4" goto RUN_STREAMLIT
if "%choice%"=="5" goto RUN_TKINTER
if "%choice%"=="6" goto RUN_DEMO
if "%choice%"=="7" goto VIEW_STATS
if "%choice%"=="8" goto CLEAN_REGEN
if "%choice%"=="9" goto OPEN_FILES
if "%choice%"=="0" goto EXIT

echo Invalid choice! Please try again.
timeout /t 2 >nul
goto MENU

REM ============================================================================
REM Option 1: Run Complete Pipeline
REM ============================================================================
:RUN_PIPELINE
cls
echo.
echo ============================================================================
echo                    RUNNING COMPLETE PIPELINE
echo ============================================================================
echo.
echo This will run all 9 phases:
echo  - Phase 1: Data Ingestion (downloads 50+ GB)
echo  - Phase 2: Schema Unification
echo  - Phase 3: Ghost Trip Detection
echo  - Phase 4: Missing Data Imputation
echo  - Phase 5: Congestion Zone Filtering
echo  - Phase 6-7: Analysis
echo  - Phase 8: Visualization
echo  - Phase 9: Weather Integration
echo.
echo WARNING: This will take 30-60 minutes and download 50+ GB of data!
echo.
set /p confirm="Continue? (Y/N): "
if /i not "%confirm%"=="Y" goto MENU

echo.
echo Starting pipeline...
venv\Scripts\python.exe pipeline.py

echo.
echo ============================================================================
echo Pipeline completed!
echo ============================================================================
pause
goto MENU

REM ============================================================================
REM Option 2: Generate Visualizations Only
REM ============================================================================
:RUN_VIZ
cls
echo.
echo ============================================================================
echo                    GENERATING VISUALIZATIONS
echo ============================================================================
echo.
echo Creating 4 matplotlib charts (300 DPI PNG):
echo  - Time Series (Daily Trip Volume)
echo  - Revenue Analysis (Dual Y-Axis)
echo  - Zone Distribution (Grouped Bars)
echo  - Leakage Analysis (Stacked Area)
echo.

venv\Scripts\python.exe -m src.visualization

echo.
echo ============================================================================
echo Visualizations created in outputs\figures\
echo ============================================================================
pause
goto MENU

REM ============================================================================
REM Option 3: Generate PDF Report Only
REM ============================================================================
:RUN_REPORT
cls
echo.
echo ============================================================================
echo                    GENERATING PDF REPORT
echo ============================================================================
echo.
echo Creating comprehensive 12-page audit report with:
echo  - Executive summary
echo  - Embedded visualizations
echo  - Statistics tables
echo  - Policy recommendations
echo.

venv\Scripts\python.exe -m src.report

echo.
echo ============================================================================
echo PDF Report created: outputs\audit_report.pdf
echo ============================================================================
echo Opening report...
start outputs\audit_report.pdf
pause
goto MENU

REM ============================================================================
REM Option 4: Launch Streamlit Dashboard
REM ============================================================================
:RUN_STREAMLIT
cls
echo.
echo ============================================================================
echo                    LAUNCHING STREAMLIT DASHBOARD
echo ============================================================================
echo.
echo Starting web-based dashboard...
echo.
echo Dashboard will open in your browser at: http://localhost:8501
echo.
echo Features:
echo  - 4 Interactive Tabs
echo  - 8 KPI Metrics
echo  - Embedded Visualizations
echo  - Data Tables
echo.
echo Press Ctrl+C to stop the server
echo.

venv\Scripts\streamlit run dashboard\streamlit_app.py

pause
goto MENU

REM ============================================================================
REM Option 5: Launch Tkinter Dashboard
REM ============================================================================
:RUN_TKINTER
cls
echo.
echo ============================================================================
echo                    LAUNCHING TKINTER DASHBOARD
echo ============================================================================
echo.
echo Starting native desktop dashboard...
echo.
echo Features:
echo  - 5 Tabs (Overview, Time Series, Revenue, Zones, Leakage)
echo  - 4 Colored Metric Cards
echo  - Scrollable Visualizations
echo  - Offline Capable
echo.

venv\Scripts\python.exe dashboard\app.py

echo.
echo Dashboard closed.
pause
goto MENU

REM ============================================================================
REM Option 6: Quick Demo
REM ============================================================================
:RUN_DEMO
cls
echo.
echo ============================================================================
echo                    RUNNING QUICK DEMO
echo ============================================================================
echo.
echo This will:
echo  1. Generate visualizations
echo  2. Create PDF report
echo  3. Launch Streamlit dashboard
echo.

echo Step 1/3: Generating visualizations...
venv\Scripts\python.exe -m src.visualization

echo.
echo Step 2/3: Creating PDF report...
venv\Scripts\python.exe -m src.report

echo.
echo Step 3/3: Launching dashboard...
echo.
echo Dashboard will open at: http://localhost:8501
echo Press Ctrl+C to stop
echo.

venv\Scripts\streamlit run dashboard\streamlit_app.py

pause
goto MENU

REM ============================================================================
REM Option 7: View Statistics
REM ============================================================================
:VIEW_STATS
cls
echo.
echo ============================================================================
echo                    PROJECT STATISTICS
echo ============================================================================
echo.

echo Checking project files...
echo.

REM Count source files
set /a src_count=0
for %%f in (src\*.py) do set /a src_count+=1

echo Source Modules: %src_count%
echo.

REM Check data files
if exist data\raw\yellow (
    echo Data Files:
    dir /b data\raw\yellow\*.parquet 2>nul | find /c ".parquet" > temp_count.txt
    set /p yellow_count=<temp_count.txt
    echo   Yellow Taxi: !yellow_count! files
    del temp_count.txt
)

REM Check outputs
if exist outputs\figures (
    echo.
    echo Visualizations:
    dir /b outputs\figures\*.png 2>nul | find /c ".png" > temp_count.txt
    set /p viz_count=<temp_count.txt
    echo   PNG Charts: !viz_count! files
    del temp_count.txt
)

if exist outputs\audit_report.pdf (
    echo.
    echo PDF Report: Generated
    for %%f in (outputs\audit_report.pdf) do echo   Size: %%~zf bytes
)

echo.
echo Dashboard Files:
if exist dashboard\streamlit_app.py echo   Streamlit: Available
if exist dashboard\app.py echo   Tkinter: Available

echo.
echo ============================================================================
pause
goto MENU

REM ============================================================================
REM Option 8: Clean and Regenerate
REM ============================================================================
:CLEAN_REGEN
cls
echo.
echo ============================================================================
echo                    CLEAN AND REGENERATE
echo ============================================================================
echo.
echo This will:
echo  1. Delete old visualizations
echo  2. Delete old PDF report
echo  3. Regenerate visualizations
echo  4. Regenerate PDF report
echo.
set /p confirm="Continue? (Y/N): "
if /i not "%confirm%"=="Y" goto MENU

echo.
echo Cleaning old files...
if exist outputs\figures\*.png del /q outputs\figures\*.png
if exist outputs\audit_report.pdf del /q outputs\audit_report.pdf

echo.
echo Regenerating visualizations...
venv\Scripts\python.exe -m src.visualization

echo.
echo Regenerating PDF report...
venv\Scripts\python.exe -m src.report

echo.
echo ============================================================================
echo Regeneration complete!
echo ============================================================================
pause
goto MENU

REM ============================================================================
REM Option 9: Open Output Files
REM ============================================================================
:OPEN_FILES
cls
echo.
echo ============================================================================
echo                    OPENING OUTPUT FILES
echo ============================================================================
echo.

echo Opening visualizations...
if exist outputs\figures\time_series_trips.png start outputs\figures\time_series_trips.png
if exist outputs\figures\revenue_analysis.png start outputs\figures\revenue_analysis.png
if exist outputs\figures\zone_category_distribution.png start outputs\figures\zone_category_distribution.png
if exist outputs\figures\leakage_analysis.png start outputs\figures\leakage_analysis.png

timeout /t 2 >nul

echo.
echo Opening PDF report...
if exist outputs\audit_report.pdf start outputs\audit_report.pdf

echo.
echo Files opened!
timeout /t 2 >nul
goto MENU

REM ============================================================================
REM Exit
REM ============================================================================
:EXIT
cls
echo.
echo ============================================================================
echo                    Thank you for using the NYC Audit System!
echo ============================================================================
echo.
echo Project: NYC Congestion Pricing Audit 2025
echo Author: Daniyal Haider
echo.
timeout /t 2 >nul
exit /b 0
