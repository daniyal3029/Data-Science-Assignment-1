"""
PHASE 10: Streamlit Dashboard (Web Version)
============================================

Interactive web-based dashboard for NYC Congestion Pricing Audit.

Features:
- 4 main tabs: Overview, Time Series, Revenue, Zones
- Interactive metrics and KPIs
- Embedded matplotlib visualizations
- Data tables and statistics
- Professional styling

Usage:
    streamlit run dashboard/streamlit_app.py
"""

import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path
import sys
from PIL import Image

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.config import AGGREGATED_DIR, FIGURES_DIR


# Page configuration
st.set_page_config(
    page_title="NYC Congestion Pricing Audit 2025",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        font-size: 1.1rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_statistics():
    """Load aggregated statistics from parquet files"""
    try:
        con = duckdb.connect()
        zone_file = AGGREGATED_DIR / "trips_by_zone_category.parquet"
        
        if zone_file.exists():
            stats = con.execute(f"""
                SELECT 
                    SUM(trip_count) as total_trips,
                    SUM(total_congestion_collected) as total_revenue,
                    SUM(trips_with_surcharge) as trips_with_surcharge,
                    SUM(trips_without_surcharge) as trips_without_surcharge,
                    AVG(avg_fare) as avg_fare,
                    AVG(avg_distance) as avg_distance
                FROM read_parquet('{zone_file}')
                WHERE after_congestion_start = 1
            """).fetchone()
            
            total_trips = stats[0] if stats[0] else 0
            total_revenue = stats[1] if stats[1] else 0
            trips_with = stats[2] if stats[2] else 0
            trips_without = stats[3] if stats[3] else 0
            avg_fare = stats[4] if stats[4] else 0
            avg_distance = stats[5] if stats[5] else 0
            
            compliance_rate = (trips_with / (trips_with + trips_without) * 100) if (trips_with + trips_without) > 0 else 0
            leakage_rate = 100 - compliance_rate
            estimated_lost = trips_without * 2.5
            
            return {
                'total_trips': total_trips,
                'total_revenue': total_revenue,
                'trips_with_surcharge': trips_with,
                'trips_without_surcharge': trips_without,
                'compliance_rate': compliance_rate,
                'leakage_rate': leakage_rate,
                'avg_fare': avg_fare,
                'avg_distance': avg_distance,
                'estimated_lost_revenue': estimated_lost
            }
        
        con.close()
        
    except Exception as e:
        st.error(f"Error loading statistics: {e}")
        return None
    
    return None


@st.cache_data
def load_zone_data():
    """Load zone category data"""
    try:
        con = duckdb.connect()
        zone_file = AGGREGATED_DIR / "trips_by_zone_category.parquet"
        
        if zone_file.exists():
            df = con.execute(f"""
                SELECT 
                    zone_category,
                    after_congestion_start,
                    SUM(trip_count) as trips,
                    SUM(total_congestion_collected) as revenue
                FROM read_parquet('{zone_file}')
                GROUP BY zone_category, after_congestion_start
                ORDER BY zone_category, after_congestion_start
            """).df()
            
            con.close()
            return df
        
        con.close()
        
    except Exception as e:
        st.error(f"Error loading zone data: {e}")
        return None
    
    return None


def main():
    """Main dashboard application"""
    
    # Header
    st.markdown('<div class="main-header">🚕 NYC Congestion Pricing Audit 2025</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Analysis of the Congestion Relief Zone Toll Effectiveness</div>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/b/ba/Flag_of_New_York_City.svg/320px-Flag_of_New_York_City.svg.png", width=100)
        st.title("Navigation")
        st.markdown("---")
        
        st.markdown("### 📊 Dashboard Sections")
        st.markdown("""
        - **Overview:** Key metrics and statistics
        - **Time Series:** Daily trip volume trends
        - **Revenue:** Revenue analysis and growth
        - **Zones:** Geographic distribution
        """)
        
        st.markdown("---")
        st.markdown("### 📈 Data Source")
        st.markdown("NYC TLC Trip Records")
        st.markdown("January 2025 (Sample)")
        
        st.markdown("---")
        st.markdown("### 🔧 Technology")
        st.markdown("- DuckDB (Out-of-core)")
        st.markdown("- Matplotlib (Viz)")
        st.markdown("- Streamlit (Dashboard)")
    
    # Load data
    stats = load_statistics()
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Time Series", "💰 Revenue", "📍 Zones"])
    
    # Tab 1: Overview
    with tab1:
        st.header("Key Performance Indicators")
        
        if stats:
            # Top metrics row
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    label="Total Trips Analyzed",
                    value=f"{stats['total_trips']:,.0f}",
                    delta="Post-implementation"
                )
            
            with col2:
                st.metric(
                    label="Congestion Revenue",
                    value=f"${stats['total_revenue']:,.2f}",
                    delta=f"+${stats['total_revenue']:,.0f}"
                )
            
            with col3:
                st.metric(
                    label="Compliance Rate",
                    value=f"{stats['compliance_rate']:.1f}%",
                    delta="High compliance"
                )
            
            with col4:
                st.metric(
                    label="Leakage Rate",
                    value=f"{stats['leakage_rate']:.1f}%",
                    delta=f"-${stats['estimated_lost_revenue']:,.0f} lost",
                    delta_color="inverse"
                )
            
            st.markdown("---")
            
            # Second row of metrics
            col5, col6, col7, col8 = st.columns(4)
            
            with col5:
                st.metric(
                    label="Trips with Surcharge",
                    value=f"{stats['trips_with_surcharge']:,.0f}"
                )
            
            with col6:
                st.metric(
                    label="Trips without Surcharge",
                    value=f"{stats['trips_without_surcharge']:,.0f}"
                )
            
            with col7:
                st.metric(
                    label="Average Fare",
                    value=f"${stats['avg_fare']:.2f}"
                )
            
            with col8:
                st.metric(
                    label="Average Distance",
                    value=f"{stats['avg_distance']:.2f} mi"
                )
            
            st.markdown("---")
            
            # Summary section
            st.subheader("📋 Executive Summary")
            
            col_left, col_right = st.columns(2)
            
            with col_left:
                st.markdown("#### Key Findings")
                st.markdown(f"""
                - **{stats['total_trips']:,.0f}** trips analyzed in the congestion zone
                - **{stats['compliance_rate']:.1f}%** compliance rate indicates strong adoption
                - **${stats['total_revenue']:,.2f}** in congestion toll revenue collected
                - **{stats['leakage_rate']:.1f}%** leakage represents opportunity for improvement
                """)
            
            with col_right:
                st.markdown("#### Recommendations")
                st.markdown("""
                - Enhance enforcement in high-leakage areas
                - Implement real-time monitoring systems
                - Mandate automated surcharge collection
                - Conduct quarterly policy reviews
                """)
        
        else:
            st.warning("Statistics not available. Please run the pipeline first.")
    
    # Tab 2: Time Series
    with tab2:
        st.header("Daily Trip Volume Trends")
        
        img_path = FIGURES_DIR / "time_series_trips.png"
        if img_path.exists():
            image = Image.open(img_path)
            st.image(image, use_container_width=True)
            
            st.markdown("---")
            st.markdown("#### Analysis")
            st.markdown("""
            This chart shows the daily trip volume over time. Key observations:
            - **Baseline Period:** Trips before January 5, 2025
            - **Implementation Date:** Vertical line marks policy start
            - **Post-Implementation:** Changes in trip patterns after toll introduction
            - **Trends:** Identify weekly patterns and anomalies
            """)
        else:
            st.error("Time series visualization not found. Please run visualization module.")
    
    # Tab 3: Revenue
    with tab3:
        st.header("Congestion Toll Revenue Analysis")
        
        img_path = FIGURES_DIR / "revenue_analysis.png"
        if img_path.exists():
            image = Image.open(img_path)
            st.image(image, use_container_width=True)
            
            st.markdown("---")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### Daily Revenue (Bars)")
                st.markdown("""
                - Green bars show daily toll collection
                - Fluctuations indicate demand patterns
                - Weekend vs weekday differences
                """)
            
            with col2:
                st.markdown("#### Cumulative Revenue (Line)")
                st.markdown("""
                - Red line shows total revenue growth
                - Steady upward trend indicates consistent collection
                - Slope indicates daily average revenue
                """)
        else:
            st.error("Revenue visualization not found. Please run visualization module.")
    
    # Tab 4: Zones
    with tab4:
        st.header("Trip Distribution by Zone Category")
        
        img_path = FIGURES_DIR / "zone_category_distribution.png"
        if img_path.exists():
            image = Image.open(img_path)
            st.image(image, use_container_width=True)
            
            st.markdown("---")
            
            # Load zone data table
            zone_data = load_zone_data()
            if zone_data is not None:
                st.subheader("📊 Zone Category Statistics")
                
                # Format the dataframe
                zone_data['Period'] = zone_data['after_congestion_start'].map({
                    0: 'Before Jan 5',
                    1: 'After Jan 5'
                })
                
                pivot_table = zone_data.pivot_table(
                    index='zone_category',
                    columns='Period',
                    values='trips',
                    fill_value=0
                )
                
                st.dataframe(pivot_table, use_container_width=True)
                
                st.markdown("#### Zone Categories Explained")
                st.markdown("""
                - **Both in Zone:** Pickup and dropoff both in congestion zone
                - **Dropoff in Zone:** Only dropoff in congestion zone
                - **Neither in Zone:** Both outside congestion zone
                - **Pickup in Zone:** Only pickup in congestion zone
                """)
        else:
            st.error("Zone visualization not found. Please run visualization module.")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 2rem 0;'>
        <p><strong>NYC Congestion Pricing Audit 2025</strong></p>
        <p>Data Source: NYC TLC Trip Records | Processing: DuckDB | Visualization: Matplotlib</p>
        <p>Built with Python, Streamlit, and Big Data Engineering principles</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
