"""
NYC Congestion Pricing Audit - Interactive Streamlit Dashboard
===============================================================
4-tab interactive dashboard showing:
1. Border Effect (Choropleth map logic)
2. Congestion Velocity (Before/After heatmaps)
3. Tip Economics (Crowding out analysis)
4. Rain Elasticity (Weather correlation)
"""

import os
import json
import warnings
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="NYC Congestion Audit",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CACHE_DIR = os.path.join(BASE_DIR, "cache")

# ============================================================================
# DATA LOADING
# ============================================================================

@st.cache_data
def load_data():
    """Load all pipeline outputs."""
    data = {}
    
    # 1. Stats
    if os.path.exists(f"{OUTPUT_DIR}/impact_stats.json"):
        with open(f"{OUTPUT_DIR}/impact_stats.json", 'r') as f:
            data['stats'] = json.load(f)
            
    # 2. CSVs
    files = {
        'ghost': f"{OUTPUT_DIR}/ghost_trip_audit.csv",
        'leakage': f"{OUTPUT_DIR}/leakage_analysis.csv",
        'border': f"{OUTPUT_DIR}/border_effect.csv",
        'velocity_2024': f"{OUTPUT_DIR}/velocity_2024.csv",
        'velocity_2025': f"{OUTPUT_DIR}/velocity_2025.csv",
        'trips': f"{OUTPUT_DIR}/daily_trips_2025.csv",
        'tips': f"{OUTPUT_DIR}/tips_economics.csv",
        'weather': f"{CACHE_DIR}/weather_2025.csv"
    }
    
    for key, path in files.items():
        if os.path.exists(path):
            try:
                data[key] = pd.read_csv(path)
            except Exception as e:
                st.error(f"Error loading {key}: {e}")
    
    # 3. Text
    if os.path.exists(f"{OUTPUT_DIR}/elasticity.txt"):
        with open(f"{OUTPUT_DIR}/elasticity.txt", 'r') as f:
            data['elasticity_text'] = f.read()
            
    return data

DATA = load_data()

# ============================================================================
# TAB 1: THE MAP (BORDER EFFECT)
# ============================================================================

def tab_map():
    st.header("🗺️ Tab 1: The Border Effect")
    st.markdown("""
        **Hypothesis**: Are passengers ending trips just outside the zone to avoid the toll?
        Comparision of Drop-off volumes in Q1 2024 vs Q1 2025.
    """)
    
    df = DATA.get('border')
    if df is not None and not df.empty:
        # Filter for significant volume
        df = df[df['count_2024'] > 100]
        
        # Sort by pct change
        df = df.sort_values('pct_change', ascending=False)
        
        # Top 10 Increases (Potential Avoidance)
        top_inc = df.head(15)
        
        col1, col2 = st.columns([2, 1])
        with col1:
            fig = px.bar(
                top_inc,
                x='pct_change',
                y='location_id',
                orientation='h',
                title='Top 15 Zones with Drop-off Increases (Possible Border Effect)',
                labels={'pct_change': '% Change', 'location_id': 'Taxi Zone ID'},
                color='pct_change',
                color_continuous_scale='Reds'
            )
            fig.update_layout(yaxis={'type': 'category'})
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            st.write("#### Data Details")
            st.dataframe(
                top_inc[['location_id', 'count_2024', 'count_2025', 'pct_change']]
                .style.format({'pct_change': '{:.1f}%'})
            )
            
        st.info("High percentage increases in zones bordering 60th St suggest passengers dropping off early to avoid toll.")
    else:
        st.warning("No Border Effect data available. Run pipeline.py first.")

# ============================================================================
# TAB 2: THE FLOW (VELOCITY)
# ============================================================================

def tab_flow():
    st.header("⚡ Tab 2: The Flow (Congestion Velocity)")
    st.markdown("**Hypothesis**: Did the toll actually speed up traffic inside the zone?")
    
    v24 = DATA.get('velocity_2024')
    v25 = DATA.get('velocity_2025')
    
    if v24 is not None and v25 is not None:
        col1, col2 = st.columns(2)
        
        def make_heatmap(df, year):
            # df columns: dow, hour, avg_speed
            pivot = df.pivot_table(index='dow', columns='hour', values='avg_speed')
            
            # Map clean DOW names if possible (0=Sun in DuckDB usually if 0-6, need check)
            # Assuming 0=Sunday
            days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
            
            # Reindex if possible
            existing_indices = pivot.index
            mapped_indices = [days[i] for i in existing_indices if i < 7]
            
            fig = px.imshow(
                pivot,
                labels=dict(x="Hour of Day", y="Day of Week", color="Speed (MPH)"),
                y=mapped_indices if len(mapped_indices) == len(existing_indices) else pivot.index,
                title=f"Q1 {year} Avg Speeds",
                color_continuous_scale='Viridis',
                aspect="auto"
            )
            return fig

        with col1:
            st.plotly_chart(make_heatmap(v24, 2024), use_container_width=True)
        with col2:
            st.plotly_chart(make_heatmap(v25, 2025), use_container_width=True)
            
        # Comparison delta
        avg24 = v24['avg_speed'].mean()
        avg25 = v25['avg_speed'].mean()
        delta = avg25 - avg24
        pct = (delta / avg24) * 100 if avg24 != 0 else 0
        
        st.metric("Overall Average Speed Change", f"{delta:.2f} MPH", f"{pct:.1f}%")
        
    else:
        st.warning("Velocity data missing.")

# ============================================================================
# TAB 3: ECONOMICS (TIPS)
# ============================================================================

def tab_economics():
    st.header("💰 Tab 3: The Economics")
    st.markdown("**Hypothesis**: Do surcharges crowd out tips?")
    
    tips = DATA.get('tips')
    
    if tips is not None:
        # tips: month, avg_surcharge, avg_tip_pct
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Bar(name="Avg Surcharge ($)", x=tips['month'], y=tips['avg_surcharge'], marker_color='red', opacity=0.5),
            secondary_y=False
        )
        
        fig.add_trace(
            go.Scatter(name="Avg Tip %", x=tips['month'], y=tips['avg_tip_pct'], mode='lines+markers', line=dict(color='blue')),
            secondary_y=True
        )
        
        fig.update_layout(title="Monthly Surcharge vs Tip % (2025)")
        fig.update_xaxes(title_text="Month")
        fig.update_yaxes(title_text="Surcharge ($)", secondary_y=False)
        fig.update_yaxes(title_text="Tip %", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
        
        correlation = tips['avg_surcharge'].corr(tips['avg_tip_pct'])
        st.write(f"**Correlation between Surcharge and Tip %**: {correlation:.3f}")
        if correlation < -0.3:
            st.error("Evidence of Crowding Out: Higher surcharges correlate with lower tips.")
        else:
            st.success("No strong evidence of Crowding Out.")
            
    else:
        st.warning("Economics data missing.")

# ============================================================================
# TAB 4: WEATHER (RAIN TAX)
# ============================================================================

def tab_weather():
    st.header("🌧️ Tab 4: The Rain Tax")
    st.markdown("**Hypothesis**: Rain Elasticity of Demand.")
    
    trips = DATA.get('trips')
    weather = DATA.get('weather')
    
    if trips is not None and weather is not None:
        # Merge
        # Ensure date format matches
        try:
            trips['date'] = pd.to_datetime(trips['date'])
            weather['date'] = pd.to_datetime(weather['date'])
            merged = pd.merge(trips, weather, on='date')
        except:
             st.error("Date format mismatch between trips and weather data")
             return

        col1, col2 = st.columns([2, 1])
        with col1:
            fig = px.scatter(
                merged, 
                x='precipitation', 
                y='trips', 
                trendline='ols',
                title="Daily Trips vs Precipitation (2025)",
                labels={'precipitation': 'Precipitation (mm)', 'trips': 'Trip Count'}
            )
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            st.write("### Analysis")
            if 'elasticity_text' in DATA:
                st.text(DATA['elasticity_text'])
            
            corr = merged['trips'].corr(merged['precipitation'])
            st.metric("Correlation", f"{corr:.3f}")
            
            if abs(corr) < 0.3:
                st.info("Demand is Inelastic (Weather has little effect).")
            else:
                st.info("Demand is Elastic (Weather affects trips).")

# ============================================================================
# MAIN APP
# ============================================================================

def main():
    st.sidebar.title("Navigation")
    tab = st.sidebar.radio("Go to", ["Summary", "The Map", "The Flow", "The Economics", "The Weather"])
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Executive Stats")
    
    stats = DATA.get('stats', {})
    if stats:
        st.sidebar.metric("2025 Revenue", f"${stats.get('revenue_2025', 0):,.0f}")
        val = stats.get('q1_pct_change', 0)
        st.sidebar.metric("Q1 Volume Change", f"{val:.2f}%")
        
    audit = DATA.get('ghost')
    if audit is not None:
        st.sidebar.metric("Ghost Trips Flagged", f"{len(audit):,}")

    if tab == "Summary":
        st.title("🚖 NYC Congestion Pricing Audit")
        st.markdown("### Executive Summary")
        st.write("This dashboard analyzes the impact of the 2025 Congestion Pricing implemented Jan 5, 2025.")
        
        leakage = DATA.get('leakage')
        if leakage is not None:
            st.markdown("### 🚨 Leakage Alert")
            st.write("Top locations with missing surcharges:")
            st.dataframe(leakage.head(5))
            
        if stats:
            st.info(f"Total Estimated Revenue 2025: **${stats.get('revenue_2025', 0):,.2f}**")
            
    elif tab == "The Map":
        tab_map()
    elif tab == "The Flow":
        tab_flow()
    elif tab == "The Economics":
        tab_economics()
    elif tab == "The Weather":
        tab_weather()

if __name__ == "__main__":
    main()
