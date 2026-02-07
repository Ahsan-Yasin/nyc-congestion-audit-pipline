"""
NYC Congestion Pricing Audit - Optimized Pipeline
=================================================
A robust, memory-efficient ETL pipeline using DuckDB to process NYC TLC data.
Prioritizes out-of-core processing to prevent system crashes.

Features:
- Direct Parquet querying (No massive CSV intermediate files).
- DuckDB based "Aggregation First" strategy.
- Automatic missing data imputation for Dec 2025.
- Ghost Trip Analysis (Vendor Audit) and Zone Impact logic.
- Graceful degradation if optional libraries (Pandas, Scipy) are missing.

Author: Antigravity
Date: January 2026
"""

import os
import sys
import json
import time
import requests
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# --- Robust Imports ---
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("WARNING: Pandas not found. Some analysis steps will be skipped.")

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    print("WARNING: Numpy not found.")

try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("WARNING: Scipy not found. Regression analysis will be skipped.")

# ============================================================================
# CONFIGURATION
# ============================================================================

# Directory Structure
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data_downloads"
OUTPUT_DIR = BASE_DIR / "output"
CACHE_DIR = BASE_DIR / "cache"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True, parents=True)
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
CACHE_DIR.mkdir(exist_ok=True, parents=True)

# TLC Data Source
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPES = ['yellow', 'green']

# Congestion Zone: Manhattan South of 60th St
CONGESTION_ZONE_IDS = (
    4, 12, 13, 24, 41, 42, 43, 45, 48, 50, 68, 74, 75, 87, 88, 90, 100, 103,
    104, 105, 107, 113, 114, 116, 120, 125, 127, 128, 137, 140, 142, 143, 144,
    148, 151, 152, 153, 158, 161, 162, 163, 164, 166, 170, 186, 194, 202, 209,
    211, 212, 213, 214, 216, 217, 224, 229, 230, 231, 232, 233, 234, 235, 236,
    237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 249, 250
)

# Ghost Trip Thresholds
GHOST_SPEED_LIMIT = 65.0  # MPH
GHOST_I_TELEPORTER_TIME = 1.0  # Minutes
GHOST_I_TELEPORTER_FARE = 20.0  # Dollars
GHOST_I_STATIONARY_DIST = 0.0  # Miles

# Weather API
CENTRAL_PARK_LAT = 40.7829
CENTRAL_PARK_LON = -73.9654


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def download_file(url: str, dest_path: Path):
    """Downloads a file if it doesn't exist, with progress printing."""
    if dest_path.exists() and dest_path.stat().st_size > 1024:
        return

    print(f"Downloading {url} to {dest_path.name}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"  -> Downloaded {dest_path.name}")
    except Exception as e:
        print(f"  -> Failed to download {url}: {e}")
        # Remove partial file
        if dest_path.exists():
            dest_path.unlink()

def get_duckdb_conn():
    """Creates a memory-optimized DuckDB connection."""
    conn = duckdb.connect(database=':memory:')
    conn.execute("SET memory_limit='4GB'")
    conn.execute("SET threads=4")
    return conn

# ============================================================================
# PHASE 1: INGESTION & IMPUTATION
# ============================================================================

def ensure_data_available():
    """
    Ensures that Parquet files for Analysis are present.
    Downloads missing files and Imputes December 2025 if missing.
    """
    print("\\n[PHASE 1] Checking Data Availability...")
    
    # 1. Zone Lookup (For Map)
    lookup_ur = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    download_file(lookup_ur, DATA_DIR / "taxi_zone_lookup.csv")

    # 2. Standard Data (Jan-Nov 2025 and comparison year 2024, source 2023)
    required_downloads = []
    
    # 2025 Jan-Nov
    for month in range(1, 12):
        for taxi in TAXI_TYPES:
            required_downloads.append((2025, month, taxi))
            
    # 2024 Full Year
    for month in range(1, 13):
        for taxi in TAXI_TYPES:
            required_downloads.append((2024, month, taxi))

    # 2023 Dec (Source)
    for taxi in TAXI_TYPES:
        required_downloads.append((2023, 12, taxi))

    for year, month, taxi in required_downloads:
        file_name = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
        url = f"{TLC_BASE_URL}/{file_name}"
        dest_dir = DATA_DIR / str(year) / taxi
        dest_dir.mkdir(parents=True, exist_ok=True)
        download_file(url, dest_dir / file_name)

    # Impute December 2025
    print("  -> Checking for December 2025 Data (Imputation Step)...")
    impute_december_data()

def impute_december_data():
    """
    Imputes Dec 2025 data if missing, using weighted average of Dec 2023 (30%) and Dec 2024 (70%).
    """
    conn = get_duckdb_conn()
    
    for taxi in TAXI_TYPES:
        target_dir = DATA_DIR / "2025" / taxi
        target_file = target_dir / f"{taxi}_tripdata_2025-12.parquet"
        
        if target_file.exists():
            print(f"  -> {target_file.name} already exists.")
            continue
            
        print(f"  -> Generating imputed data for {taxi} Dec 2025...")
        
        src_2023 = DATA_DIR / "2023" / taxi / f"{taxi}_tripdata_2023-12.parquet"
        src_2024 = DATA_DIR / "2024" / taxi / f"{taxi}_tripdata_2024-12.parquet"
        
        if not src_2023.exists() or not src_2024.exists():
            print(f"  -> WARNING: Missing source data for imputation. Skipping {taxi}")
            continue

        try:
            print("     - Sampling 2023 (30%) & 2024 (70%)...")
            
            pickup_col = 'tpep_pickup_datetime' if taxi == 'yellow' else 'lpep_pickup_datetime'
            dropoff_col = 'tpep_dropoff_datetime' if taxi == 'yellow' else 'lpep_dropoff_datetime'
            
            q_src_2023 = str(src_2023).replace('\\\\', '/')
            q_src_2024 = str(src_2024).replace('\\\\', '/')
            q_target = str(target_file).replace('\\\\', '/')

            query = f"""
            COPY (
                SELECT * REPLACE (
                    {pickup_col} + INTERVAL 2 YEAR AS {pickup_col},
                    {dropoff_col} + INTERVAL 2 YEAR AS {dropoff_col}
                )
                FROM '{q_src_2023}'
                WHERE random() < 0.3
                
                UNION ALL
                
                SELECT * REPLACE (
                    {pickup_col} + INTERVAL 1 YEAR AS {pickup_col},
                    {dropoff_col} + INTERVAL 1 YEAR AS {dropoff_col}
                )
                FROM '{q_src_2024}'
                WHERE random() < 0.7
            ) TO '{q_target}' (FORMAT PARQUET)
            """
            conn.execute(query)
            print(f"  -> Imputed file created: {target_file.name}")
            
        except Exception as e:
            print(f"  -> Error imputing data for {taxi}: {e}")

# ============================================================================
# PHASE 2: GHOST TRIP FILTER & AUDIT
# ============================================================================

def run_ghost_trip_audit(conn):
    print("\\n[PHASE 2] Auditing for Ghost Trips...")
    
    yellow_glob = str(DATA_DIR / "2025/yellow/*.parquet").replace('\\\\', '/')
    green_glob = str(DATA_DIR / "2025/green/*.parquet").replace('\\\\', '/')
    
    query_view = f"""
    CREATE OR REPLACE VIEW all_trips_2025 AS
    SELECT 
        VendorID,
        'yellow' as type,
        tpep_pickup_datetime as pickup_time,
        tpep_dropoff_datetime as dropoff_time,
        PULocationID as pickup_loc,
        DOLocationID as dropoff_loc,
        trip_distance,
        fare_amount as fare,
        total_amount,
        COALESCE(congestion_surcharge, 0) as congestion_surcharge
    FROM read_parquet('{yellow_glob}', union_by_name=True)
    UNION ALL
    SELECT 
        VendorID,
        'green' as type,
        lpep_pickup_datetime as pickup_time,
        lpep_dropoff_datetime as dropoff_time,
        PULocationID as pickup_loc,
        DOLocationID as dropoff_loc,
        trip_distance,
        fare_amount as fare,
        total_amount,
        COALESCE(congestion_surcharge, 0) as congestion_surcharge
    FROM read_parquet('{green_glob}', union_by_name=True)
    """
    try:
        conn.execute(query_view)
    except Exception as e:
        print(f"  -> Error creating view: {e}. Are files downloaded?")
        return 0, []
    
    audit_file = str(OUTPUT_DIR / 'ghost_trip_audit.csv').replace('\\\\', '/')
    
    audit_query = f"""
    COPY (
        SELECT 
            *,
            date_diff('minute', pickup_time, dropoff_time) as duration_min,
            CASE 
                WHEN date_diff('minute', pickup_time, dropoff_time) <= 0 THEN 0 
                ELSE (trip_distance / (date_diff('minute', pickup_time, dropoff_time) / 60.0)) 
            END as speed_mph,
            CASE
                WHEN (trip_distance / (GREATEST(date_diff('minute', pickup_time, dropoff_time), 0.1) / 60.0)) > {GHOST_SPEED_LIMIT} THEN 'Impossible Physics'
                WHEN date_diff('minute', pickup_time, dropoff_time) < {GHOST_I_TELEPORTER_TIME} AND fare > {GHOST_I_TELEPORTER_FARE} THEN 'Teleporter'
                WHEN trip_distance = {GHOST_I_STATIONARY_DIST} AND fare > 0 THEN 'Stationary Ride'
                ELSE 'OK'
            END as fraud_flag
        FROM all_trips_2025
        WHERE 
            (trip_distance / (GREATEST(date_diff('minute', pickup_time, dropoff_time), 0.1) / 60.0)) > {GHOST_SPEED_LIMIT}
            OR (date_diff('minute', pickup_time, dropoff_time) < {GHOST_I_TELEPORTER_TIME} AND fare > {GHOST_I_TELEPORTER_FARE})
            OR (trip_distance = {GHOST_I_STATIONARY_DIST} AND fare > 0)
    ) TO '{audit_file}' (HEADER, FORMAT CSV)
    """
    
    print("  -> Executing Audit Query...")
    conn.execute(audit_query)
    
    count = conn.execute(f"SELECT COUNT(*) FROM '{audit_file}'").fetchone()[0]
    print(f"  -> {count} suspicious trips flagged.")
    
    # Vendor Audit
    print("  -> Auditing Vendors...")
    vendor_query = f"""
    SELECT VendorID, COUNT(*) as ghost_count
    FROM '{audit_file}'
    GROUP BY VendorID
    ORDER BY ghost_count DESC
    LIMIT 5
    """
    vendors = conn.execute(vendor_query).fetchall()
    print(f"  -> Top suspicious vendor code: {vendors[0][0] if vendors else 'None'}")
    
    return count, vendors

# ============================================================================
# PHASE 3: IMPACT ANALYSIS & AGGREGATIONS
# ============================================================================

def run_impact_analysis(conn, ghost_count, suspicious_vendors):
    print("\\n[PHASE 3] Analyzing Congestion Impact...")
    
    start_date = '2025-01-05'
    zone_ids_str = ', '.join(map(str, CONGESTION_ZONE_IDS))
    
    # 1. Revenue
    try:
        rev_query = f"""
            SELECT SUM(congestion_surcharge) 
            FROM all_trips_2025 
            WHERE pickup_time >= '{start_date}'
            AND (pickup_loc IN ({zone_ids_str}) OR dropoff_loc IN ({zone_ids_str}))
        """
        revenue = conn.execute(rev_query).fetchone()[0]
        revenue = revenue if revenue else 0.0
        print(f"  -> Estimated 2025 Surcharge Revenue: ${revenue:,.2f}")
    except:
        revenue = 0.0

    # 2. Leakage
    leakage_file = str(OUTPUT_DIR / 'leakage_analysis.csv').replace('\\\\', '/')
    leakage_query = f"""
    COPY (
        SELECT 
            pickup_loc,
            COUNT(*) as total_trips,
            SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) as compliant_trips,
            CAST(SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as compliance_rate,
            1.0 - (CAST(SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)) as leakage_rate
        FROM all_trips_2025
        WHERE pickup_time >= '{start_date}'
          AND pickup_loc NOT IN ({zone_ids_str})
          AND dropoff_loc IN ({zone_ids_str})
        GROUP BY pickup_loc
        HAVING COUNT(*) > 100
        ORDER BY leakage_rate DESC
        LIMIT 20
    ) TO '{leakage_file}' (HEADER, FORMAT CSV)
    """
    conn.execute(leakage_query)
    print("  -> Leakage analysis saved.")
    
    # 3. Q1 Decline
    print("  -> Calculating Q1 Volume Decline...")
    def get_q1_count(year):
        yg = str(DATA_DIR / f"{year}/yellow/*.parquet").replace('\\\\', '/')
        gg = str(DATA_DIR / f"{year}/green/*.parquet").replace('\\\\', '/')
        # Careful with union_by_name if schemas drift massively, but VendorID/dates usually stable
        q_zone = f"""
        SELECT COUNT(*)
        FROM (
            SELECT tpep_dropoff_datetime as t, DOLocationID as loc FROM read_parquet('{yg}', union_by_name=True)
            UNION ALL
            SELECT lpep_dropoff_datetime as t, DOLocationID as loc FROM read_parquet('{gg}', union_by_name=True)
        )
        WHERE month(t) IN (1, 2, 3) 
          AND t >= '{year}-01-01' AND t < '{year}-04-01'
          AND loc IN ({zone_ids_str})
        """
        try:
            return conn.execute(q_zone).fetchone()[0]
        except:
            return 0

    q1_2024 = get_q1_count(2024)
    q1_2025 = get_q1_count(2025)
    diff = (q1_2025 - q1_2024) / q1_2024 * 100 if q1_2024 > 0 else 0
    print(f"     Q1 2024: {q1_2024:,} | Q1 2025: {q1_2025:,} | Change: {diff:.2f}%")
    
    stats = {
        "revenue_2025": revenue,
        "q1_2024_vol": q1_2024,
        "q1_2025_vol": q1_2025,
        "q1_pct_change": diff,
        "ghost_count": ghost_count,
        "suspicious_vendors": suspicious_vendors
    }
    with open(OUTPUT_DIR / "impact_stats.json", "w") as f:
        json.dump(stats, f)
        
    # 4. Velocity Heatmap
    print("  -> Generating Velocity Heatmap Data...")
    def export_velocity(year):
        yg = str(DATA_DIR / f"{year}/yellow/*.parquet").replace('\\\\', '/')
        out_file = str(OUTPUT_DIR / f'velocity_{year}.csv').replace('\\\\', '/')
        q = f"""
        COPY (
            SELECT 
                dayofweek(tpep_pickup_datetime) as dow,
                hour(tpep_pickup_datetime) as hour,
                AVG(trip_distance / (GREATEST(date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime), 1) / 60.0)) as avg_speed
            FROM read_parquet('{yg}', union_by_name=True)
            WHERE month(tpep_pickup_datetime) IN (1, 2, 3)
              AND DOLocationID IN ({zone_ids_str})
              AND date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) > 1
              AND trip_distance > 0.1
              AND (trip_distance / (GREATEST(date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime), 1) / 60.0)) < 100
            GROUP BY 1, 2
        ) TO '{out_file}' (HEADER, FORMAT CSV)
        """
        try:
            conn.execute(q)
        except Exception as e:
            print(f"     Warning: Velocity query failed for {year}: {e}")

    export_velocity(2024)
    export_velocity(2025)
    
    # 5. Border Effect
    print("  -> Generating Border Effect Data...")
    border_file = str(OUTPUT_DIR / 'border_effect.csv').replace('\\\\', '/')
    yg_2024 = str(DATA_DIR / "2024/yellow/*.parquet").replace('\\\\', '/')
    yg_2025 = str(DATA_DIR / "2025/yellow/*.parquet").replace('\\\\', '/')
    
    border_query = f"""
    COPY (
        WITH q1_2024 AS (
            SELECT DOLocationID as loc, COUNT(*) as cnt 
            FROM read_parquet('{yg_2024}', union_by_name=True)
            WHERE month(tpep_dropoff_datetime) IN (1,2,3)
            GROUP BY 1
        ),
        q1_2025 AS (
            SELECT DOLocationID as loc, COUNT(*) as cnt 
            FROM read_parquet('{yg_2025}', union_by_name=True)
            WHERE month(tpep_dropoff_datetime) IN (1,2,3)
            GROUP BY 1
        )
        SELECT 
            COALESCE(a.loc, b.loc) as location_id,
            COALESCE(a.cnt, 0) as count_2024,
            COALESCE(b.cnt, 0) as count_2025,
            (COALESCE(b.cnt, 0) - COALESCE(a.cnt, 0)) as diff,
            CASE WHEN COALESCE(a.cnt, 0) > 0 
                 THEN (COALESCE(b.cnt, 0) - COALESCE(a.cnt, 0)) * 100.0 / a.cnt 
                 ELSE 0 END as pct_change
        FROM q1_2024 a
        FULL OUTER JOIN q1_2025 b ON a.loc = b.loc
    ) TO '{border_file}' (HEADER, FORMAT CSV)
    """
    try:
        conn.execute(border_query)
    except Exception as e:
        print(f"    Warning: Border effect failed: {e}")

# ============================================================================
# PHASE 4: WEATHER & ECONOMICS
# ============================================================================

def fetch_weather_and_analyze(conn):
    print("\\n[PHASE 4] Weather & Economics...")
    
    # 1. Fetch Weather
    weather_file = CACHE_DIR / "weather_2025.csv"
    if not weather_file.exists():
        try:
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                'latitude': CENTRAL_PARK_LAT,
                'longitude': CENTRAL_PARK_LON,
                'start_date': '2025-01-01',
                'end_date': '2025-12-31',
                'daily': 'precipitation_sum',
                'timezone': 'America/New_York'
            }
            r = requests.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            
            if PANDAS_AVAILABLE:
                df_weather = pd.DataFrame({
                    'date': data['daily']['time'],
                    'precipitation': data['daily']['precipitation_sum']
                })
                df_weather.to_csv(weather_file, index=False)
                print("  -> Weather data downloaded.")
            else:
                 # Minimal CSV write
                with open(weather_file, "w") as f:
                    f.write("date,precipitation\\n")
                    for d, p in zip(data['daily']['time'], data['daily']['precipitation_sum']):
                        f.write(f"{d},{p}\\n")
                print("  -> Weather data downloaded (Manual CSV).")

        except Exception as e:
            print(f"  -> Failed to fetch weather: {e}")
            
    # 2. Daily Trip Counts
    out_trips = str(OUTPUT_DIR / 'daily_trips_2025.csv').replace('\\\\', '/')
    daily_trips_query = f"""
    COPY (
        SELECT 
            CAST(pickup_time AS DATE) as date,
            COUNT(*) as trips
        FROM all_trips_2025
        GROUP BY 1
        ORDER BY 1
    ) TO '{out_trips}' (HEADER, FORMAT CSV)
    """
    conn.execute(daily_trips_query)
    
    # 3. Tip Economics
    out_tips = str(OUTPUT_DIR / 'tips_economics.csv').replace('\\\\', '/')
    tips_query = f"""
    COPY (
        SELECT 
            month(pickup_time) as month,
            AVG(congestion_surcharge) as avg_surcharge,
            AVG(CASE WHEN fare > 0 THEN (total_amount - fare)/fare ELSE 0 END) * 100 as avg_tip_pct
        FROM all_trips_2025
        GROUP BY 1
        ORDER BY 1
    ) TO '{out_tips}' (HEADER, FORMAT CSV)
    """
    conn.execute(tips_query)

    # 4. Elasticity
    if PANDAS_AVAILABLE and SCIPY_AVAILABLE:
        try:
            df_trips = pd.read_csv(OUTPUT_DIR / "daily_trips_2025.csv")
            df_weather = pd.read_csv(weather_file)
            df_merge = pd.merge(df_trips, df_weather, on='date')
            df_merge = df_merge.dropna()
            
            if len(df_merge) > 10:
                correlation = df_merge['trips'].corr(df_merge['precipitation'])
                slope, intercept, r_value, p_value, std_err = stats.linregress(
                    df_merge['precipitation'], df_merge['trips']
                )
                print(f"  -> Rain Elasticity (Correlation): {correlation:.4f}")
                with open(OUTPUT_DIR / "elasticity.txt", "w") as f:
                    f.write(f"Correlation: {correlation}\\nSlope: {slope}\\n")
        except Exception as e:
            print(f"  -> Economics analysis error: {e}")

# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

def main():
    print("="*60)
    print("Starting NYC Congestion Pricing Audit Pipeline (Final)")
    print("="*60)
    
    ensure_data_available()
    print("\\nInitializing Database Connection...")
    conn = get_duckdb_conn()
    
    try:
        count, vendors = run_ghost_trip_audit(conn)
        run_impact_analysis(conn, count, vendors)
        fetch_weather_and_analyze(conn)
        
    except Exception as e:
        print(f"\\nCRITICAL PIPELINE ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        
    print("\\nPipeline Completed.")

if __name__ == "__main__":
    main()