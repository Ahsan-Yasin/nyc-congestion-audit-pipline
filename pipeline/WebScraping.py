import os
import requests
import polars as pl

# --- CONFIGURATION ---
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data" 
#Below are the months we want to download for each year. Adjust as needed. All 12 were available at the time of writing, but this allows for flexibility if some months are missing or if you want to limit the scope.
DATA_NEEDS = {
    2025: range(1, 12),       
    2024: range(1, 12),     
    2023: range(1, 12),                 
}
TAXI_TYPES = ['yellow', 'green']
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data_downloads")

def download_file(url, save_path):      
    """Downloads a file if it doesn't exist."""
    if os.path.exists(save_path):
        # simple check: if file is too small (<1KB), it's likely corrupt.
        if os.path.getsize(save_path) < 1024:
            print(f"Removing corrupt file: {save_path}")
            os.remove(save_path)
        else:
            print(f"Skipping {save_path} (exists)")
            return True
            
    print(f"Downloading {url}...")
    try:  
        response = requests.get(url, stream=True) 
        if response.status_code == 200:  
            with open(save_path, 'wb') as f: 
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk: f.write(chunk)
            print(f"Saved to {save_path}")
            return True
        else:
            print(f"Failed {url} (Status: {response.status_code})")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False

def standardize_and_select(lf, taxi_type):
    """
    Takes a LazyFrame (single file), renames columns, 
    casts datetimes to 'us' (microseconds) to fix mismatch errors,
    and handles missing surcharge columns.
    """
    
    if taxi_type == 'yellow':
        rename_map = {
            'tpep_pickup_datetime': 'pickup_time',
            'tpep_dropoff_datetime': 'dropoff_time',
            'PULocationID': 'pickup_loc',
            'DOLocationID': 'dropoff_loc',
            'fare_amount': 'fare'
        }
    else: # green
        rename_map = {
            'lpep_pickup_datetime': 'pickup_time',
            'lpep_dropoff_datetime': 'dropoff_time',
            'PULocationID': 'pickup_loc',
            'DOLocationID': 'dropoff_loc',
            'fare_amount': 'fare'
        }
    
    # Apply rename if columns exist
    current_cols = lf.collect_schema().names()
    valid_renames = {k: v for k, v in rename_map.items() if k in current_cols}
    lf = lf.rename(valid_renames)
    
    # 2. Add congestion_surcharge if missing
    if 'congestion_surcharge' not in lf.collect_schema().names():
        lf = lf.with_columns(pl.lit(0.0).alias('congestion_surcharge'))

    # 3. STRICT SELECT & CAST 
    # This fixes the 'ns' vs 'us' mismatch by forcing everything to Microseconds ('us')
    return lf.select([
        pl.col('pickup_time').cast(pl.Datetime('us')),
        pl.col('dropoff_time').cast(pl.Datetime('us')),
        pl.col('pickup_loc').cast(pl.Int64),
        pl.col('dropoff_loc').cast(pl.Int64),
        pl.col('trip_distance').cast(pl.Float64),
        pl.col('fare').cast(pl.Float64),
        pl.col('total_amount').cast(pl.Float64),
        pl.col('congestion_surcharge').fill_null(0.0).cast(pl.Float64)
    ])

def process_and_unify(year, taxi_type):
    """
    Reads files INDIVIDUALLY to handle schema drifts, then concatenates.
    """
    print(f"\n--- Processing {year} {taxi_type} ---")
    directory = f"{OUTPUT_DIR}/{year}/{taxi_type}"
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
    
    if not files:
        print("No files found.")
        return

    try:
        # Create a list of standardized LazyFrames
        lazy_frames = []
        for f in files:
            lf = pl.scan_parquet(f)
            lf_clean = standardize_and_select(lf, taxi_type)
            lazy_frames.append(lf_clean)
        
        # Concat all of them 
   
        combined_q = pl.concat(lazy_frames, rechunk=False)

        # Execute and Write
        output_csv = f"{OUTPUT_DIR}/{year}_{taxi_type}_unified.csv"
        
        # We perform collect() here to write to CSV
        print(f"Collecting & Writing to {output_csv}...")
        df = combined_q.collect() 
        df.write_csv(output_csv)
        print(f"Success! ({df.shape[0]} rows)")

    except Exception as e:
        print(f"CRITICAL ERROR processing {year} {taxi_type}: {e}")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

    for year, months in DATA_NEEDS.items():
        for taxi in TAXI_TYPES:
            year_dir = f"{OUTPUT_DIR}/{year}/{taxi}"
            if not os.path.exists(year_dir): os.makedirs(year_dir)
            for month in months:
                file_name = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
                download_file(f"{BASE_URL}/{file_name}", f"{year_dir}/{file_name}")

    # 2. Process & Unify
    print("\nStarting Schema Unification...")
    for year in DATA_NEEDS.keys():
        for taxi in TAXI_TYPES:
            process_and_unify(year, taxi)
            
    print("\nDONE.")