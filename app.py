import subprocess
import sys
import time
import os

def main():
    print("=========================================")
    print("   NYC Congestion Pricing Audit Runner   ")
    print("=========================================")

    # 1. Run Data Ingestion (WebScraping)
    print(f"\n[1/5] Running Data Ingestion (WebScraping)...")
    try:
        subprocess.run([sys.executable, os.path.join("pipeline", "WebScraping.py")], check=True)
    except subprocess.CalledProcessError:
        print("Error encountered in WebScraping.py. Stopping.")
        return

    # 2. Run Pipeline
    print(f"\n[2/5] Running ETL Pipeline...")
    try:
        subprocess.run([sys.executable, os.path.join("pipeline", "pipeline.py")], check=True)
    except subprocess.CalledProcessError:
        print("Error encountered in pipeline.py. Stopping.")
        return

    # 3. Generate Report
    print(f"\n[3/5] Generating PDF Report...")
    try:
        subprocess.run([sys.executable, os.path.join("pipeline", "generate_report.py")], check=True)
    except subprocess.CalledProcessError:
        print("Error encountered in generate_report.py. Stopping.")
        return

    # 4. Generate Blogs
    print(f"\n[4/5] Generating Blog Content...")
    try:
        subprocess.run([sys.executable, os.path.join("pipeline", "generate_blogs.py")], check=True)
    except subprocess.CalledProcessError:
        print("Error encountered in generate_blogs.py. Stopping.")
        return

    # 5. Launch Dashboard
    print(f"\n[5/5] Launching Streamlit Dashboard...")
    print("Press Ctrl+C to stop the dashboard server.")
    dashboard_path = os.path.join("pipeline", "dashboard.py")
    try:
        # Use python -m streamlit to ensure we use the correct environment
        subprocess.run([sys.executable, "-m", "streamlit", "run", dashboard_path], check=True)
    except KeyboardInterrupt:
        print("\nDashboard stopped by user.")
    except Exception as e:
        print(f"Error launching dashboard: {e}")

if __name__ == "__main__":
    main()
