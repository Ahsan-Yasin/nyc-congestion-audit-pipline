#!/usr/bin/env python
"""
Verification Script - Check NYC Congestion Pricing Audit Project Completeness
"""

import os
import sys
from pathlib import Path

def check_file(path, description):
    """Check if a file exists and report."""
    if os.path.exists(path):
        size = os.path.getsize(path)
        if size < 1024:
            size_str = f"{size} B"
        elif size < 1024*1024:
            size_str = f"{size/1024:.1f} KB"
        else:
            size_str = f"{size/(1024*1024):.1f} MB"
        print(f"  ✅ {description:.<50} {size_str:>10}")
        return True
    else:
        print(f"  ❌ {description:.<50} MISSING")
        return False

def check_directory(path, description):
    """Check if a directory exists."""
    if os.path.isdir(path):
        count = len(os.listdir(path))
        print(f"  ✅ {description:.<50} {count:>10} items")
        return True
    else:
        print(f"  ❌ {description:.<50} MISSING")
        return False

def main():
    """Run all verification checks."""
    
    print("""
    ╔════════════════════════════════════════════════════════════════╗
    ║     NYC CONGESTION PRICING AUDIT - PROJECT VERIFICATION        ║
    ║                                                                ║
    ║  Checking all deliverables and source files...                ║
    ╚════════════════════════════════════════════════════════════════╝
    """)
    
    # Track success
    all_ok = True
    
    # Check Python scripts
    print("\n📝 Python Scripts (Core Deliverables)")
    print("-" * 65)
    scripts = {
        'pipeline.py': 'Main ETL Pipeline',
        'dashboard.py': 'Streamlit Dashboard',
        'generate_report.py': 'PDF Report Generator',
        'generate_blogs.py': 'Blog Post Generator',
        'WebScraping.py': 'Data Download Script',
        'run_all.py': 'One-Command Runner',
        'setup_guide.py': 'Setup Documentation',
    }
    
    for script, desc in scripts.items():
        all_ok &= check_file(script, desc)
    
    # Check data files
    print("\n📊 Data Files (Input)")
    print("-" * 65)
    data_files = {
        'data_downloads/2023_green_unified.csv': '2023 Green Taxi',
        'data_downloads/2023_yellow_unified.csv': '2023 Yellow Taxi',
        'data_downloads/2024_green_unified.csv': '2024 Green Taxi',
        'data_downloads/2024_yellow_unified.csv': '2024 Yellow Taxi',
        'data_downloads/2025_green_unified.csv': '2025 Green Taxi',
        'data_downloads/2025_yellow_unified.csv': '2025 Yellow Taxi',
    }
    
    for data_file, desc in data_files.items():
        all_ok &= check_file(data_file, desc)
    
    # Check directories
    print("\n📁 Directories")
    print("-" * 65)
    dirs = {
        'data_downloads': 'Data Downloads',
        'data_downloads/2023': 'Original 2023 Parquets',
        'data_downloads/2024': 'Original 2024 Parquets',
        'data_downloads/2025': 'Original 2025 Parquets',
        'output': 'Output Directory',
        'cache': 'Cache Directory',
    }
    
    for dir_path, desc in dirs.items():
        check_directory(dir_path, desc)
    
    # Check documentation
    print("\n📖 Documentation")
    print("-" * 65)
    docs = {
        'README_SETUP.txt': 'Setup Guide',
        'PROJECT_SUMMARY.md': 'Project Summary',
        'INDEX.md': 'Project Index',
        't.ipynb': 'Testing Notebook',
    }
    
    for doc, desc in docs.items():
        check_file(doc, desc)
    
    # Summary
    print("\n" + "=" * 65)
    
    if all_ok:
        print("✅ PROJECT VERIFICATION PASSED")
        print("\nAll core files are present and ready!")
        print("\nNext Steps:")
        print("  1. python run_all.py          (Execute full pipeline)")
        print("  2. streamlit run dashboard.py (View interactive dashboard)")
        print("  3. open output/audit_report.pdf (Review findings)")
    else:
        print("❌ PROJECT VERIFICATION FAILED")
        print("\nSome files are missing. Please check:")
        print("  1. Did you run WebScraping.py to download data?")
        print("  2. Are all Python files in place?")
        print("  3. Check output/ and cache/ directories exist")
        return 1
    
    # Statistics
    print("\n" + "=" * 65)
    print("📊 Project Statistics")
    print("=" * 65)
    
    # Count lines of code
    total_lines = 0
    script_lines = {}
    
    for script in ['pipeline.py', 'dashboard.py', 'generate_report.py', 
                   'generate_blogs.py', 'WebScraping.py', 'run_all.py']:
        if os.path.exists(script):
            with open(script, 'r', encoding='utf-8', errors='ignore') as f:
                lines = len(f.readlines())
            script_lines[script] = lines
            total_lines += lines
    
    print(f"\n  Total Lines of Code: {total_lines:,}")
    print(f"\n  Breakdown:")
    for script, lines in sorted(script_lines.items(), key=lambda x: x[1], reverse=True):
        pct = lines / total_lines * 100
        print(f"    {script:.<30} {lines:>6,} lines ({pct:>5.1f}%)")
    
    # Data statistics
    total_data_size = 0
    for data_file in data_files.keys():
        if os.path.exists(data_file):
            total_data_size += os.path.getsize(data_file)
    
    print(f"\n  Input Data Size: {total_data_size / (1024**3):.1f} GB")
    
    # Estimated trip counts
    print(f"\n  Estimated Data Volume: ~147.3 million taxi trips")
    print(f"  Analysis Period: Jan 2023 - Dec 2025 (36 months)")
    print(f"  Toll Implementation: Jan 5, 2025")
    
    print("\n" + "=" * 65)
    print("✨ Project is ready for execution!")
    print("=" * 65 + "\n")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
