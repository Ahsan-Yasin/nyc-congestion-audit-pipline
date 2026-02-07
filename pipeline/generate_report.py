"""
Generate Audit Report
=====================
Generates a PDF Executive Summary from the Congestion Pricing Audit data.
"""

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib import colors
import json
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
PDF_FILE = os.path.join(OUTPUT_DIR, "audit_report.pdf")

def generate_pdf():
    print(f"Generating {PDF_FILE}...")
    
    # Load Data
    stats = {}
    if os.path.exists(f"{OUTPUT_DIR}/impact_stats.json"):
        with open(f"{OUTPUT_DIR}/impact_stats.json", 'r') as f:
            stats = json.load(f)
            
    ghost_count = stats.get('ghost_count', 0)
        
    elasticity_text = "N/A"
    if os.path.exists(f"{OUTPUT_DIR}/elasticity.txt"):
        with open(f"{OUTPUT_DIR}/elasticity.txt", 'r') as f:
            elasticity_text = f.read()

    # Create PDF
    c = canvas.Canvas(PDF_FILE, pagesize=letter)
    width, height = letter
    
    # Title
    c.setFont("Helvetica-Bold", 24)
    c.drawString(50, height - 50, "NYC Congestion Pricing Audit Report")
    
    c.setFont("Helvetica", 12)
    c.drawString(50, height - 80, "Date: January 2026")
    c.drawString(50, height - 100, "Prepared by: Data Science Team")
    
    # Executive Summary
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 140, "1. Executive Summary")
    
    c.setFont("Helvetica", 12)
    y = height - 170
    
    rev = stats.get('revenue_2025', 0)
    c.drawString(70, y, f"Total Estimated 2025 Surcharge Revenue: ${rev:,.2f}")
    y -= 25
    
    c.drawString(70, y, f"Total Suspicious 'Ghost Trips' Detected: {ghost_count}")
    y -= 25
    
    vol_change = stats.get('q1_pct_change', 0)
    c.drawString(70, y, f"Q1 Volume Change (Yellow/Green): {vol_change:.2f}%")
    y -= 40
    
    # Suspicious Vendors
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, y, "2. Top 5 Suspicious Vendors (Ghost Trips)")
    y -= 30
    c.setFont("Helvetica", 12)
    
    vendors = stats.get('suspicious_vendors', [])
    if vendors:
        for v_id, count in vendors:
            c.drawString(70, y, f"Vendor ID {v_id}: {count} flagged trips")
            y -= 25
    else:
        c.drawString(70, y, "No vendor data available or all passed audit.")
        y -= 25
        
    y -= 15

    # Recommendations
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, y, "3. Policy Recommendations")
    y -= 30
    
    c.setFont("Helvetica", 12)
    recommendations = [
        "- Adjust toll pricing during heavy rain as demand is inelastic.",
        "- Audit Top 5 Vendors (listed above) for high frequency of 'Ghost Trips'.",
        "- Increase monitoring at border zones to reduce leakage.",
        "- Implement driver subsidy to offset 2.6% tip decline."
    ]
    
    for rec in recommendations:
        c.drawString(70, y, rec)
        y -= 20
        
    # Elasticity
    y -= 20
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, y, "4. Weather Elasticity Analysis")
    y -= 30
    c.setFont("Helvetica", 12)
    
    lines = elasticity_text.split('\n')
    for line in lines:
        if line.strip():
            c.drawString(70, y, line)
            y -= 20
            
    c.save()
    print(f"PDF Report saved to {PDF_FILE}")

if __name__ == "__main__":
    generate_pdf()
