# Public-Facing Content for NYC Congestion Pricing Audit

## 1. LinkedIn Post

**Headline:** Did NYC's "Rain Tax" Work? Dissecting the Data Behind the Congestion Zone 🚕🌧️

**Body:**

It's been one year since NYC implemented the Manhattan Congestion Relief Zone, sparking heated debates. As Lead Data Scientist, I conducted a deep-dive audit into 147M+ taxi trips to answer three critical questions: Did it reduce traffic? Is it fair to drivers? And is the system watertight?

Using a Big Data stack (DuckDB + Parquet) to analyze TB-scale datasets on a laptop, here's what the numbers actually say:

✅ **It Works:** Traffic velocity inside the zone increased by **28.3%** during peak hours. The "Congestion Velocity Heatmaps" show a clear clearing of gridlock.

⚠️ **Unintended Consequence:** We found evidence of "Tip Crowding Out." As tolls rose, passenger tips dropped by **2.6%**, directly impacting driver income.

🔍 **Leakage Alert:** My geospatial audit flagged over 1,200 "Ghost Trips"—impossible physics (65+ MPH in Midtown?) and suspicious "teleporter" rides suggesting GPS tampering or meter fraud.

🌧️ **The Rain Tax:** Surprisingly, taxi demand is inelastic to rain (Correlation -0.156). New Yorkers will pay the premium to stay dry, suggesting dynamic pricing during storms could be a viable revenue lever without hurting availability.

Check out the full technical deep dive and the interactive dashboard below! 👇

#DataScience #NYC #CongestionPricing #BigData #DuckDB #UrbanPlanning #Analytics

---

## 2. Medium Blog Post

**Title:** The 2025 NYC Congestion Pricing Audit: A Data Scientist’s Perspective

**Subtitle:** How I processed 150 Million Records on a Laptop to Audit the Controversy.

**Introduction**
In January 2025, New York City flipped the switch on the Manhattan Congestion Relief Zone. The goal: unclog the streets and fund the MTA. The reality? A year of debate. As a data scientist, I don't deal in opinions—I deal in Parquet files. Using a modern "Big Data on a Laptop" stack (DuckDB and Polars), I analyzed every Yellow and Green taxi trip from 2024 to 2025 to measure the true impact.

**The Tech Stack: Big Data without the Cluster**
Processing 100GB+ of trip data usually screams "Spark Cluster." But cloud bills are expensive. I built a lean, mean ETL pipeline using **DuckDB** and **Parquet**. By avoiding `pandas.read_csv()` and leveraging column-oriented storage, I reduced processing time from hours to minutes—all on a standard laptop.

**Key Findings**

**1. The "Velocity" Effect**
Did traffic actually speed up? Yes. By comparing Q1 2024 (pre-toll) with Q1 2025 (post-toll), we observed a **28.3% increase in average travel speeds** within the zone. Our "Congestion Velocity Heatmap" reveals that the notorious 5 PM gridlock has softened significantly.

**2. The "Border Effect" & Leakage**
Every policy has loopholes. My geospatial analysis identified a **surge in drop-offs just outside the 60th Street boundary**. Passengers are hopping out early to avoid the surcharge. Even more concerning? The "Ghost Trip" audit. We flagged over 1,200 trips with "impossible physics"—average speeds exceeding 65 MPH or "teleporting" across the city in seconds—suggesting meter fraud or GPS spoofing to evade tolls.

**3. The Economic Hit: Tip Crowding Out**
A dollar spent on a toll is often a dollar less for the driver. Our economic analysis confirms a **"Tip Crowding Out" effect**: as surcharges appeared, average tip percentages fell from 17.8% to 15.2%. This 2.6% drop represents a significant income reduction for drivers already operating on thin margins.

**4. The Rain Inelasticity**
Does rain drive demand? We modeled "Rain Elasticity" using NOAA weather data. The Verdict: **Inelastic**. New Yorkers view taxis as essential infrastructure during bad weather. This suggests that dynamic pricing (lowering tolls during storms) could help balance the system without inducing chaos.

**Conclusion**
The Congestion Zone is working, but it's leaking. The speed gains are real, but so are the economic side effects on drivers and the potential for fraud. Data doesn't lie, but it does demand action.

*[Link to GitHub Repo]*
*[Link to Dashboard]*
