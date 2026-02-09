# AMR-Fleet-Health-Monitoring-Pipeline 🤖📊

A scalable data engineering pipeline built on **Databricks** and **PySpark** to monitor, analyze, and detect anomalies in Autonomous Mobile Robot (AMR) fleets.

## 🚀 Tech Stack
- **Language:** Python (PySpark)
- **Platform:** Databricks
- **Storage:** Delta Lake (Bronze/Silver architecture)
- **Libraries:** Pandas, NumPy, PySpark SQL

## 🛠️ Pipeline Features
- **Real-time Simulation:** Generates synthetic telemetry logs (battery, temperature, status) for multiple AMRs.
- **Delta Lake Integration:** Persists raw and processed data in Delta tables for reliability and performance.
- **Anomaly Detection:** Uses PySpark Window functions to calculate temperature deltas and flag sudden spikes (potential motor issues).
- **Fleet Summary:** Aggregates health metrics to identify the most unreliable units in the fleet.

