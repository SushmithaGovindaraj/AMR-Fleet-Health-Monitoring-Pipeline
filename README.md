# AMR Fleet Health Monitoring & Intelligence 🦾📊

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)
![Databricks](https://img.shields.io/badge/Platform-Databricks-orange?logo=databricks&logoColor=white)
![ML](https://img.shields.io/badge/ML-Random%20Forest-green?logo=scikit-learn&logoColor=white)

Welcome to the next level of robot fleet management. This project has been enhanced from a simple monitoring script into a comprehensive **Intelligence & Monitoring Pipeline**.

## 🌟 The "Data Journey"
We've organized our technical logic into a human-readable "Data Journey" that describes how raw signals become actionable insights.

1.  **Phase 1: Raw Collection 📥**
    - Robots stream telemetry including battery levels, motor temperatures, and GPS coordinates.
    - We simulate realistic failure modes like gradual overheating and excessive battery drain.
2.  **Phase 2: Data Refining 🧹**
    - Technical cleaning and "Feature Engineering" (e.g., rolling temperature averages).
    - Preparing data for our AI models.
3.  **Phase 3: Fleet Insights 💡**
    - Translates complex data into human alerts like "CRITICAL: Overheating" or "WARNING: Low Battery".

## 🧠 Intelligence Layer (Predictive Maintenance)
Instead of just reacting to failures, our system now **predicts** them. Our Intelligence Layer uses a Machine Learning model to analyze trends and flag robots that are likely to fail in the near future.

## 🧠 Intelligence Layer (Predictive Maintenance)
Instead of just reacting to failures, our system now **predicts** them. Our Intelligence Layer uses a Machine Learning model to analyze trends and flag robots that are likely to fail in the near future.

## 🚀 Getting Started
1.  **Run the Pipeline**: Use `Robot Fleet Analysis.py` in Databricks to process the Data Journey.
2.  **Train the Intelligence**: Run `Predictive_Maintenance.py` to refresh the failure prediction model.

## 🛠️ Tech Stack
- **Data Engine**: PySpark & Delta Lake (Databricks)
- **Intelligence**: Scikit-learn (Random Forest)
- **Simulation**: NumPy & Pandas
