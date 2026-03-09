# AMR Fleet Health Monitoring & Intelligence Pipeline

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Community%20Edition-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://community.cloud.databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.3+-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Enabled-003366?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-Random%20Forest-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white)](https://scikit-learn.org/)

> A production-grade **Autonomous Mobile Robot (AMR)** fleet monitoring system built on Apache Spark and Databricks. The pipeline ingests real-time telemetry, engineers health signals, and uses a **Random Forest Classifier** to predict robot failures *before* they happen.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Pipeline — The Data Journey](#data-pipeline--the-data-journey)
- [Intelligence Layer — Predictive Maintenance](#intelligence-layer--predictive-maintenance)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Fleet Simulation Details](#fleet-simulation-details)

---

## Overview

Traditional fleet monitoring systems are **reactive** — they alert engineers only after a robot has already failed. This project flips that model entirely.

By combining **PySpark-based stream processing**, **Delta Lake** for reliable data persistence, and a **Machine Learning** failure prediction model, this pipeline provides:

- ✅ Real-time telemetry ingestion and anomaly detection  
- ✅ Automated feature engineering on live sensor data  
- ✅ Human-readable health alerts (`CRITICAL`, `WARNING`, `HEALTHY`)  
- ✅ Per-robot failure probability scores and risk rankings  

---

## Architecture

```
Raw Telemetry (PySpark)
        │
        ▼
┌─────────────────────┐
│  Phase 1: Ingest    │  ← Simulated AMR sensors (battery, temp, GPS)
│  → raw_telemetry    │    Delta Lake table
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  Phase 2: Refine    │  ← Feature engineering (rolling averages,
│  → refined_telemetry│    temp change rate, battery drop rate)
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  Phase 3: Insights  │  ← Rule-based health classification
│  → fleet_insights   │    (HEALTHY / WARNING / CRITICAL)
└────────┬────────────┘
         │
         ▼
┌──────────────────────────────┐
│  Intelligence Layer (ML)     │  ← Random Forest Classifier
│  → failure_probability score │    Predicts needs_service = 1
│  → risk_level (HIGH/MED/LOW) │    per robot reading
└──────────────────────────────┘
```

---

## Data Pipeline — The Data Journey

The pipeline is organized into three sequential phases, each producing a Delta Lake table consumed by the next.

### Phase 1 — Raw Collection 📥

**Notebook:** `Robot Fleet Analysis.py` (Cells 1–2)  
**Output Table:** `raw_telemetry`

Simulates a warehouse fleet of 5 AMRs streaming telemetry every 5 seconds across 5,000 readings. Each record captures:

| Field | Description |
|---|---|
| `timestamp` | UTC timestamp of the reading |
| `robot_id` | Unique robot identifier (AMR_001 → AMR_005) |
| `battery_pct` | Current battery level (0–100%) |
| `motor_temp` | Motor temperature in °C |
| `x_coord` / `y_coord` | Position on warehouse floor |
| `status` | Operational state: Moving / Idle / Charging |

---

### Phase 2 — Data Refining 🧹

**Notebook:** `Robot Fleet Analysis.py` (Cell 3)  
**Output Table:** `refined_telemetry`

Applies PySpark **Window Functions** (partitioned per robot, ordered by timestamp) to compute derived signals:

| Feature | Formula | Purpose |
|---|---|---|
| `temp_change` | `motor_temp - lag(motor_temp)` | Rate of temperature rise |
| `rolling_avg_temp` | 10-reading rolling mean of `motor_temp` | Smoothed thermal trend |
| `battery_drop` | `lag(battery_pct) - battery_pct` | Battery drain rate per interval |

---

### Phase 3 — Fleet Insights 💡

**Notebook:** `Robot Fleet Analysis.py` (Cells 4–5)  
**Output Table:** `fleet_insights`

Applies deterministic rules to classify each reading and flag service needs:

| Health Status | Trigger Condition |
|---|---|
| ✅ `HEALTHY` | All readings within normal range |
| ⚠️ `WARNING: Low Battery` | `battery_pct < 20%` |
| 🔴 `CRITICAL: Overheating` | `motor_temp > 50°C` |
| 🔧 `needs_service = 1` | `motor_temp > 45°C` AND `temp_change > 2°C` |

---

## Intelligence Layer — Predictive Maintenance

**Notebook:** `Predictive_Maintenance.py`  
**Model:** `sklearn.ensemble.RandomForestClassifier` (100 estimators)

Rather than reacting to failures after the fact, this layer predicts them in advance.

### Model Pipeline

| Step | Action |
|---|---|
| **1. Load** | Read `fleet_insights` Delta table into Pandas |
| **2. Features** | `rolling_avg_temp`, `temp_change`, `battery_drop`, `battery_pct` |
| **3. Train** | 80/20 train-test split, Random Forest, `random_state=42` |
| **4. Evaluate** | Accuracy score + full classification report |
| **5. Score Fleet** | Attach `failure_probability` (0–1) to every robot reading |

### Fleet Risk Output

Each robot receives a risk classification based on its average failure probability:

| Risk Level | Threshold |
|---|---|
| 🔴 `HIGH` | Average failure probability > 40% |
| ⚠️ `MEDIUM` | Average failure probability > 10% |
| ✅ `LOW` | Below 10% |

> **Prerequisite:** Run `Robot Fleet Analysis.py` *before* `Predictive_Maintenance.py` to ensure the `fleet_insights` table exists.

---

## Project Structure

```
amr-fleet-health-monitoring/
│
├── Robot Fleet Analysis.py       # Main pipeline: ingest → refine → insights
├── Predictive_Maintenance.py     # ML model: train → score → fleet risk report
├── requirements.txt              # Python dependencies
└── README.md
```

---

## Tech Stack

| Category | Technology |
|---|---|
| **Compute & Processing** | Apache Spark (PySpark) on Databricks |
| **Storage** | Delta Lake (ACID-compliant columnar tables) |
| **Machine Learning** | scikit-learn — Random Forest Classifier |
| **Data Manipulation** | NumPy, Pandas |
| **Language** | Python 3.10+ |

---

## Getting Started

### Prerequisites

- A [Databricks Community Edition](https://community.cloud.databricks.com/) account  
- A running Databricks cluster (runtime ≥ 11.x with Spark 3.3+)

### Installation

1. **Clone this repository** and upload both `.py` notebooks to your Databricks workspace.

2. **Install Python dependencies** (PySpark and Delta Lake are pre-installed on Databricks):

   ```bash
   pip install -r requirements.txt
   ```

   Or, inside a Databricks notebook cell:

   ```python
   %pip install scikit-learn numpy pandas
   ```

### Running the Pipeline

**Step 1 — Run the Data Journey:**
```
Workspace → Robot Fleet Analysis.py → Run All
```
This populates three Delta tables: `raw_telemetry`, `refined_telemetry`, `fleet_insights`.

**Step 2 — Run the Intelligence Layer:**
```
Workspace → Predictive_Maintenance.py → Run All
```
This trains the Random Forest model and outputs per-robot `failure_probability` and `risk_level`.

---

## Fleet Simulation Details

The data generator (`generate_robot_logs`) simulates realistic failure scenarios to provide a meaningful training signal for the ML model:

| Robot | Simulated Behaviour |
|---|---|
| `AMR_001` | Healthy — baseline normal operation |
| `AMR_002` | **Motor bearing failure** — temperature rises gradually after row 2,000 |
| `AMR_003` | Healthy — baseline normal operation |
| `AMR_004` | **Faulty battery cell** — drains 3× faster than healthy robots |
| `AMR_005` | Healthy — baseline normal operation |

This deliberate imbalance (3 healthy : 2 faulty) gives the Random Forest model a realistic and challenging training environment, reflected in the classification report output.
