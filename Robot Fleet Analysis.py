# Databricks notebook source
# MAGIC %md
# MAGIC # AMR Fleet Health Monitoring 🦾📊
# MAGIC > **Autonomous Mobile Robot Fleet** — Real-time telemetry processing, health analysis, and anomaly detection.
# MAGIC
# MAGIC **Data Journey:**
# MAGIC 1. 📥 **Phase 1 — Raw Collection**: Ingest simulated robot telemetry
# MAGIC 2. 🧹 **Phase 2 — Data Refining**: Feature engineering and cleaning
# MAGIC 3. 💡 **Phase 3 — Fleet Insights**: Human-readable health alerts and summaries

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("Libraries loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1: Raw Collection 📥
# MAGIC *Generating realistic telemetry logs from our robot fleet.*
# MAGIC
# MAGIC | Robot   | Simulated Behaviour                        |
# MAGIC |---------|--------------------------------------------|
# MAGIC | AMR_001 | Normal operation                           |
# MAGIC | AMR_002 | Gradual motor overheating failure          |
# MAGIC | AMR_003 | Normal operation                           |
# MAGIC | AMR_004 | Accelerated battery drain issue            |
# MAGIC | AMR_005 | Normal operation                           |

# COMMAND ----------

def generate_robot_logs(rows=5000):
    robots = ['AMR_001', 'AMR_002', 'AMR_003', 'AMR_004', 'AMR_005']
    data = []
    start_time = datetime.now()

    # Each robot starts with a full battery and a random position on the warehouse floor
    robot_states = {
        r: {
            "battery": 100.0,
            "x": np.random.uniform(10, 90),
            "y": np.random.uniform(10, 90)
        } for r in robots
    }

    for i in range(rows):
        r_id = np.random.choice(robots)
        state = robot_states[r_id]

        # Base motor temperature (normal range: 35–40°C)
        temp = 38 + np.random.normal(0, 1.5)

        # AMR_002: Simulates a motor bearing failure — temperature rises gradually past row 2000
        if r_id == 'AMR_002' and i > 2000:
            temp += (i - 2000) * 0.012 + np.random.uniform(5, 15)

        # AMR_004: Simulates a faulty battery cell — drains 3x faster than healthy robots
        drain_rate = 0.15 if r_id == 'AMR_004' else 0.05
        state["battery"] = max(0, state["battery"] - drain_rate)
        if state["battery"] < 5:
            state["battery"] = 100.0  # Robot returns to charging dock

        # Simulate movement (small position changes)
        state["x"] += np.random.uniform(-1, 1)
        state["y"] += np.random.uniform(-1, 1)

        data.append({
            "timestamp":   start_time + timedelta(seconds=i * 5),
            "robot_id":    r_id,
            "battery_pct": round(state["battery"], 2),
            "motor_temp":  round(temp, 2),
            "x_coord":     round(state["x"], 2),
            "y_coord":     round(state["y"], 2),
            "status":      np.random.choice(['Moving', 'Idle', 'Charging'], p=[0.7, 0.2, 0.1])
        })

    return pd.DataFrame(data)

raw_data = spark.createDataFrame(generate_robot_logs())
raw_data.write.format("delta").mode("overwrite").saveAsTable("raw_telemetry")

print(f"✅ Phase 1 Complete — {raw_data.count():,} rows saved to 'raw_telemetry'.")
display(raw_data.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2: Data Refining 🧹
# MAGIC *Computing features that reveal hidden patterns in the robot behaviour.*
# MAGIC
# MAGIC - **Temperature Change**: How fast is temperature rising between readings?
# MAGIC - **Rolling Average Temp**: Smoothed temperature over the last 10 readings
# MAGIC - **Battery Drop Rate**: How much battery was lost since the last reading?

# COMMAND ----------

df = spark.table("raw_telemetry")

robot_window = Window.partitionBy("robot_id").orderBy("timestamp")

refined_df = (
    df
    .withColumn("prev_temp",         F.lag("motor_temp").over(robot_window))
    .withColumn("temp_change",       F.col("motor_temp") - F.col("prev_temp"))
    .withColumn("rolling_avg_temp",  F.avg("motor_temp").over(robot_window.rowsBetween(-10, 0)))
    .withColumn("battery_drop",      F.lag("battery_pct").over(robot_window) - F.col("battery_pct"))
    .fillna(0)
)

refined_df.write.format("delta").mode("overwrite").saveAsTable("refined_telemetry")

print("✅ Phase 2 Complete — Feature-engineered data saved to 'refined_telemetry'.")
display(refined_df.select("robot_id", "timestamp", "motor_temp", "temp_change", "rolling_avg_temp", "battery_pct", "battery_drop").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3: Fleet Insights 💡
# MAGIC *Translating data into clear, human-readable health status and alerts.*
# MAGIC
# MAGIC | Status               | Condition                                     |
# MAGIC |----------------------|-----------------------------------------------|
# MAGIC | ✅ HEALTHY           | All readings within normal range              |
# MAGIC | ⚠️ WARNING: Low Battery | Battery below 20%                          |
# MAGIC | 🔴 CRITICAL: Overheating | Motor temp above 50°C                    |

# COMMAND ----------

insights_df = (
    spark.table("refined_telemetry")
    .withColumn(
        "health_status",
        F.when(F.col("motor_temp") > 50,     "🔴 CRITICAL: Overheating")
         .when(F.col("battery_pct") < 20,    "⚠️ WARNING: Low Battery")
         .otherwise("✅ HEALTHY")
    )
    .withColumn(
        "needs_service",
        F.when((F.col("motor_temp") > 45) & (F.col("temp_change") > 2), 1).otherwise(0)
    )
)

insights_df.write.format("delta").mode("overwrite").saveAsTable("fleet_insights")

print("✅ Phase 3 Complete — 'fleet_insights' table is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Fleet Health Summary
# MAGIC > Which robots need attention right now?

# COMMAND ----------

summary = (
    insights_df
    .groupBy("robot_id")
    .agg(
        F.last("health_status").alias("current_status"),
        F.round(F.avg("motor_temp"), 2).alias("avg_temp_c"),
        F.round(F.min("battery_pct"), 2).alias("min_battery_pct"),
        F.sum("needs_service").alias("service_alerts")
    )
    .orderBy("service_alerts", ascending=False)
)

display(summary)