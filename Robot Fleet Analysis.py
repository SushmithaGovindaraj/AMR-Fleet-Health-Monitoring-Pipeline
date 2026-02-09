# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Generation

# COMMAND ----------


def generate_robot_logs(rows=2000):
    robots = ['AMR_001', 'AMR_002', 'AMR_003']
    data = []
    start_time = datetime.now()
    
    for i in range(rows):
        r_id = np.random.choice(robots)
        # Normal temp is 35-40. Robot 002 will have a "hidden" motor issue.
        temp = 38 + np.random.normal(0, 2)
        if r_id == 'AMR_002' and i > 1200:
            temp += np.random.uniform(10, 25) # The failure event
            
        data.append({
            "timestamp": start_time + timedelta(seconds=i*10),
            "robot_id": r_id,
            "battery_pct": max(0, 100 - (i * 0.01) % 100),
            "motor_temp": temp,
            "status": np.random.choice(['Moving', 'Idle', 'Charging'], p=[0.8, 0.1, 0.1])
        })
    return pd.DataFrame(data)

raw_df = spark.createDataFrame(generate_robot_logs())
raw_df.write.format("delta").mode("overwrite").saveAsTable("robot_telemetry")

print("Complete: Table 'robot_telemetry' is ready!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze Data

# COMMAND ----------


# Load raw data
df = spark.table("robot_telemetry")

# Define the Window (group by robot, sort by time)
robot_window = Window.partitionBy("robot_id").orderBy("timestamp")

# Calculate "Temperature Delta" (Current Temp - Previous Temp)
analysis_df = df.withColumn("prev_temp", F.lag("motor_temp").over(robot_window)) \
              .withColumn("temp_spike", F.col("motor_temp") - F.col("prev_temp")) \
              .withColumn("is_alert", F.when(F.col("temp_spike") > 8, 1).otherwise(0))


analysis_df.write.format("delta").mode("overwrite").saveAsTable("robot_health")

print("Complete:Table with 'temp_spike' logic is ready!")

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary

# COMMAND ----------


# Create a summary of which robots are most "unhealthy"
summary = spark.table("robot_health") \
    .groupBy("robot_id") \
    .agg(
        F.count(F.when(F.col("is_alert") == 1, 1)).alias("total_anomalies"),
        F.avg("motor_temp").alias("avg_operating_temp")
    ).orderBy("total_anomalies", ascending=False)

display(summary)