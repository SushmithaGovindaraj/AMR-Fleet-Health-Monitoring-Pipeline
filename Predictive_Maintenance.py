# Databricks notebook source
# MAGIC %md
# MAGIC # Predictive Maintenance — Intelligence Layer 🧠
# MAGIC > **Run this notebook after `Robot Fleet Analysis` to train the failure prediction model.**
# MAGIC
# MAGIC Instead of reacting to failures, we use **Machine Learning** to predict which robots are likely to fail — before it happens.
# MAGIC
# MAGIC **Model:** Random Forest Classifier  
# MAGIC **Goal:** Predict `needs_service = 1` based on temperature trends and battery behaviour

# COMMAND ----------

# Install scikit-learn (required in Databricks Community Edition)
# MAGIC %pip install scikit-learn

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from pyspark.sql import functions as F

print("✅ Libraries loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Training Data
# MAGIC *We use the 'fleet_insights' table created by the Data Journey notebook.*

# COMMAND ----------

try:
    df = spark.table("fleet_insights").toPandas()
    print(f"✅ Loaded {len(df):,} rows from 'fleet_insights'.")
except Exception:
    print("⚠️  'fleet_insights' not found. Run 'Robot Fleet Analysis' notebook first.")
    raise

display(df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Prepare Features

# COMMAND ----------

# Features that our model will learn from
features = ['rolling_avg_temp', 'temp_change', 'battery_drop', 'battery_pct']
label    = 'needs_service'

X = df[features].fillna(0)
y = df[label].astype(int)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(f"Training samples : {len(X_train):,}")
print(f"Test samples     : {len(X_test):,}")
print(f"Failure rate     : {y.mean()*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Train the Intelligence Model 🌲

# COMMAND ----------

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

accuracy = model.score(X_test, y_test)
print(f"✅ Model Accuracy: {accuracy * 100:.2f}%")
print()
print(classification_report(y_test, model.predict(X_test), target_names=["No Service", "Needs Service"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: What the Model Learned 🔍
# MAGIC *Which signals are the strongest predictors of a robot failure?*

# COMMAND ----------

importance_df = (
    pd.DataFrame({
        'Feature':    features,
        'Importance': model.feature_importances_
    })
    .sort_values('Importance', ascending=False)
    .reset_index(drop=True)
)

display(importance_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Score the Entire Fleet 🚦
# MAGIC *Attach a failure probability to every robot reading.*

# COMMAND ----------

df['failure_probability'] = model.predict_proba(X)[:, 1]

# Aggregate to robot level
fleet_risk = (
    df.groupby('robot_id')
      .agg(
          avg_failure_prob = ('failure_probability', 'mean'),
          max_failure_prob = ('failure_probability', 'max'),
          total_alerts     = ('needs_service', 'sum')
      )
      .sort_values('avg_failure_prob', ascending=False)
      .reset_index()
)

fleet_risk['risk_level'] = fleet_risk['avg_failure_prob'].apply(
    lambda p: "🔴 HIGH" if p > 0.4 else ("⚠️ MEDIUM" if p > 0.1 else "✅ LOW")
)

display(fleet_risk)
