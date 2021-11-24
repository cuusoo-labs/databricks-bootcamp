# Databricks notebook source
# MAGIC %run ./_utils

# COMMAND ----------

# MAGIC %run "../Lab 1: Data Engineering/Utils/Fetch_User_Metadata"

# COMMAND ----------

import json
import time
import pandas as pd

# COMMAND ----------

# MAGIC %md #Step 0: Setup

# COMMAND ----------

experiment_config = get_or_create_experiment(PROJECT_PATH, 'Plant Status Prediction')
experiment_path = experiment_config['experiment_path']
experiment_id = experiment_config['experiment_id']

# COMMAND ----------

setup_config = run_setup(USERNAME, DATABASE_NAME)
print(setup_config)

# COMMAND ----------

sql(f'USE {DATABASE_NAME}')

# COMMAND ----------

# MAGIC %md # Step 1: Define re-train loop

# COMMAND ----------

from mlflow.tracking import MlflowClient
client = MlflowClient()

model_name = f'sensor_status__{USERNAME}'
print(f'Will be using model name: "{model_name}"')

# COMMAND ----------

def retrain_loop(model_name, database_name, experiment_id, performance_threshold=0.7):
  print('Checking if need to re-train the model')
  retrain_stats = check_if_need_retrain(model_name, database_name, performance_threshold=performance_threshold)
  
  if retrain_stats['need_retrain']:
    print(f'Re-training needed, stats={retrain_stats}')
    
    new_model_stats = retrain(model_name, experiment_id, database_name)
    print(f'New model stats={new_model_stats}')
    
    promote_to_prod(model_name, new_model_stats['run_id'])
    print('Done')
  else:
    print(f'Re-training not needed, stats={retrain_stats}')
  
  # this is just for visualisation purposes later, not needed yet
  
  df = pd.DataFrame([[retrain_stats['need_retrain'], retrain_stats['score'], int(time.time())]], columns=['needed_retrain', 'score', 'ts'])
  
  spark \
    .createDataFrame(df) \
    .write \
    .format('delta') \
    .mode('append') \
    .saveAsTable('model_performance')
  

retrain_loop(
  model_name, DATABASE_NAME, experiment_id, 0.6
)

# COMMAND ----------

# MAGIC %md #Step 2: Simulate Drift

# COMMAND ----------

# MAGIC %md ## 2.1 Run 1

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE sensor_readings_historical_bronze_sample SET reading_1 = reading_1 * 0.97;

# COMMAND ----------

dbutils.notebook.run("./_update_features", 60, {"database_name":DATABASE_NAME})

# COMMAND ----------

retrain_loop(model_name, DATABASE_NAME, experiment_id, 0.6)

# COMMAND ----------

# MAGIC %md ## 2.2 Run 2

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE sensor_readings_historical_bronze_sample SET reading_1 = reading_1 * 0.97;

# COMMAND ----------

dbutils.notebook.run("./_update_features", 60, {"database_name":DATABASE_NAME})

# COMMAND ----------

retrain_loop(model_name, DATABASE_NAME, experiment_id, 0.6)

# COMMAND ----------

# MAGIC %md ## 2.3 Run 3: Retraining model

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE sensor_readings_historical_bronze_sample SET reading_1 = reading_1 * 0.90;

# COMMAND ----------

dbutils.notebook.run("./_update_features", 60, {"database_name":DATABASE_NAME})

# COMMAND ----------

retrain_loop(model_name, DATABASE_NAME, experiment_id, 0.6)

# COMMAND ----------

# MAGIC %md ##2.4 Run 4: Verifying old performance

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE sensor_readings_historical_bronze_sample SET reading_1 = reading_1 * 1.02

# COMMAND ----------

dbutils.notebook.run("./_update_features", 60, {"database_name":DATABASE_NAME})

# COMMAND ----------

retrain_loop(model_name, DATABASE_NAME, experiment_id, 0.6)

# COMMAND ----------

# MAGIC %md # Step 3: Check our historical performance

# COMMAND ----------

# MAGIC %sql select * from model_performance order by ts

# COMMAND ----------


