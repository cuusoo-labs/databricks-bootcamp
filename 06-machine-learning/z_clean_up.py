# Databricks notebook source
# MAGIC %run ./_utils

# COMMAND ----------

# MAGIC %run "../Lab 1: Data Engineering/Utils/Fetch_User_Metadata"

# COMMAND ----------

spark.sql(f"DROP DATABASE {DATABASE_NAME} CASCADE")

# COMMAND ----------

import mlflow

experiment_config = get_or_create_experiment(PROJECT_PATH, 'Plant Status Prediction')
experiment_path = experiment_config['experiment_path']
experiment_id = experiment_config['experiment_id']

mlflow.delete_experiment(experiment_id)

# COMMAND ----------

MODEL_NAME = f'sensor_status__{USERNAME}'

client = mlflow.tracking.MlflowClient()
version = client.get_latest_versions(MODEL_NAME, ['Production'])[0].version
client.transition_model_version_stage(MODEL_NAME, version, 'Archived')

client.delete_registered_model(MODEL_NAME)

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

fs._catalog_client.delete_feature_table(f'{DATABASE_NAME}.window_features')

# COMMAND ----------


