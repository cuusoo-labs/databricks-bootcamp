# Databricks notebook source
import warnings
import json
import os
import time
warnings.filterwarnings("ignore")

import mlflow

# COMMAND ----------

def _get_model_name(model_class):
  out = str(model_class).replace('<class ', '').replace('>', '').replace("'", '')
  return out


def _transform_labels(y, model):
  encoder = model._y_encoder
  out = encoder.transform(y)
  return out


def _get_metrics(classification_report):
  keys_to_save = ['macro avg', 'weighted avg']
  out = {}
  
  for k in keys_to_save:
    for m, v in classification_report[k].items():
      _k = f'{k}__{m}'.replace(' ', '_').replace('-', '_')
      out[_k] = v

  return out


def get_or_create_experiment(project_path, experiment_name):
  experiment_path = os.path.join(project_path, experiment_name)
  
  try:
    experiment_id = mlflow.create_experiment(experiment_path)
  except:
    experiment_id = mlflow.tracking.MlflowClient().get_experiment_by_name(experiment_path).experiment_id
    
  out = {'experiment_path': experiment_path, 'experiment_id': experiment_id}
  return out

# COMMAND ----------

def run_setup(username, database, force_restart=False):
  # database exists
  database_exists = spark._jsparkSession.catalog().databaseExists(database)
  bronze_exists = spark._jsparkSession.catalog().tableExists(database, 'sensor_readings_historical_bronze')
  
  if (database_exists and bronze_exists) and not force_restart:
    pass
  else:
    setup_responses = dbutils.notebook.run("../Lab 1: Data Engineering/Utils/Setup-Batch-GDrive", 0, {"db_name": username}).split()
    dbfs_data_path = setup_responses[1]
    
    bronze_table_path = f"{dbfs_data_path}tables/bronze"
    silver_table_path = f"{dbfs_data_path}tables/silver"
    gold_table_path = f"{dbfs_data_path}tables/gold"
    
    dbutils.fs.rm(bronze_table_path, recurse=True)
    dbutils.fs.rm(silver_table_path, recurse=True)
    dbutils.fs.rm(gold_table_path, recurse=True)
    
    dataPath = f"{dbfs_data_path}historical_sensor_data.csv"
    
    df = spark.read\
      .option("header", "true").option("delimiter", ",").option("inferSchema", "true").csv(dataPath)
    
    df.write \
      .format("delta").mode("overwrite").saveAsTable(f"{database}.sensor_readings_historical_bronze")
    
  
  bronze_sample_exists = spark._jsparkSession.catalog().tableExists(database, 'sensor_readings_historical_bronze_sample')
  
  if not bronze_sample_exists:
    df = spark.sql(f'select * from {database}.sensor_readings_historical_bronze').sample(False, 0.05, 42)
    df.write.format('delta').mode('overwrite').saveAsTable(f'{database}.sensor_readings_historical_bronze_sample')    
    
  out = {
    "database_name": database
  }
  
  return out

# COMMAND ----------

def generate_data(database, delay_sec=5, num_batches=10):
  for i in range(num_batches):
    df = spark.sql(f'select * from {database}.training_data').sample(False, 0.001).limit(10)
    df.write.format('delta').mode('append').saveAsTable(f'{database}.bronze_streaming_cp')
    time.sleep(delay_sec)

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report

import mlflow
import mlflow.pyfunc

def check_if_need_retrain(model_name, database_name, performance_threshold=0.7):
  client = MlflowClient()
  out = False
  
  # Step 1: Get latest production model
  production_model = client.get_latest_versions(model_name, ['Production'])[0]
  model = mlflow.pyfunc.load_model(production_model.source)
  
  # Step 2: Measure performance
  df = spark.sql(f'select * from {database_name}.training_data').toPandas()
  
  preds = model.predict(df)
  
  label_encoder = LabelEncoder().fit(df['device_operational_status'])
  
  pred_y_num = label_encoder.transform(preds)
  true_y_num = label_encoder.transform(df['device_operational_status'])
    
  f1 = classification_report(true_y_num, pred_y_num, output_dict=True)['weighted avg']['f1-score']
  
  if f1 < performance_threshold:
    out = True
    
  return {'need_retrain': out, 'score': f1}


def retrain(model_name, experiment_id, database_name):
  latest_model = client.get_latest_versions(model_name, ['Production'])[0]
  
  out_performance = json.loads(
    dbutils.notebook.run('./_model', timeout_seconds=1200, arguments={'database_name': database_name, 'experiment_id': experiment_id})
  )
  
  return out_performance


def promote_to_prod(model_name, run_id):
  client = MlflowClient()
  old_model = client.get_latest_versions(model_name, ['Production'])[0]
  
  # Step 1: Register to prod
  new_model = mlflow.register_model(f'runs:/{run_id}/model', model_name)
  client.transition_model_version_stage(name=model_name, version=new_model.version, stage='Production')
  print('Moved new model to Production')
  
  # Step 2: Move last prod to archive
  client.transition_model_version_stage(name=model_name, version=old_model.version, stage='Archived')
  print('Moved old model to Archived')
  
  displayHTML(f"<h2>Check your new model <a href='#mlflow/models/{model_name}/versions/{new_model.version}'>here</a></h2>")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window

def calculate_window_features(df):
  aggs = df\
    .groupBy("device_id", window("reading_time", "5 minutes"))\
    .agg(
      mean("reading_1").alias("mean_5m_reading_1"),
      mean("reading_2").alias("mean_5m_reading_2"),
      mean("reading_3").alias("mean_5m_reading_3"),
    )

  features = df.select('id', 'reading_time')\
    .join(aggs, [df.device_id == aggs.device_id, df.reading_time >= aggs.window.end])\
    .withColumn("rank", row_number().over(Window.partitionBy(df.id).orderBy(desc(aggs.window.end))))\
    .filter(col("rank") == 1) \
    .select("id", "mean_5m_reading_1", "mean_5m_reading_2", "mean_5m_reading_3")
  
  return features
