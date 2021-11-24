# Databricks notebook source
# MAGIC %run ./_utils

# COMMAND ----------

dbutils.widgets.text('database_name', '')

# COMMAND ----------

DATABASE_NAME = dbutils.widgets.get('database_name')
TABLE_NAME = 'sensor_readings_historical_bronze_sample'
VERSION = sql(f'describe history {DATABASE_NAME}.{TABLE_NAME} LIMIT 1').collect()[0].version

df = spark.sql(f"select * from {DATABASE_NAME}.{TABLE_NAME}")

# COMMAND ----------

# MAGIC %md #Step 1: Calculate Features and update feature store

# COMMAND ----------

features = calculate_window_features(df)

# COMMAND ----------

from databricks import feature_store

fs = feature_store.FeatureStoreClient()

fs.write_table(
    name="{}.window_features".format(DATABASE_NAME),
    df=features,
    mode="overwrite"
)

# COMMAND ----------

# MAGIC %md #Step 2: Update training data

# COMMAND ----------

from databricks.feature_store import FeatureLookup

feature_table = "{}.window_features".format(DATABASE_NAME)

feature_lookups = [
    FeatureLookup( 
      table_name = feature_table,
      feature_name = "mean_5m_reading_1",
      lookup_key = ["id"],
    ),
    FeatureLookup( 
      table_name = feature_table,
      feature_name = "mean_5m_reading_2",
      lookup_key = ["id"],
    ),
  FeatureLookup( 
      table_name = feature_table,
      feature_name = "mean_5m_reading_3",
      lookup_key = ["id"],
    ),
]

training_set = fs.create_training_set(
  df,
  feature_lookups = feature_lookups,
  label = "device_operational_status"
)
 
training_df = training_set.load_df()

training_df.filter(training_df.mean_5m_reading_1.isNotNull())\
  .write.mode("overwrite").format("delta").saveAsTable(f"{DATABASE_NAME}.training_data")

# COMMAND ----------


