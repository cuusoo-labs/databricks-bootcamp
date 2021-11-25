# Databricks notebook source
# MAGIC %run ./Create_User_DB

# COMMAND ----------

# Get the email address entered by the user on the calling notebook
db_name = spark.conf.get("com.databricks.training.spark.dbName")

# Get user name

#username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
#username_replaced = username.replace(".", "_").replace("@","_")
username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')

base_table_path = f"dbfs:/FileStore/{username}/bootcamp_data/"
local_data_path = f"/dbfs/FileStore/{username}/bootcamp_data/"

checkpoint_stream1_path = f"dbfs:/FileStore/{username}/checkpoint_stream1/"

# Construct the unique database name
database_name = db_name
print(f"Database Name: {database_name}")

# DBFS Path is
print(f"DBFS Path is: {base_table_path}")

#Local Data path is
print(f"Local Data Path is: {local_data_path}")

#Checkpoint Path is 
print(f"Checkpoint Path is: {checkpoint_stream1_path}")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

display(dbutils.fs.ls(f"{base_table_path}"))

# COMMAND ----------

dataPath = f"{base_table_path}sensor_readings_current_labeled.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

df.createOrReplaceTempView("input_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS current_readings_labeled;
# MAGIC 
# MAGIC CREATE TABLE current_readings_labeled 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT * FROM input_vw
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS current_readings_unlabeled;
# MAGIC 
# MAGIC CREATE TABLE current_readings_unlabeled 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     reading_1,
# MAGIC     reading_2,
# MAGIC     reading_3
# MAGIC   FROM input_vw
# MAGIC )

# COMMAND ----------

# Return to the caller, passing the variables needed for file paths and database

response = checkpoint_stream1_path + " " + base_table_path + " " + database_name

dbutils.notebook.exit(response)
