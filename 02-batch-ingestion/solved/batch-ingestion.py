# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ACID Transactions with Delta Lake (IoT Data)
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" width=200/>
# MAGIC 
# MAGIC This is a companion notebook to provide a Delta Lake example against the IoT data.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

setup_responses = dbutils.notebook.run("./Utils/Setup-Batch-GDrive", 0).split()

dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]
bronze_table_path = f"{dbfs_data_path}tables/bronze"
silver_table_path = f"{dbfs_data_path}tables/silver"
silver_clone_table_path = f"{dbfs_data_path}tables/silver_clone"
silver_sh_clone_table_path = f"{dbfs_data_path}tables/silver_clone_shallow"
silver_constraints_table_path = f"{dbfs_data_path}tables/silver_constraints"
gold_table_path = f"{dbfs_data_path}tables/gold"
gold_agg_table_path = f"{dbfs_data_path}tables/goldagg"
parquet_table_path = f"{dbfs_data_path}tables/parquet"
autoloader_ingest_path = f"{dbfs_data_path}/autoloader_ingest/"
dbutils.fs.rm(bronze_table_path, recurse=True)
dbutils.fs.rm(silver_table_path, recurse=True)
dbutils.fs.rm(gold_table_path, recurse=True)
dbutils.fs.rm(parquet_table_path, recurse=True)
dbutils.fs.rm(silver_clone_table_path, recurse=True)

print("DBFS path is {}".format(dbfs_data_path))
print("Database name is {}".format(database_name))
print("Brone Table Location is {}".format(bronze_table_path))
print("Silver Table Location is {}".format(silver_table_path))
print("Gold Table Location is {}".format(gold_table_path))
print("Parquet Table Location is {}".format(parquet_table_path))

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## About the Data
# MAGIC 
# MAGIC The data used is mock data for 2 types of devices - Transformer/ Rectifier from 3 power plants generating 3 set of readings relevant to monitoring the status of that device type
# MAGIC 
# MAGIC ![ioT_Data](https://miro.medium.com/max/900/1*M_Q4XQ4pTCuANLyEZqrDOg.jpeg)

# COMMAND ----------

dataPath = f"{dbfs_data_path}historical_sensor_data.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

df.createOrReplaceTempView("bronze_readings_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_readings_view

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from bronze_readings_view

# COMMAND ----------

# MAGIC %sql
# MAGIC Select distinct(device_operational_status) from bronze_readings_view

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ETL Flow - Batch

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Architecture
# MAGIC 
# MAGIC <img src="https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://drive.google.com/uc?export=download&id=1cZJCR5Z_9VDG05u0VfKplDjk7k5r6Y7I" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Bronze Delta Lake Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/simplysaydelta.png" width=800/>

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS sensor_readings_historical_bronze USING DELTA LOCATION '{bronze_table_path}' AS SELECT * from bronze_readings_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from sensor_readings_historical_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### What does Delta Log look like?

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sensor_readings_historical_bronze

# COMMAND ----------

dbutils.fs.ls(bronze_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date range & data volume for current Bronze Data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Maybe Date would be a good way to partition the data
# MAGIC 
# MAGIC SELECT DISTINCT DATE(reading_time), count(*) FROM sensor_readings_historical_bronze group by DATE(reading_time)
# MAGIC 
# MAGIC -- Hmmm, there are only two dates, so maybe that's not the best choice.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Databricks notebooks built-in visulaizations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's make a query that shows a meaningful graphical view of the table
# MAGIC -- How many rows exist for each operational status?
# MAGIC -- Experiment with different graphical views... be creative!
# MAGIC 
# MAGIC SELECT 
# MAGIC device_operational_status, count(*) as count
# MAGIC FROM sensor_readings_historical_bronze
# MAGIC GROUP BY device_operational_status
# MAGIC ORDER BY count desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver Table 
# MAGIC #### There is some missing data. Time to create a silver table, backfill and transform!

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver;

# COMMAND ----------

spark.sql(f"CREATE TABLE if not exists sensor_readings_historical_silver USING DELTA LOCATION '{silver_table_path}' AS SELECT * from bronze_readings_view")

# COMMAND ----------

dataPath = f"{dbfs_data_path}backfill_sensor_data_final.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

df.createOrReplaceTempView("backfill_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from backfill_view

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT DATE(reading_time) as BF_DATE, count(*) as BF_COUNT FROM backfill_view group by DATE(reading_time);

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) MERGE INTO Support
# MAGIC 
# MAGIC #### INSERT or UPDATE with Parquet: 7-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake: 2-step process
# MAGIC 
# MAGIC With Delta Lake, inserting or updating a table is a simple 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use the `MERGE` command

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO sensor_readings_historical_silver AS SL
# MAGIC USING backfill_view AS BF
# MAGIC ON 
# MAGIC   SL.id = BF.id
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sensor_readings_historical_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Validate the output of merge

# COMMAND ----------

# MAGIC %md
# MAGIC You should see data for a 21st of Feb + an increased count for 23rd of Feb

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT DATE(reading_time) as SL_DATE, count(*) as SL_COUNT FROM sensor_readings_historical_silver group by DATE(reading_time);

# COMMAND ----------

# MAGIC %md
# MAGIC #### Correcting some bad data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE reading_1 = 999.99 OR reading_2 = 999.99 OR reading_3 = 999.99

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using Lag & Lead to create an average value for bad readings

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_interpolations;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_interpolations AS (
# MAGIC   WITH lags_and_leads AS (
# MAGIC     SELECT
# MAGIC       id, 
# MAGIC       reading_time,
# MAGIC       device_type,
# MAGIC       device_id,
# MAGIC       device_operational_status,
# MAGIC       reading_1,
# MAGIC       LAG(reading_1, 1, 0)  OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_1_lag,
# MAGIC       LEAD(reading_1, 1, 0) OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_1_lead,
# MAGIC       reading_2,
# MAGIC       LAG(reading_2, 1, 0)  OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_2_lag,
# MAGIC       LEAD(reading_2, 1, 0) OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_2_lead,
# MAGIC       reading_3,
# MAGIC       LAG(reading_3, 1, 0)  OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_3_lag,
# MAGIC       LEAD(reading_3, 1, 0) OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_3_lead
# MAGIC     FROM sensor_readings_historical_silver
# MAGIC   )
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     device_operational_status,
# MAGIC     ((reading_1_lag + reading_1_lead) / 2) AS reading_1,
# MAGIC     ((reading_2_lag + reading_2_lead) / 2) AS reading_2,
# MAGIC     ((reading_3_lag + reading_3_lead) / 2) AS reading_3
# MAGIC   FROM lags_and_leads
# MAGIC   WHERE reading_1 = 999.99 OR reading_2 = 999.99 OR reading_3 = 999.99
# MAGIC   ORDER BY id ASC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's examine our interpolations to make sure they are correct
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_interpolations

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO sensor_readings_historical_silver as SL
# MAGIC USING sensor_readings_historical_interpolations as INTP
# MAGIC ON
# MAGIC SL.id = INTP.id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET SL.reading_1 = INTP.reading_1, SL.reading_2 = INTP.reading_2, SL.reading_3 = INTP.reading_3
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Validate the output of merge

# COMMAND ----------

# MAGIC %md
# MAGIC You should not get any output

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE reading_1 = 999.99 OR reading_2 = 999.99 OR reading_3 = 999.99

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sensor_readings_historical_silver

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) DELTA Table Constraints
# MAGIC 
# MAGIC #### How can have we avoid this from happening during ingest time?

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver_with_constraints

# COMMAND ----------

spark.sql(f"CREATE TABLE sensor_readings_historical_silver_with_constraints (id STRING, reading_time TIMESTAMP, device_type STRING, device_id STRING, device_operational_status STRING, reading_1 DOUBLE, reading_2 DOUBLE, reading_3 DOUBLE) USING DELTA LOCATION '{silver_constraints_table_path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC ALTER TABLE sensor_readings_historical_silver_with_constraints ADD CONSTRAINT reading_1_check CHECK (reading_1 != 999.99);
# MAGIC ALTER TABLE sensor_readings_historical_silver_with_constraints ADD CONSTRAINT reading_2_check CHECK (reading_2 != 999.99);
# MAGIC ALTER TABLE sensor_readings_historical_silver_with_constraints ADD CONSTRAINT reading_3_check CHECK (reading_3 != 999.99);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sensor_readings_historical_silver_with_constraints SELECT * FROM bronze_readings_view WHERE reading_1 != 999.99 OR reading_2 != 999.99 OR reading_3 != 999.99

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from sensor_readings_historical_silver_with_constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sensor_readings_historical_silver_with_constraints SELECT * FROM bronze_readings_view WHERE reading_1 = 999.99

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) DELETE Support
# MAGIC 
# MAGIC Let us delete data for device_operational_status = 'CORRUPTED'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_operational_status, count(*) AS count FROM sensor_readings_historical_silver
# MAGIC GROUP BY device_operational_status
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md Let's start by creating a traditional Parquet table

# COMMAND ----------

# Load new DataFrame based on current Delta table
df_1 = sql("SELECT * FROM sensor_readings_historical_silver")

parquet_table_path = f"{dbfs_data_path}tables/parquet"

# Save DataFrame to Parquet
df_1.write.mode("overwrite").parquet(parquet_table_path)

# Reload Parquet Data
df_pq = spark.read.parquet(parquet_table_path)

# Create new table on this parquet data
df_pq.createOrReplaceTempView("sensor_readings_historical_silver_pq_tmpview")

sqlCreate = f"DROP TABLE IF EXISTS sensor_readings_historical_silver_pq"
spark.sql(sqlCreate)

sqlCreate = spark.sql(f"create table sensor_readings_historical_silver_pq using PARQUET LOCATION '{parquet_table_path}' AS SELECT * FROM sensor_readings_historical_silver_pq_tmpview")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `DELETE` on the Parquet table
# MAGIC DELETE FROM sensor_readings_historical_silver_pq WHERE device_operational_status = 'CORRUPTED'

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the `DELETE` statements are not supported in Parquet, but are supported in Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `DELETE` on the Delta table
# MAGIC DELETE FROM sensor_readings_historical_silver WHERE device_operational_status = 'CORRUPTED'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_operational_status, count(*) AS count FROM sensor_readings_historical_silver
# MAGIC GROUP BY device_operational_status
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) UPDATE Support
# MAGIC There is one record which has a wrong device id '7G007TTTTT' instead of '7G007T'. Let us update this

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, count(*) AS count FROM sensor_readings_historical_silver
# MAGIC GROUP BY device_id
# MAGIC ORDER BY count ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `UPDATE` on the Parquet table
# MAGIC UPDATE sensor_readings_historical_silver_pq SET `device_id` = '7G007T' WHERE device_id = '7G007TTTTT'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `UPDATE` on the Delta table
# MAGIC UPDATE sensor_readings_historical_silver SET `device_id` = '7G007T' WHERE device_id = '7G007TTTTT'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, count(*) AS count FROM sensor_readings_historical_silver
# MAGIC GROUP BY device_id
# MAGIC ORDER BY count ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Cloning
# MAGIC Shallow Clone & Deep Clone

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM sensor_readings_historical_silver VERSION AS OF 1

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS sensor_readings_historical_silver_clone DEEP CLONE sensor_readings_historical_silver VERSION AS OF 1 LOCATION '{silver_clone_table_path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM sensor_readings_historical_silver_clone

# COMMAND ----------

dbutils.fs.ls(f'{silver_clone_table_path}')

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS sensor_readings_historical_silver_clone_shallow SHALLOW CLONE sensor_readings_historical_silver VERSION AS OF 1 LOCATION '{silver_sh_clone_table_path}'")

# COMMAND ----------

dbutils.fs.ls(f'{silver_sh_clone_table_path}')

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Schema Evolution
# MAGIC With the `mergeSchema` option, you can evolve your Delta Lake table schema

# COMMAND ----------

# Generate new loans with dollar amounts 
tmp_df = sql("SELECT *, CAST(rand(10000)/8 AS double) AS reading_4, CAST(rand(1000)/9 AS double) AS reading_5 FROM sensor_readings_historical_silver LIMIT 10")
display(tmp_df)

# COMMAND ----------

# Let's write this data out to our Delta table
tmp_df.write.format("delta").mode("append").save(silver_clone_table_path)

# COMMAND ----------

# MAGIC %md **Note**: The command above fails because the schema of our new data does not match the schema of our original data.
# MAGIC 
# MAGIC By adding the **mergeSchema** option, we can successfully migrate our schema, as shown below.

# COMMAND ----------

# Add the mergeSchema option
tmp_df.write.option("mergeSchema","true").format("delta").mode("append").save(silver_clone_table_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_historical_silver_clone ORDER BY reading_4 desc

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Let's Travel back in Time!
# MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
# MAGIC 
# MAGIC * Audit Data Changes
# MAGIC * Reproduce experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC You can query by:
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC using Python, Scala, and/or Scala syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Review Delta Lake Table History
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sensor_readings_historical_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, count(*) AS count FROM sensor_readings_historical_silver VERSION AS OF 1
# MAGIC GROUP BY device_id
# MAGIC ORDER BY count ASC 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Benefits

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading CSV

# COMMAND ----------

dataPath = f"{dbfs_data_path}historical_sensor_data.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Reading parquet

# COMMAND ----------

df = spark.read.parquet(parquet_table_path)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading Delta

# COMMAND ----------

df = spark.read.option("format", "delta").load(bronze_table_path)
display(df)

# COMMAND ----------

# For our dataset of aprox 300MB we could get better performance with smaller fiels - let’s change max file size to 10MB: 10485760
spark.conf.set("spark.databricks.delta.optimize.maxFileSize",10485760)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE sensor_readings_historical_bronze ZORDER BY (device_id, device_operational_status)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_historical_bronze WHERE device_id = '7G007R' AND device_operational_status != 'NOMINAL'

# COMMAND ----------

dbutils.fs.ls(bronze_table_path)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM sensor_readings_historical_bronze RETAIN 0 HOURS 

# COMMAND ----------

dbutils.fs.ls(bronze_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Create Gold table
# MAGIC Now that our Silver table has been cleaned and conformed, and we've evolved the schema, the next step is to create a Gold table. Gold tables are often created to provide clean, reliable data for a specific business unit or use case.
# MAGIC 
# MAGIC In our case, we'll create a Gold table that joins the silver table with the Plant dimension - to provide an aggregated view of our data. For our purposes, this table will allow us to show what Delta Lake can do, but in practice a table like this could be used to feed a downstream reporting or BI tool that needs data formatted in a very specific way. Silver tables often feed multiple downstream Gold tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_plant

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sensor_readings_historical_gold_view
# MAGIC AS
# MAGIC SELECT a.plant_id, a.device_id, a.plant_type, b.device_type, b.device_operational_status, b.reading_time, b.reading_1, b.reading_2, b.reading_3
# MAGIC FROM dim_plant a INNER JOIN sensor_readings_historical_silver b
# MAGIC ON a.device_id = b.device_id

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Plot the occurrence of all events grouped by `plant_id`.
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Keys:</b> `plant_id
# MAGIC * <b>Series Groupings:</b> `device_type, device_operational_status, plant_type`
# MAGIC * <b>Values:</b> `count`
# MAGIC 
# MAGIC In <b>Display type</b>, use <b>Bar Chart</b> and click <b>Apply</b>.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_gold_view LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC Select plant_id, plant_type, device_type, device_operational_status, count(*) as count from sensor_readings_historical_gold_view
# MAGIC group by plant_id, plant_type, device_type, device_operational_status

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Create the Gold Delta lake table from the temporary view

# COMMAND ----------

spark.sql(f"CREATE TABLE if not exists sensor_readings_gold USING DELTA LOCATION '{gold_table_path}' AS SELECT * FROM sensor_readings_historical_gold_view")

# COMMAND ----------

spark.sql(f"CREATE TABLE if not exists sensor_readings_gold_agg USING DELTA LOCATION '{gold_agg_table_path}' AS Select plant_id, plant_type, device_type, device_operational_status, count(*) as count from sensor_readings_historical_gold_view group by plant_id, plant_type, device_type, device_operational_status")

# COMMAND ----------

# MAGIC %md [Time to make our streams come true!]([https://demo.cloud.databricks.com/#/notebook/8498362])