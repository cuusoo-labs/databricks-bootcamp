# Databricks notebook source
# MAGIC %md
# MAGIC ## Part 1: Set up the environment 
# MAGIC 
# MAGIC Run the cells below up to part 2 to setup files used for this exercise. 

# COMMAND ----------

setup_responses = dbutils.notebook.run("../../../includes/Setup-Batch-GDrive", 0).split()
setup_responses = dbutils.notebook.run("../../../includes/Setup-Streaming-GDrive", 0).split()

checkpoint_stream1_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]
streaming_table_path = f"{dbfs_data_path}tables/streaming"
output_sink_path = f"{dbfs_data_path}tables/streaming_output"

dbutils.fs.rm(streaming_table_path, recurse=True)
dbutils.fs.rm(checkpoint_stream1_path, recurse=True)
dbutils.fs.rm(output_sink_path, recurse=True)

print("DBFS path is {}".format(dbfs_data_path))
print("Database name is {}".format(database_name))
print("Streaming Table Location is {}".format(streaming_table_path))
print("Checkpoint Location is {}".format(checkpoint_stream1_path))
print("Output Sink Location is {}".format(output_sink_path))

# COMMAND ----------

dbutils.fs.rm(streaming_table_path, recurse=True)
dbutils.fs.rm(checkpoint_stream1_path, recurse=True)
dbutils.fs.rm(output_sink_path, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Create delta table to simulate a stream source

# COMMAND ----------

dataPath = f"{dbfs_data_path}sensor_readings_current_labeled.csv"

raw_df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

raw_df.createOrReplaceTempView("input_vw")

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
# MAGIC DROP TABLE IF EXISTS readings_stream_source;

# COMMAND ----------

spark.sql("CREATE TABLE if not exists readings_stream_source (id INTEGER, reading_time TIMESTAMP, device_type STRING, device_id STRING, device_operational_status STRING, reading_1 DOUBLE, reading_2 DOUBLE, reading_3 DOUBLE) USING DELTA LOCATION '" + streaming_table_path + "'")

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's simulate an application that streams data into our readings_stream_source table

# COMMAND ----------

import time
import threading 

def insert_rows(): 
  next_row = 0

  while(next_row < 12000):

    time.sleep(1)

    next_row += 3

    spark.sql(f"""
      INSERT INTO readings_stream_source (
        SELECT * FROM current_readings_labeled
        WHERE id < {next_row} )
    """)

background_insert_rows = threading.Thread(name="insert_rows_background", target=insert_rows)
background_insert_rows.start()

# COMMAND ----------

# MAGIC %md
# MAGIC Observe rows being added into `readings_stream_source`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM readings_stream_source 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Understand the data 
# MAGIC 
# MAGIC 
# MAGIC ##### About the Data
# MAGIC 
# MAGIC The data used is mock data for 2 types of devices - Transformer/ Rectifier from 3 power plants generating 3 set of readings relevant to monitoring the status of that device type
# MAGIC 
# MAGIC ![ioT_Data](https://miro.medium.com/max/900/1*M_Q4XQ4pTCuANLyEZqrDOg.jpeg)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Preview the data by reading it from: 
# MAGIC 
# MAGIC `f"{dbfs_data_path}/sensor_readings_current_labeled.csv"` and displaying the `df` 

# COMMAND ----------

dataPath = f"{dbfs_data_path}/sensor_readings_current_labeled.csv"

df = (spark.read
  .option("header", "true")
  .option("delimiter", ",")
  .option("inferSchema", "true")
  .csv(dataPath))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Working with streaming data
# MAGIC 
# MAGIC ### Run a stream and display the results 
# MAGIC 
# MAGIC Create a streaming dataframe by using `spark.readStream` and load data from the delta table specified in the variable `streaming_table_path`. 
# MAGIC 
# MAGIC Display the streaming dataframe using `display()`.

# COMMAND ----------

streaming_df = (
  spark.readStream
  .format("delta")
  .load(streaming_table_path)
)

display(streaming_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Run a stream with Aggregation
# MAGIC 
# MAGIC Using the previous query created, perform an aggregation using: 
# MAGIC 1. `groupBy`: `device_type`, `device_operational_status`
# MAGIC 2. `avg`: `reading_1`
# MAGIC 
# MAGIC 
# MAGIC Display the streaming dataframe using `display()`.

# COMMAND ----------

from pyspark.sql.functions import avg, col 
streaming_agg_df = (
  spark
  .readStream
  .format("delta")
  .load(streaming_table_path)
  .groupBy(col("device_type"), col("device_operational_status"))
  .avg("reading_1")
)
display(streaming_agg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Run a stream with a Windowed Aggregation
# MAGIC 
# MAGIC Using the previous query created, perform a windowed aggregation using `reading_time` with a window duration of `"1 minute"`
# MAGIC 
# MAGIC The method `window` accepts a timestamp column and a window duration to define tumbling windows. Adding a third argument for `slideDuration` allows definition of a sliding window; see [documentation](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=window#pyspark.sql.functions.window) for more details.
# MAGIC 
# MAGIC Display the streaming dataframe using `display()`.

# COMMAND ----------

from pyspark.sql.functions import avg, col, window

streamingWindowDF = (
  spark
  .readStream
  .format("delta")
  .load(streaming_table_path)
  .groupBy(
    col("device_type"), 
    col("device_operational_status"),
    window(col("reading_time"), "1 minute")
  )
  .avg("reading_1")
)
display(streamingWindowDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Run a Stream with Watermarking
# MAGIC 
# MAGIC Using the previous query, add in watermarking using the `reading_time` column and a watermark duration of `"2 hours"`. 
# MAGIC 
# MAGIC The `withWatermark` method should be used after the `load` method. 
# MAGIC 
# MAGIC Display the streaming dataframe using `display()`.

# COMMAND ----------

from pyspark.sql.functions import avg, col, window

streamingWindowWatermarkDF = (
  spark
  .readStream
  .format("delta")
  .load(streaming_table_path)
  .withWatermark("reading_time", "2 hours")
  .groupBy(
    col("device_type"), 
    col("device_operational_status"),
    window(col("reading_time"), "1 minute")
  )
  .avg("reading_1")
)
display(streamingWindowWatermarkDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Writing streaming data 
# MAGIC 
# MAGIC 
# MAGIC Write the watermarking stream from the previous query using: 
# MAGIC ```
# MAGIC .format('delta')
# MAGIC .outputMode('append')
# MAGIC .option("checkpointLocation", checkpoint_stream1_path)
# MAGIC ```
# MAGIC 
# MAGIC And the output path specified in the variable `output_sink_path` 

# COMMAND ----------

# clear this directory in case lesson has been run previously
dbutils.fs.rm(output_sink_path, True)    

# COMMAND ----------

streaming_query = (streamingWindowWatermarkDF                                
  .writeStream                                                
  .format("delta")                                          
  .option("checkpointLocation", checkpoint_stream1_path)               
  .outputMode("append")
  .start(output_sink_path)                                       
)

# COMMAND ----------

# MAGIC %md
# MAGIC Exercises complete - stop all streams

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream
