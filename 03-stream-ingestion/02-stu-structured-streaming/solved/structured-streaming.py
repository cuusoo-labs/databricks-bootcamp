# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup environment 
# MAGIC 
# MAGIC Run the cell below to setup files used for this exercise. 

# COMMAND ----------

setup_responses = dbutils.notebook.run("../../includes/Setup-Batch-GDrive", 0).split()
setup_responses = dbutils.notebook.run("../../includes/Setup-Streaming-GDrive", 0).split()

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

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simulate a Stream using data in a table
# MAGIC 
# MAGIC A Delta Lake table can be a streaming source, sink, or both.
# MAGIC 
# MAGIC We're going to use a __Delta Lake table__, `readings_stream_source`, as our __stream source__.  It's just an ordinary Delta Lake table, but when we run "readStream" against it below, it will become a streaming source.
# MAGIC 
# MAGIC The table doesn't contain any data yet.  We'll initiate the stream now, and then later we'll generate data into the table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Setup 1 - Load input data for simulation
# MAGIC 
# MAGIC 
# MAGIC ##### About the Data
# MAGIC 
# MAGIC The data used is mock data for 2 types of devices - Transformer/ Rectifier from 3 power plants generating 3 set of readings relevant to monitoring the status of that device type
# MAGIC 
# MAGIC ![ioT_Data](https://miro.medium.com/max/900/1*M_Q4XQ4pTCuANLyEZqrDOg.jpeg)

# COMMAND ----------

dataPath = f"{dbfs_data_path}/sensor_readings_current_labeled.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup 2 - Landing Zone for Streaming Input Data - Bronze table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Specify the schema using the following: 
# MAGIC 
# MAGIC ```
# MAGIC id INTEGER, reading_time TIMESTAMP, device_type STRING, device_id STRING, device_operational_status STRING, reading_1 DOUBLE, reading_2 DOUBLE, reading_3 DOUBLE
# MAGIC ```

# COMMAND ----------

schema = "id INTEGER, reading_time TIMESTAMP, device_type STRING, device_id STRING, device_operational_status STRING, reading_1 DOUBLE, reading_2 DOUBLE, reading_3 DOUBLE"

# COMMAND ----------

# MAGIC %md
# MAGIC Create a read stream with the following parameters 
# MAGIC ```
# MAGIC .format('delta')
# MAGIC .table('readings_stream_source')
# MAGIC ```
# MAGIC 
# MAGIC Create or replace a temp view with the read stream, and give it a name of `readings_streaming`

# COMMAND ----------

streamingDF = (spark
  .readStream
  .format("csv")
  .schema(schema)
  .load(dbfs_data_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Write the stream using: 
# MAGIC ```
# MAGIC .format('delta')
# MAGIC .outputMode('append')
# MAGIC .option("checkpointLocation", checkpoint_stream1_path)
# MAGIC ```
# MAGIC 
# MAGIC And the output path specified in the variable `output_sink_path` 

# COMMAND ----------

dbutils.fs.rm(output_sink_path, True)    # clear this directory in case lesson has been run previously

# COMMAND ----------

streamingQuery = (streamingDF                                
  .writeStream                                                
  .format("delta")                                          
  .option("checkpointLocation", checkpoint_stream1_path)               
  .outputMode("append")
  .start(output_sink_path)                                       
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Create a streaming SQL table by executing the code below. This will allow us to run Spark SQL queries against the streaming dataset. 

# COMMAND ----------

spark.sql("CREATE TABLE if not exists readings_stream_source (id INTEGER, reading_time TIMESTAMP, device_type STRING, device_id STRING, device_operational_status STRING, reading_1 DOUBLE, reading_2 DOUBLE, reading_3 DOUBLE) USING DELTA LOCATION '" + output_sink_path + "'")

readings_stream = (spark
                   .readStream
                   .format('delta')
                   .table('readings_stream_source'))

# Register the stream as a temporary view so we can run SQL on it
readings_stream.createOrReplaceTempView("readings_streaming")

# COMMAND ----------

# MAGIC %md 
# MAGIC Using Spark SQL, write the following query: 
# MAGIC 
# MAGIC Display 
# MAGIC - device type
# MAGIC - device_operational_status
# MAGIC - average of reading 1 
# MAGIC 
# MAGIC From the `readings_streaming` table 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   device_type,
# MAGIC   device_operational_status, 
# MAGIC   AVG(reading_1) AS average
# MAGIC FROM readings_streaming
# MAGIC GROUP BY device_type, device_operational_status
# MAGIC ORDER BY device_type ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   window,
# MAGIC   device_type,
# MAGIC   device_operational_status,
# MAGIC   count(device_type) count,
# MAGIC   avg(reading_1) average
# MAGIC FROM readings_streaming 
# MAGIC GROUP BY 
# MAGIC   WINDOW(reading_time, '2 minutes', '1 minute'),
# MAGIC   device_type,
# MAGIC   device_operational_status
# MAGIC ORDER BY 
# MAGIC   window DESC, 
# MAGIC   device_type ASC 
# MAGIC LIMIT 10 -- this lets us see the last five window aggregations for device_type

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream
