# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ../../../includes/classic-setup $mode="reset"

# COMMAND ----------

dbutils.fs.ls("/mnt/training/definitive-guide/data/activity-json/streaming")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Micro-Batches as a Table
# MAGIC 
# MAGIC For more information, see the analogous section in the [Structured Streaming Programming Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts) (from which several images have been borrowed).
# MAGIC 
# MAGIC Spark Structured Streaming approaches streaming data by modeling it as a series of continuous appends to an unbounded table. While similar to defining **micro-batch** logic, this model allows incremental queries to be defined against streaming sources as if they were static input.
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png" style="height: 300px"/>
# MAGIC 
# MAGIC ### Basic Concepts
# MAGIC 
# MAGIC - The developer defines an **input table** by configuring a streaming read against a **source**. The syntax provides entry that is nearly analogous to working with static data.
# MAGIC - A **query** is defined against the input table. Both the DataFrames API and Spark SQL can be used to easily define transformations and actions against the input table.
# MAGIC - This logical query on the input table generates the **results table**. The results table contains the incremental state information of the stream.
# MAGIC - The **output** of a streaming pipeline will persist updates to the results table by writing to an external **sink**. Generally, a sink will be a durable system such as files or a pub/sub messaging bus.
# MAGIC - New rows are appended to the input table for each **trigger interval**. These new rows are essentially analogous to micro-batch transactions and will be automatically propagated through the results table to the sink.
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-model.png" style="height: 300px"/>
# MAGIC 
# MAGIC This lesson will demonstrate the ease of adapting batch logic to streaming data to run data workloads in near real-time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-end Fault Tolerance
# MAGIC 
# MAGIC Structured Streaming ensures end-to-end exactly-once fault-tolerance guarantees through _checkpointing_ (discussed below) and <a href="https://en.wikipedia.org/wiki/Write-ahead_logging" target="_blank">Write Ahead Logs</a>.
# MAGIC 
# MAGIC Structured Streaming sources, sinks, and the underlying execution engine work together to track the progress of stream processing. If a failure occurs, the streaming engine attempts to restart and/or reprocess the data.
# MAGIC For best practices on recovering from a failed streaming query see <a href="">docs</a>.
# MAGIC 
# MAGIC This approach _only_ works if the streaming source is replayable; replayable sources include cloud-based object storage and pub/sub messaging services.
# MAGIC 
# MAGIC At a high level, the underlying streaming mechanism relies on a couple approaches:
# MAGIC 
# MAGIC * First, Structured Streaming uses checkpointing and write-ahead logs to record the offset range of data being processed during each trigger interval.
# MAGIC * Next, the streaming sinks are designed to be _idempotent_—that is, multiple writes of the same data (as identified by the offset) do _not_ result in duplicates being written to the sink.
# MAGIC 
# MAGIC Taken together, replayable data sources and idempotent sinks allow Structured Streaming to ensure **end-to-end, exactly-once semantics** under any failure condition.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reading a Stream
# MAGIC 
# MAGIC The `readStream` method returns a `DataStreamReader` used to configure the stream.
# MAGIC 
# MAGIC Configuring a streaming read on a source requires:
# MAGIC * The schema of the data
# MAGIC * The `format` of the source [(file format or named connector)](https://docs.databricks.com/spark/latest/structured-streaming/data-sources.html)
# MAGIC * Configurations specific to the source:
# MAGIC   * [Kafka](https://docs.databricks.com/spark/latest/structured-streaming/kafka.html)
# MAGIC   * [Kinesis](https://docs.databricks.com/spark/latest/structured-streaming/kinesis.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### The Schema
# MAGIC 
# MAGIC Every streaming DataFrame must have a schema. When connecting to pub/sub systems like Kafka and Kinesis, the schema will be automatically provided by the source.
# MAGIC 
# MAGIC For other streaming sources, the schema must be user-defined. It is not safe to infer schema from files, as the assumption is that the source is growing indefinitely from zero records.

# COMMAND ----------

schema = "Arrival_Time BIGINT, Creation_Time BIGINT, Device STRING, Index BIGINT, Model STRING, User STRING, geolocation STRUCT<city: STRING, country: STRING>, gt STRING, id BIGINT, x DOUBLE, y DOUBLE, z DOUBLE"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Differences between Static and Streaming Reads
# MAGIC 
# MAGIC In the cell below, a static and streaming read are each defined against the same source (files in a directory on a cloud object store). Note that the syntax is identical except that the streaming query uses `readStream` instead of `read`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> While `maxFilesPerTrigger` will be used throughout the material in this course to limit how quickly source files are consumed, this is optional and for demonstration purposes. This option allows control over how much data will be processed in each micro-batch.

# COMMAND ----------

from pyspark.sql.functions import col

dataPath = "/mnt/training/definitive-guide/data/activity-json/streaming"

staticDF = (spark
  .read
  .format("json")
  .schema(schema)
  .load(dataPath)
)

streamingDF = (spark
  .readStream
  .format("json")
  .schema(schema)
  .option("maxFilesPerTrigger", 1) 
  .load(dataPath)
  .select((col("Creation_Time")/1E9).alias("time").cast("timestamp"),
        col("gt").alias("action"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Just like with static DataFrames, data is not processed and jobs are not triggered until an action is called.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Writing a Stream
# MAGIC 
# MAGIC The method `DataFrame.writeStream` returns a `DataStreamWriter` used to configure the output of the stream.
# MAGIC 
# MAGIC There are a number of required parameters to configure a streaming write:
# MAGIC * The `format` of the **output sink** (see [documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks))
# MAGIC * The location of the **checkpoint directory**
# MAGIC * The **output mode**
# MAGIC * Configurations specific to the output sink, such as:
# MAGIC   * [Kafka](https://docs.databricks.com/spark/latest/structured-streaming/kafka.html)
# MAGIC   * [Kinesis](https://docs.databricks.com/spark/latest/structured-streaming/kinesis.html)
# MAGIC   * A <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=foreach#pyspark.sql.streaming.DataStreamWriter.foreach"target="_blank">custom sink</a> via `writeStream.foreach(...)`
# MAGIC 
# MAGIC Once the configuration is completed, trigger the job with a call to `.start()`. When writing to files, use `.start(filePath)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpointing
# MAGIC 
# MAGIC Databricks creates checkpoints by storing the current state of your streaming job to S3.
# MAGIC 
# MAGIC Checkpointing combines with write ahead logs to allow a terminated stream to be restarted and continue from where it left off.
# MAGIC 
# MAGIC Checkpoints cannot be shared between separate streams.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Output Modes
# MAGIC 
# MAGIC Streaming jobs have output modes similar to static/batch workloads. [More details here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes).
# MAGIC 
# MAGIC | Mode   | Example | Notes |
# MAGIC | ------------- | ----------- |
# MAGIC | **Append** | `.outputMode("append")`     | _DEFAULT_ - Only the new rows appended to the Result Table since the last trigger are written to the sink. |
# MAGIC | **Complete** | `.outputMode("complete")` | The entire updated Result Table is written to the sink. The individual sink implementation decides how to handle writing the entire table. |
# MAGIC | **Update** | `.outputMode("update")`     | Only the rows in the Result Table that were updated since the last trigger will be outputted to the sink. Since Spark 2.1.1 |
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Not all sinks will support `update` mode.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Differences between Static and Streaming Writes
# MAGIC 
# MAGIC The following cell demonstrates batch logic to append data from a static read.

# COMMAND ----------

outputPath = userhome + "/static-write"

dbutils.fs.rm(outputPath, True)    # clear this directory in case lesson has been run previously

(staticDF                                
  .write                                               
  .format("delta")                                          
  .mode("append")                                       
  .save(outputPath))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Note that there are only minor syntax differences when writing a stream instead:
# MAGIC - `writeStream` instead of `write`
# MAGIC - The path for the checkpoint is provided to the option `checkpointLocation`
# MAGIC - `outputMode` instead of `mode` (note that streaming uses `complete` instead of `overwrite` for similar functionality here)
# MAGIC - `start` instead of `save`
# MAGIC 
# MAGIC The following cell demonstrates a streaming write to Delta files.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Assigning a variable name when writing to a sink provides programmatic access to a `StreamingQuery` object. This will be discussed below.

# COMMAND ----------

outputPath = userhome + "/streaming-concepts"
checkpointPath = outputPath + "/checkpoint"

dbutils.fs.rm(outputPath, True)    # clear this directory in case lesson has been run previously

streamingQuery = (streamingDF                                
  .writeStream                                                
  .format("delta")                                          
  .option("checkpointLocation", checkpointPath)               
  .outputMode("append")
#   .queryName("my_stream")        # optional argument to register stream to Spark catalog
  .start(outputPath)                                       
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming with Delta Lake
# MAGIC 
# MAGIC In the logic defined above, data is read from JSON files and then saved out in the Delta Lake format. Note that because Delta creates a new version for each transaction, when working with streaming data this will mean that the Delta table creates a new version for each trigger interval in which new data is processed. [More info on streaming with Delta](https://docs.databricks.com/delta/delta-streaming.html#table-streaming-reads-and-writes).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Defining the Trigger Interval
# MAGIC 
# MAGIC When defining a streaming write, the `trigger` method specifies when the system should process the next set of data. The example above uses the default, which is the same as `.trigger(Trigger.ProcessingTime("500 ms"))`.
# MAGIC 
# MAGIC | Trigger Type                           | Example | Notes |
# MAGIC |----------------------------------------|-----------|-------------|
# MAGIC | Unspecified                            |  | _DEFAULT_ - The query will be executed as soon as the system has completed processing the previous query |
# MAGIC | Fixed interval micro-batches           | `.trigger(Trigger.ProcessingTime("2 minutes"))` | The query will be executed in micro-batches and kicked off at the user-specified intervals |
# MAGIC | One-time micro-batch                   | `.trigger(Trigger.Once())` | The query will execute _only one_ micro-batch to process all the available data and then stop on its own |
# MAGIC | Continuous w/fixed checkpoint interval | `.trigger(Trigger.Continuous("1 second"))` | The query will be executed in a low-latency, <a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing" target = "_blank">continuous processing mode</a>. _EXPERIMENTAL_ in 2.3.2 |
# MAGIC 
# MAGIC Note that triggers are specified when defining how data will be written to a sink and control the frequency of micro-batches. By default, Spark will automatically detect and process all data in the source that has been added since the last trigger; some sources allow configuration to limit the size of each micro-batch.
# MAGIC 
# MAGIC :BEST_PRACTICE: Read [this blog post](https://databricks.com/blog/2017/05/22/running-streaming-jobs-day-10x-cost-savings.html) to learn more about using `Trigger.Once` to simplify CDC with a hybrid streaming/batch design.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Streaming Aggregations
# MAGIC 
# MAGIC Continuous applications often require near real-time decisions on real-time, aggregated statistics.
# MAGIC 
# MAGIC Some examples include
# MAGIC * Aggregating errors in data from IoT devices by type
# MAGIC * Detecting anomalous behavior in a server's log file by aggregating by country.
# MAGIC * Doing behavior analysis on instant messages via hash tags.
# MAGIC 
# MAGIC While these streaming aggregates may need to reference historic trends, generally analytics will be calculated over discrete units of time. Spark Structured Streaming supports time-based **windows** on streaming DataFrames to make these calculations easy.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What is Time?
# MAGIC 
# MAGIC Multiple times may be associated with each streaming event. Consider the discrete differences between the time at which the event data was:
# MAGIC - Generated
# MAGIC - Written to the streaming source
# MAGIC - Processed into Spark
# MAGIC 
# MAGIC Each of these times will be recorded from the system clock of the machine running the process. Discrepancies and latencies may have many different causes. 
# MAGIC 
# MAGIC Generally speaking, most analytics will be interested in the time the data was generated. As such, this lesson will focus on timestamps recorded at the time of data generation, here referred to as the **event time**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Windowing
# MAGIC 
# MAGIC Defining windows on a time series field allows users to utilize this field for aggregations in the same way they would use distinct values when calling `GROUP BY`. The state table will maintain aggregates for each user-defined bucket of time. Spark supports two types of windows:
# MAGIC 
# MAGIC **Tumbling Windows**
# MAGIC 
# MAGIC Windows do not overlap, but rather represent distinct buckets of time. Each event will be aggregated into only one window. 
# MAGIC 
# MAGIC **Sliding windows** 
# MAGIC 
# MAGIC The windows overlap and a single event may be aggregated into multiple windows. 
# MAGIC 
# MAGIC The diagram below from the <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a> guide shows sliding windows.
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-window.png">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Define a Windowed Aggregation
# MAGIC 
# MAGIC The method `window` accepts a timestamp column and a window duration to define tumbling windows. Adding a third argument for `slideDuration` allows definition of a sliding window; see [documentation](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=window#pyspark.sql.functions.window) for more details.
# MAGIC 
# MAGIC Here, the count of actions for each hour is aggregated.

# COMMAND ----------

from pyspark.sql.functions import window, col

countsDF = (streamingDF
  .groupBy(col("action"),                     
           window(col("time"), "1 hour"))    
  .count()                                    
  .select(col("window.start").alias("start"), 
          col("action"),                     
          col("count"))                      
  .orderBy(col("start"), col("action"))      
)

# COMMAND ----------

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stop all Streams
# MAGIC 
# MAGIC When you are done, stop all the streaming jobs.

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Watermarking
# MAGIC 
# MAGIC By default, Structured Streaming keeps around only the minimal intermediate state required to update the results table. When aggregating with many buckets over a long running stream, this can lead to slowdown and eventually OOM errors as the number of buckets calculated with each trigger grows.
# MAGIC 
# MAGIC **Watermarking** allows users to define a cutoff threshold for how much state should be maintained. This cutoff is calculated against the max event time seen by the engine (i.e., the most recent event). Late arriving data outside of this threshold will be discarded.
# MAGIC 
# MAGIC ![watermarking](https://spark.apache.org/docs/latest/img/structured-streaming-watermark-update-mode.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Run a Stream with Watermarking
# MAGIC 
# MAGIC The `withWatermark` option allows users to easily define this cutoff threshold.

# COMMAND ----------

watermarkedDF = (streamingDF
  .withWatermark("time", "2 hours")           # Specify a 2-hour watermark
  .groupBy(col("action"),                     # Aggregate by action...
           window(col("time"), "1 hour"))     # ...then by a 1 hour window
  .count()                                    # For each aggregate, produce a count
  .select(col("window.start").alias("start"), # Elevate field to column
          col("action"),                      # Include count
          col("count"))                       # Include action
  .orderBy(col("start"), col("action"))       # Sort by the start time
)
display(watermarkedDF)                        # Start the stream and display it

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Example Details
# MAGIC 
# MAGIC The threshold is always calculated against the max event time seen.
# MAGIC 
# MAGIC In the example above,
# MAGIC * The in-memory state is limited to two hours of historic data.
# MAGIC * Data arriving more than 2 hours late should be dropped.
# MAGIC * Data received within 2 hours of being generated will never be dropped.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This guarantee is strict in only one direction. Data delayed by more than 2 hours is not guaranteed to be dropped; it may or may not get aggregated. The more delayed the data is, the less likely the engine is going to process it.

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
