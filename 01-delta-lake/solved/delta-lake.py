# Databricks notebook source
# MAGIC %md
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Architecture
# MAGIC 
# MAGIC <img src="https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up the required files paths and user database

# COMMAND ----------

# MAGIC %run "../../includes/main-includes"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ensure we are using our own database

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lets see what sample data sets are available to us in the Databricks file system...

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### We will use the retail data set. There appears to be a README - Lets see what is says

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/retail-org/"))

# COMMAND ----------

display(spark.read.text("dbfs:/databricks-datasets/retail-org/README.md"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a look at the sales orders - Only the top 10 rows as a sample

# COMMAND ----------

df = spark.read.format("json").load("dbfs:/databricks-datasets/retail-org/sales_orders/")
display(df.take(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lets get started
# MAGIC We need the columns below from the sales orders json file:
# MAGIC - customer_id renamed to **CustomerID**
# MAGIC - customer_name renamed to **CustomerName**
# MAGIC - order_datetime renamed to **OrderDateTime**
# MAGIC - order_number renamed to **OrderNo**
# MAGIC - number_of_line_items to **LineItemCount**
# MAGIC - ordered_products renamed to **OrderedProducts**
# MAGIC 
# MAGIC You will notice that the source **ordered_products** column is a nested array - We will split that up later. 
# MAGIC Once you are done renaming your columns, create a temp table called **Sales_Order_Bronze_Temp**

# COMMAND ----------

dfSales = df.selectExpr("customer_id AS CustomerID",
                        "customer_name AS CustomerName", 
                        "order_datetime AS OrderDateTime", 
                        "order_number AS OrderNo",
                        "number_of_line_items AS LineItemCount",
                        "ordered_products AS OrderedProducts")
dfSales.createOrReplaceTempView("Sales_Order_Bronze_Temp")

# COMMAND ----------

# MAGIC %md
# MAGIC #### We can now change language and start writing some queries using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Sales_Order_Bronze_Temp LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CustomerID
# MAGIC   ,CustomerName
# MAGIC   ,COUNT(*) AS NoOfOrders
# MAGIC   ,SUM(LineItemCount) AS LineItemCount
# MAGIC FROM
# MAGIC   Sales_Order_Bronze_Temp
# MAGIC GROUP BY
# MAGIC   CustomerID
# MAGIC   ,CustomerName
# MAGIC ORDER BY
# MAGIC   NoOfOrders DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lets create a bronze delta table - it's easy!
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/simplysaydelta.png" width=800/>

# COMMAND ----------

# Write the data as a delta table if it doesn't exist
tableName = "Sales_Order_Bronze"
tablePath = f"{bronze_table_path}/Sales_Order"
try: 
    spark.sql(f"DESCRIBE {tableName}")
except:    
    df = spark.table("Sales_Order_Bronze_Temp")
    df\
        .write\
        .format("delta")\
        .save(f"{tablePath}")
    # Create the bronze table
    spark.sql(f"CREATE TABLE Sales_Order_Bronze USING DELTA LOCATION '{tablePath}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What does the Delta Log look like?

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY Sales_Order_Bronze

# COMMAND ----------

display(spark.read.json(f"{tablePath}/_delta_log/00000000000000000000.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Lets see what the earliest order date is

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   Sales_Order_Bronze
# MAGIC ORDER BY
# MAGIC   OrderDateTime

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM Sales_Order_Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Oh damn, I accidentally ran a DELETE without a WHERE clause - What now??????
# MAGIC 
# MAGIC ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Let's Travel back in Time!
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

# MAGIC %sql
# MAGIC RESTORE Sales_Order_Bronze VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the log one last time

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY Sales_Order_Bronze
