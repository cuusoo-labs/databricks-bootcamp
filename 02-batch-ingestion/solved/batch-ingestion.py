# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Batch processing with Delta Lake
# MAGIC 
# MAGIC <img src="https://drive.google.com/uc?export=download&id=1cZJCR5Z_9VDG05u0VfKplDjk7k5r6Y7I" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the delta lake notebook to ensure we have the required data from the previous lab

# COMMAND ----------

# MAGIC %run "../../01-delta-lake/solved/delta-lake"

# COMMAND ----------

# MAGIC %md
# MAGIC ### If you looked carefully, you would have noticed that there is a products data set in the original location too - It is a csv file

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/retail-org/"))
display(spark.read.text("dbfs:/databricks-datasets/retail-org/README.md"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lets use that as the source for our Product table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_Product_Bronze
# MAGIC USING csv
# MAGIC OPTIONS (path='dbfs:/databricks-datasets/retail-org/products/', delimiter';', header='true')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist the bronze product data as a delta table

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS Product_Bronze USING DELTA LOCATION '{bronze_table_path}/Product' AS SELECT * FROM vw_Product_Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remember the OrderedProducts column is a json array? Lets get the ordered line item details

# COMMAND ----------

# DBTITLE 1,Check for a sample set of data
# MAGIC %sql
# MAGIC SELECT * FROM Sales_Order_Bronze LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_Sales_Order_Line_Item_Bronze
# MAGIC AS
# MAGIC SELECT
# MAGIC   CustomerID
# MAGIC   ,col.id AS ProductID
# MAGIC   ,OrderDateTime
# MAGIC   ,OrderNo
# MAGIC   ,pos + 1 AS LineNo
# MAGIC   ,col.curr AS CurrencyCode
# MAGIC   ,col.price AS Price
# MAGIC   ,col.qty AS Quantity
# MAGIC   ,col.price * col.qty AS OrderAmount
# MAGIC   ,col.unit AS UnitOfMeasure
# MAGIC FROM
# MAGIC   (
# MAGIC   SELECT
# MAGIC     CustomerID
# MAGIC     ,CustomerName
# MAGIC     ,from_unixtime(OrderDateTime) AS OrderDateTime
# MAGIC     ,OrderNo
# MAGIC     ,posexplode_outer(OrderedProducts) -- posexplode_outer return all rows, even those where OrderedProducts is empty, and adds a positional column to the data set
# MAGIC   FROM
# MAGIC     Sales_Order_Bronze
# MAGIC   ) Order_Lines

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time to create all the silver tables

# COMMAND ----------

# Create the product table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS Product_Silver
USING DELTA
LOCATION '{silver_table_path}/Product'
AS
SELECT
    product_id AS ProductID
    ,product_category AS ProductCategory
    ,product_name AS ProductName
    ,product_unit AS UnitOfMeasure
    ,CAST(sales_price AS DECIMAL(10,2)) AS SalePrice 
FROM
    Product_Bronze
""")

# Create the Customer table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS Customer_Silver
USING DELTA
LOCATION '{silver_table_path}/Customer'
AS
SELECT DISTINCT
    CustomerID
    ,CustomerName
FROM
    Sales_Order_Bronze
""")

# Create the Customer table
spark.sql(f"CREATE TABLE IF NOT EXISTS Sales_Order_Line_Item_Silver USING DELTA LOCATION '{silver_table_path}/Sales_Order_Line' AS SELECT * FROM vw_Sales_Order_Line_Item_Bronze")

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) UPDATE Support
# MAGIC There are a some records where the **OrderDateTime** is empty. Lets set them to 1900-01-01

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE
# MAGIC   Sales_Order_Line_Item_Silver
# MAGIC SET
# MAGIC   OrderDateTime = '1900-01-01'
# MAGIC WHERE
# MAGIC   OrderDateTime IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the delta logs

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY Sales_Order_Line_Item_Silver;

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) DELETE Support
# MAGIC There appear to be some duplicate rows - Lets remove them

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerID,ProductID,OrderDateTime,LineNo, COUNT(*) FROM Sales_Order_Line_Item_Silver
# MAGIC GROUP BY CustomerID,ProductID,OrderDateTime,LineNo
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM 
# MAGIC   Sales_Order_Line_Item_Silver
# MAGIC WHERE 
# MAGIC   (CustomerID = 18558459 AND ProductID = 'AVpiHEE31cnluZ0-J8jJ' AND OrderDateTime = '1900-01-01' AND LineNo = 2)
# MAGIC   OR
# MAGIC   (CustomerID = 15424995 AND ProductID = 'AVpjedgc1cnluZ0-W4NI' AND OrderDateTime = '1900-01-01' AND LineNo = 3)

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
# MAGIC -- Lets remove the duplicate records in source using a windowing function
# MAGIC MERGE INTO Sales_Order_Line_Item_Silver AS T
# MAGIC USING 
# MAGIC   (
# MAGIC   SELECT
# MAGIC     CustomerID
# MAGIC     ,ProductID
# MAGIC     ,OrderDateTime
# MAGIC     ,OrderNo
# MAGIC     ,LineNo
# MAGIC     ,CurrencyCode
# MAGIC     ,Price
# MAGIC     ,Quantity
# MAGIC     ,OrderAmount
# MAGIC     ,UnitOfMeasure
# MAGIC   FROM
# MAGIC     (SELECT
# MAGIC       *
# MAGIC       ,ROW_NUMBER() OVER(PARTITION BY CustomerID,ProductID,OrderDateTime,LineNo ORDER BY OrderDateTime) AS RowID
# MAGIC      FROM
# MAGIC        vw_Sales_Order_Line_Item_Bronze
# MAGIC     ) AS Source
# MAGIC   WHERE
# MAGIC     RowID = 1
# MAGIC   ) AS S
# MAGIC ON 
# MAGIC   T.CustomerID = S.CustomerID
# MAGIC   AND T.ProductID = S.ProductID
# MAGIC   AND T.OrderDateTime = S.OrderDateTime
# MAGIC   AND T.LineNo = S.LineNo
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY Sales_Order_Line_Item_Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Cloning
# MAGIC ### Shallow Clone & Deep Clone
# MAGIC A clone can be either deep or shallow: deep clones copy over the data from the source and shallow clones do not

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lets clone the original version of the Sales_Order_Line_Item_Silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM Sales_Order_Line_Item_Silver VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deep clone

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS Sales_Order_Line_Item_Silver_Deep_Clone DEEP CLONE Sales_Order_Line_Item_Silver VERSION AS OF 0 LOCATION '{silver_clone_table_path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM Sales_Order_Line_Item_Silver_Deep_Clone

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lets take a look at the new files on disk

# COMMAND ----------

dbutils.fs.ls(f'{silver_clone_table_path}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shallow clone

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS Sales_Order_Line_Item_Silver_Shallow_Clone SHALLOW CLONE Sales_Order_Line_Item_Silver VERSION AS OF 0 LOCATION '{silver_sh_clone_table_path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM Sales_Order_Line_Item_Silver_Shallow_Clone

# COMMAND ----------

# MAGIC %md
# MAGIC ### This time, there are no new data files

# COMMAND ----------

dbutils.fs.ls(f'{silver_sh_clone_table_path}')

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Schema Evolution
# MAGIC With the `mergeSchema` option, you can evolve your Delta Lake table schema

# COMMAND ----------

# Add a new column to calculate the Unit price
tmp_df = spark.sql("SELECT *, Price / Quantity AS UnitPrice FROM Sales_Order_Line_Item_Silver")
display(tmp_df)

# COMMAND ----------

# Let's write this data out to our Delta table
tmp_df.write.format("delta").mode("overwrite").save(f'{silver_table_path}/Sales_Order_Line')

# COMMAND ----------

# MAGIC %md **Note**: The command above fails because the schema of our new data does not match the schema of our original data.
# MAGIC 
# MAGIC By adding the **mergeSchema** option, we can successfully migrate our schema, as shown below.

# COMMAND ----------

# Add the mergeSchema option
tmp_df.write.option("mergeSchema","true").format("delta").mode("overwrite").save(f'{silver_table_path}/Sales_Order_Line')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a look at the original and the new schema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Sales_Order_Line_Item_Silver VERSION AS OF 0 LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Sales_Order_Line_Item_Silver VERSION AS OF 4 LIMIT 10;

# COMMAND ----------

# MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Review Delta Lake Table History
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete and merge with schema modification

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY Sales_Order_Line_Item_Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Create Gold tables
# MAGIC Now that our Silver tables have been cleaned and transformed, and we've evolved the schema, the next step is to create our Gold tables. Gold tables are often created to provide clean, reliable data for a specific business unit or use case.
# MAGIC 
# MAGIC In our case, we'll create Gold tables for the **Customer** and **Product** dimensions and join those with the **Sales Order Line** silver table to provide an aggregated view of our data. For our purposes, this table will allow us to show what Delta Lake can do, but in practice a table like this could be used to feed a downstream reporting or BI tool that needs data formatted in a very specific way. Silver tables often feed multiple downstream Gold tables.

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS Customer_Gold USING DELTA LOCATION '{gold_table_path}/Customer' AS SELECT * FROM Customer_Silver")
spark.sql(f"CREATE TABLE IF NOT EXISTS Product_Gold USING DELTA LOCATION '{gold_table_path}/Product' AS SELECT * FROM Product_Silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Sales_Order_Line_Item_Gold_View
# MAGIC AS
# MAGIC SELECT 
# MAGIC   P.ProductCategory
# MAGIC   ,P.ProductName
# MAGIC   ,C.CustomerName
# MAGIC   ,LI.OrderDateTime
# MAGIC   ,LI.Quantity
# MAGIC   ,LI.OrderAmount
# MAGIC FROM 
# MAGIC   Sales_Order_Line_Item_Silver LI 
# MAGIC   LEFT OUTER JOIN Customer_Gold C ON LI.CustomerID = C.CustomerID
# MAGIC   LEFT OUTER JOIN Product_Gold P ON LI.ProductID = P.ProductID

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a visualisation
# MAGIC 
# MAGIC Plot the total order amount of all products grouped by `ProductCategory`.
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Keys:</b> `CustomerName`
# MAGIC * <b>Series Groupings:</b> `ProductCategory`
# MAGIC * <b>Values:</b> `SalesAmount`
# MAGIC 
# MAGIC In <b>Display type</b>, use <b>Bar Chart, 100% Stacked</b> and click <b>Apply</b>.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Sales_Order_Line_Item_Gold_View LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create the gold Sales Order table

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS Sales_Order_Gold USING DELTA LOCATION '{gold_table_path}/Sales_Order' AS SELECT * FROM Sales_Order_Line_Item_Gold_View")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use the legacy datetime parser for Spark 3.0

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create an aggregated table for monthly sales analysis

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS Sales_Order_Gold_Agg USING DELTA LOCATION '{gold_agg_table_path}' 
AS 
SELECT
    ProductCategory
    ,ProductName
    ,CustomerName
    ,to_date(OrderDateTime, 'yyyy-MM') AS YearMonth
    ,SUM(Quantity) AS Quantity
    ,SUM(OrderAmount) AS OrderAmount
FROM
    Sales_Order_Line_Item_Gold_View
GROUP BY
    ProductCategory
    ,ProductName
    ,CustomerName
    ,YearMonth
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Sales_Order_Gold_Agg ORDER BY OrderAmount DESC LIMIT 10;

# COMMAND ----------

# MAGIC %md [Time to make our streams come true!]([https://demo.cloud.databricks.com/#/notebook/8498362])
