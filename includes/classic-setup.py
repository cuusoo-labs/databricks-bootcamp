# Databricks notebook source

import pyspark.sql.functions as F
import re

dbutils.widgets.text("course", "sslh")
course = dbutils.widgets.get("course")

username = spark.sql("SELECT current_user()").collect()[0][0]
userhome = f"dbfs:/user/{username}/{course}"
database = f"""{course}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""

print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

dbutils.widgets.text("mode", "setup")
mode = dbutils.widgets.get("mode")

if mode == "reset":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)

# COMMAND ----------

# MAGIC %run ./mount-datasets
